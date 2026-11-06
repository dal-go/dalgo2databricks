package dalgo2databricks

import (
	"context"
	"errors"
	"reflect"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

type backend struct {
	dal.NoConcurrency
	client *Client
}

var _ dal.Backend = (*backend)(nil)

func (c *Client) DB() dal.DB {
	return dal.NewDB(&backend{client: c})
}

func (b *backend) ID() string {
	if b.client.options.Catalog != "" {
		return b.client.options.Catalog
	}
	return b.client.options.WarehouseID
}

func (b *backend) Adapter() dal.Adapter {
	return dal.NewAdapter("dalgo2databricks", "0.1.0")
}

func (b *backend) Schema() dal.Schema {
	return nil
}

func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}

func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}

func contextRank(e error) int {
	if errors.Is(e, context.Canceled) {
		return 3
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return 2
	}
	var x *Error
	if errors.As(e, &x) && x.Code == "closed" {
		return 1
	}
	return 0
}

func setRecord(o *operation, r record.Record, e error) error {
	if final := o.check(); contextRank(final) > contextRank(e) {
		e = final
	}
	for i := 0; i < 4; i++ {
		r.SetError(e)
		final := o.check()
		if final != nil && contextRank(final) > contextRank(e) {
			e = final
			continue
		}
		if final != nil {
			return final
		}
		return e
	}
	return e
}

func finishRecords(o *operation, records []record.Record, e error) error {
	for _, r := range records {
		if r == nil {
			continue
		}
		if final := setRecord(o, r, e); final != nil && contextRank(final) > contextRank(e) {
			e = final
		}
	}
	if final := o.check(); final != nil {
		return final
	}
	return e
}

func identity(o *operation, r record.Record) (*record.Key, string, string, error) {
	if r == nil {
		return nil, "", "", failure("target")
	}
	key, err := observe(o, r.Key)
	if err != nil {
		return nil, "", "", err
	}
	if key == nil || key.Parent() != nil {
		return nil, "", "", dal.ErrNotSupported
	}
	id, ok := key.ID.(string)
	if !ok || id == "" || key.Validate() != nil {
		return nil, "", "", dal.ErrNotSupported
	}
	collection := key.Collection()
	return record.NewKeyWithID(collection, id), collection, id, nil
}

func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return false, err
	}
	defer o.finish()

	if key == nil || key.Parent() != nil {
		return false, dal.ErrNotSupported
	}
	id, ok := key.ID.(string)
	if !ok {
		return false, dal.ErrNotSupported
	}
	p, err := b.client.pointPlan(o, key.Collection(), id)
	if err != nil {
		return false, err
	}
	row, err := b.client.point(o, p, id)
	if final := o.check(); final != nil {
		return false, final
	}
	return row != nil, err
}

func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.GetMulti(ctx, []record.Record{r})
}

func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	o, err := b.client.begin(ctx)
	if err != nil {
		return err
	}
	defer o.finish()

	if len(records) > b.client.options.MaxKeys {
		return failure("bounds")
	}
	records = append([]record.Record(nil), records...)
	keys := make([]*record.Key, len(records))
	ids := make([]string, len(records))
	plans := make([]plan, len(records))

	for i, r := range records {
		key, collection, id, err := identity(o, r)
		if err != nil {
			return finishRecords(o, records, err)
		}
		keys[i] = key
		ids[i] = id
		p, err := b.client.pointPlan(o, collection, id)
		if err != nil {
			return finishRecords(o, records, err)
		}
		plans[i] = p
	}

	var missing error
	rows := make([]*Document, len(records))
	for i, id := range ids {
		rows[i], err = b.client.point(o, plans[i], id)
		if err != nil {
			return finishRecords(o, records, err)
		}
	}

	for i, r := range records {
		if err := o.check(); err != nil {
			return finishRecords(o, records, err)
		}
		if rows[i] == nil {
			notFound := dal.NewErrNotFoundByKey(keys[i], nil)
			if missing == nil {
				missing = notFound
			}
			r.SetError(notFound)
			continue
		}
		if err := setRecord(o, r, nil); err != nil {
			return finishRecords(o, records, err)
		}
		data, err := observe(o, r.Data)
		if err != nil {
			return finishRecords(o, records, err)
		}
		if err := mapTarget(data, rows[i].Row); err != nil {
			return finishRecords(o, records, setRecord(o, r, err))
		}
		if err := setRecord(o, r, nil); err != nil {
			return finishRecords(o, records, err)
		}
	}

	if final := o.check(); final != nil {
		return finishRecords(o, records, final)
	}
	return missing
}

type identifiedRecord struct {
	record.Record
	key *record.Key
}

func (i identifiedRecord) Key() *record.Key { return i.key }

type recordReader struct {
	op         *operation
	collection string
	idColumn   string
	result     *Result
	index      int
	template   record.Record
	first      record.Record
	closed     bool
}

var _ dal.RecordsReader = (*recordReader)(nil)

func (r *recordReader) Cursor() (string, error) {
	return "", dal.ErrNotSupported
}

func (r *recordReader) Close() error {
	if !r.closed {
		r.closed = true
		r.op.finish()
	}
	return nil
}

func (r *recordReader) Next() (record.Record, error) {
	if r.closed {
		return nil, failure("closed")
	}
	if err := r.op.check(); err != nil {
		return nil, err
	}
	if r.index >= len(r.result.Rows) {
		return nil, dal.ErrNoMoreRecords
	}
	row := r.result.Rows[r.index]
	id := row[r.idColumn]
	if id == nil || *id == "" {
		return nil, failure("identity")
	}

	var target record.Record
	key := record.NewKeyWithID(r.collection, *id)
	if key.Validate() != nil {
		return nil, dal.ErrNotSupported
	}

	if r.index == 0 && r.first != nil {
		target = r.first
	} else if r.template != nil {
		data := make(map[string]any)
		target = record.NewRecordWithData(key, &data)
	} else {
		data := make(map[string]any)
		target = record.NewRecordWithData(key, &data)
	}

	if err := setRecord(r.op, target, nil); err != nil {
		return nil, err
	}
	data, err := observe(r.op, target.Data)
	if err != nil {
		return nil, err
	}
	if err = mapTarget(data, row); err != nil {
		return nil, setRecord(r.op, target, err)
	}
	if err = setRecord(r.op, target, nil); err != nil {
		return nil, err
	}
	if err = r.op.check(); err != nil {
		return nil, err
	}

	result := identifiedRecord{Record: target, key: key}
	r.index++
	r.first = nil
	return result, nil
}

func (b *backend) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			o.finish()
		}
	}()

	p, err := b.client.compile(o, query, true)
	if err != nil {
		return nil, err
	}

	sq := query.(dal.StructuredQuery)
	kind, err := observe(o, sq.IDKind)
	if err != nil {
		return nil, err
	}
	if kind != reflect.Invalid && kind != reflect.String {
		return nil, dal.ErrNotSupported
	}

	result, err := b.client.execute(o, p)
	if err != nil {
		return nil, err
	}

	intoRec, err := observe(o, sq.IntoRecord)
	if err != nil {
		return nil, err
	}

	r := &recordReader{
		op:         o,
		collection: p.collection,
		idColumn:   p.table.IDColumn,
		result:     result,
		template:   intoRec,
		first:      intoRec,
	}
	if err = o.check(); err != nil {
		return nil, err
	}
	success = true
	return r, nil
}

type recordsetReader struct {
	op           *operation
	rs           recordset.Recordset
	columns      []Column
	index, total int
	closed       bool
}

var _ dal.RecordsetReader = (*recordsetReader)(nil)

func (r *recordsetReader) Columns() []Column              { return copyColumns(r.columns) }
func (r *recordsetReader) Recordset() recordset.Recordset { return r.rs }
func (r *recordsetReader) Cursor() (string, error)        { return "", dal.ErrNotSupported }

func (r *recordsetReader) Close() error {
	if !r.closed {
		r.closed = true
		r.op.finish()
	}
	return nil
}

func (r *recordsetReader) Next() (recordset.Row, recordset.Recordset, error) {
	if r.closed {
		return nil, nil, failure("closed")
	}
	if err := r.op.check(); err != nil {
		return nil, nil, err
	}
	if r.index >= r.total {
		return nil, r.rs, dal.ErrNoMoreRecords
	}
	row := r.rs.GetRow(r.index)
	if err := r.op.check(); err != nil {
		return nil, nil, err
	}
	r.index++
	return row, r.rs, nil
}

func (b *backend) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	o, err := b.client.begin(ctx)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			o.finish()
		}
	}()

	p, err := b.client.compile(o, query, false)
	if err != nil {
		return nil, err
	}

	recordsetName := p.collection
	for _, opt := range options {
		if opt == nil {
			return nil, failure("input")
		}
		resolved := recordset.NewOptions(opt)
		if n := resolved.Name(); n != "" {
			recordsetName = n
		}
	}

	result, err := b.client.execute(o, p)
	if err != nil {
		return nil, err
	}

	cols := make([]recordset.Column[any], len(result.Columns))
	for i, c := range result.Columns {
		cols[i] = recordset.NewTypedColumn[any](c.Name, nil, recordset.ColDbType(c.Type))
	}
	rs := recordset.NewColumnarRecordset(recordsetName, cols...)
	for _, data := range result.Rows {
		if err = o.check(); err != nil {
			return nil, err
		}
		row := rs.NewRow()
		for i, c := range result.Columns {
			var value any
			if v := data[c.Name]; v != nil {
				value = *v
			}
			if err = row.SetValueByIndex(i, value, rs); err != nil {
				return nil, failure("target")
			}
		}
	}

	if err = o.check(); err != nil {
		return nil, err
	}
	success = true
	return &recordsetReader{
		op:      o,
		rs:      rs,
		columns: copyColumns(result.Columns),
		total:   len(result.Rows),
	}, nil
}
