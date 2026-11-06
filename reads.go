package dalgo2databricks

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

func (c *Client) point(o *operation, p plan, id string) (*Document, error) {
	result, err := c.execute(o, p)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, nil
	}
	if len(result.Rows) != 1 {
		return nil, failure("identity")
	}
	row := result.Rows[0]
	nativeID := row[p.table.IDColumn]
	if nativeID == nil || *nativeID != id {
		return nil, failure("identity")
	}
	return &Document{ID: id, Row: row, Columns: result.Columns}, nil
}

func (c *Client) Get(ctx context.Context, collection, id string) (*Document, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	p, err := c.pointPlan(o, collection, id)
	if err != nil {
		return nil, err
	}
	doc, err := c.point(o, p, id)
	if final := o.check(); final != nil {
		return nil, final
	}
	return doc, err
}

func (c *Client) Exists(ctx context.Context, collection, id string) (bool, error) {
	doc, err := c.Get(ctx, collection, id)
	return doc != nil, err
}

func (c *Client) GetMany(ctx context.Context, collection string, ids []string) ([]*Document, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	if len(ids) > c.options.MaxKeys {
		return nil, failure("bounds")
	}
	if _, ok := c.options.Tables[collection]; !ok {
		return nil, failure("mapping")
	}

	plans := make([]plan, len(ids))
	for i, id := range ids {
		p, err := c.pointPlan(o, collection, id)
		if err != nil {
			return nil, err
		}
		plans[i] = p
	}

	docs := make([]*Document, len(ids))
	for i, id := range ids {
		doc, err := c.point(o, plans[i], id)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}

	if err := o.check(); err != nil {
		return nil, err
	}
	return docs, nil
}

func (c *Client) Query(ctx context.Context, query dal.Query) (*Result, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	p, err := c.compile(o, query, false)
	if err != nil {
		return nil, err
	}
	result, err := c.execute(o, p)
	if final := o.check(); final != nil {
		return nil, final
	}
	return result, err
}

func mapTarget(target any, row Row) error {
	if target == nil {
		return nil
	}
	val := reflect.ValueOf(target)
	if val.Kind() != reflect.Ptr {
		return failure("target")
	}
	val = val.Elem()

	if val.Kind() == reflect.Map {
		if val.IsNil() {
			val.Set(reflect.MakeMap(val.Type()))
		}
		for k, v := range row {
			if v != nil {
				val.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(*v))
			} else {
				val.SetMapIndex(reflect.ValueOf(k), reflect.Zero(reflect.TypeOf((*string)(nil)).Elem()))
			}
		}
		return nil
	}

	if val.Kind() == reflect.Struct {
		typ := val.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			tag := field.Tag.Get("db")
			if tag == "" {
				tag = field.Tag.Get("json")
			}
			if tag == "-" {
				continue
			}
			colName := strings.Split(tag, ",")[0]
			if colName == "" {
				colName = field.Name
			}
			cellVal, ok := row[colName]
			if !ok || cellVal == nil {
				continue
			}
			fieldVal := val.Field(i)
			if !fieldVal.CanSet() {
				continue
			}
			switch fieldVal.Kind() {
			case reflect.String:
				fieldVal.SetString(*cellVal)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if n, err := strconv.ParseInt(*cellVal, 10, 64); err == nil {
					fieldVal.SetInt(n)
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if n, err := strconv.ParseUint(*cellVal, 10, 64); err == nil {
					fieldVal.SetUint(n)
				}
			case reflect.Float32, reflect.Float64:
				if f, err := strconv.ParseFloat(*cellVal, 64); err == nil {
					fieldVal.SetFloat(f)
				}
			case reflect.Bool:
				if b, err := strconv.ParseBool(*cellVal); err == nil {
					fieldVal.SetBool(b)
				}
			}
		}
		return nil
	}

	return failure("target")
}
