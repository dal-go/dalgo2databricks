package dalgo2databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
	"github.com/dal-go/record"
)

var _ dbschema.SchemaReader = (*backend)(nil)

func (b *backend) ListCollections(ctx context.Context, parent *record.Key) ([]dal.CollectionRef, error) {
	return b.client.ListCollections(ctx, parent)
}

func (b *backend) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	return b.client.DescribeCollection(ctx, ref)
}

func (b *backend) ListIndexes(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return b.client.ListIndexes(ctx, ref)
}

func (b *backend) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	return b.client.ListConstraints(ctx, ref)
}

func (b *backend) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	return b.client.ListReferrers(ctx, ref)
}

func (c *Client) ListCollections(ctx context.Context, parent *record.Key) ([]dal.CollectionRef, error) {
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	schema := c.options.Schema
	if parent != nil && parent.Collection() != "" {
		schema = parent.Collection()
	}

	var sql string
	var params []statementParameter

	if schema != "" {
		sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = :p0 ORDER BY table_name"
		params = []statementParameter{{Name: "p0", Value: schema, Type: "STRING"}}
	} else {
		sql = "SELECT table_name FROM information_schema.tables ORDER BY table_name"
	}

	p := plan{
		sql:        sql,
		parameters: params,
	}

	res, err := c.execute(o, p)
	if err != nil {
		// Fallback for legacy non-Unity-Catalog metastore (hive_metastore)
		var fbSQL string
		if schema != "" {
			fbSQL = fmt.Sprintf("SHOW TABLES IN %s", quoteIdent(schema))
		} else {
			fbSQL = "SHOW TABLES"
		}
		fbPlan := plan{sql: fbSQL}
		fbRes, fbErr := c.execute(o, fbPlan)
		if fbErr != nil {
			return nil, err
		}
		res = fbRes
	}

	var refs []dal.CollectionRef
	for _, row := range res.Rows {
		nameVal := row["table_name"]
		if nameVal == nil {
			nameVal = row["tableName"]
		}
		if nameVal != nil && *nameVal != "" {
			refs = append(refs, dal.NewRootCollectionRef(*nameVal, ""))
		}
	}
	return refs, nil
}

func mapDatabricksType(dt string) dbschema.Type {
	upper := strings.ToUpper(strings.TrimSpace(dt))
	switch {
	case strings.HasPrefix(upper, "BOOL"):
		return dbschema.Bool
	case strings.HasPrefix(upper, "INT"), strings.HasPrefix(upper, "BIGINT"),
		strings.HasPrefix(upper, "SMALLINT"), strings.HasPrefix(upper, "TINYINT"),
		strings.HasPrefix(upper, "SHORT"), strings.HasPrefix(upper, "LONG"),
		strings.HasPrefix(upper, "BYTE"):
		return dbschema.Int
	case strings.HasPrefix(upper, "FLOAT"), strings.HasPrefix(upper, "DOUBLE"),
		strings.HasPrefix(upper, "REAL"):
		return dbschema.Float
	case strings.HasPrefix(upper, "DECIMAL"), strings.HasPrefix(upper, "NUMERIC"):
		return dbschema.Decimal
	case strings.HasPrefix(upper, "DATE"), strings.HasPrefix(upper, "TIMESTAMP"):
		return dbschema.Time
	case strings.HasPrefix(upper, "BINARY"):
		return dbschema.Bytes
	default:
		return dbschema.String
	}
}

func (c *Client) DescribeCollection(ctx context.Context, ref *dal.CollectionRef) (*dbschema.CollectionDef, error) {
	if ref == nil || ref.Name() == "" {
		return nil, failure("input")
	}
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	tableName := ref.Name()
	schema := c.options.Schema
	if ref.Schema() != "" {
		schema = ref.Schema()
	}

	sql := "SELECT column_name, data_type, is_nullable, ordinal_position FROM information_schema.columns WHERE table_name = :p0"
	params := []statementParameter{{Name: "p0", Value: tableName, Type: "STRING"}}
	if schema != "" {
		sql += " AND table_schema = :p1"
		params = append(params, statementParameter{Name: "p1", Value: schema, Type: "STRING"})
	}
	sql += " ORDER BY ordinal_position"

	p := plan{sql: sql, parameters: params}
	res, err := c.execute(o, p)
	if err != nil {
		// Fallback for legacy metastore
		fbTarget := tableName
		if schema != "" {
			fbTarget = quoteIdent(schema) + "." + quoteIdent(tableName)
		} else {
			fbTarget = quoteIdent(tableName)
		}
		fbPlan := plan{sql: "DESCRIBE TABLE " + fbTarget}
		fbRes, fbErr := c.execute(o, fbPlan)
		if fbErr != nil {
			return nil, err
		}
		res = fbRes
	}

	var fields []dbschema.FieldDef
	for _, row := range res.Rows {
		colName := row["column_name"]
		if colName == nil {
			colName = row["col_name"]
		}
		dataType := row["data_type"]
		if colName == nil || *colName == "" || strings.HasPrefix(*colName, "#") {
			continue
		}

		nullable := true
		if isNull := row["is_nullable"]; isNull != nil && strings.EqualFold(*isNull, "NO") {
			nullable = false
		}

		typeName := "STRING"
		if dataType != nil {
			typeName = *dataType
		}

		fields = append(fields, dbschema.FieldDef{
			Name:     dal.FieldName(*colName),
			Type:     mapDatabricksType(typeName),
			Nullable: nullable,
		})
	}

	// Discover primary keys via table constraints
	var pkNames []dal.FieldName
	constraints, _ := c.ListConstraints(ctx, ref)
	for _, cdef := range constraints {
		if cdef.Type == "primary-key" {
			pkNames = append(pkNames, dal.FieldName(cdef.Name))
		}
	}

	return &dbschema.CollectionDef{
		Name:       tableName,
		Fields:     fields,
		PrimaryKey: pkNames,
		Indexes:    nil,
	}, nil
}

// ListIndexes returns empty slice since Delta Lake does not support secondary B-tree indexes.
func (c *Client) ListIndexes(_ context.Context, _ *dal.CollectionRef) ([]dbschema.IndexDef, error) {
	return []dbschema.IndexDef{}, nil
}

func (c *Client) ListConstraints(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.ConstraintDef, error) {
	if ref == nil || ref.Name() == "" {
		return nil, failure("input")
	}
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	tableName := ref.Name()
	schema := c.options.Schema
	if ref.Schema() != "" {
		schema = ref.Schema()
	}

	sql := "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name " +
		"FROM information_schema.table_constraints tc " +
		"LEFT JOIN information_schema.key_column_usage kcu " +
		"ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name " +
		"WHERE tc.table_name = :p0"
	params := []statementParameter{{Name: "p0", Value: tableName, Type: "STRING"}}
	if schema != "" {
		sql += " AND tc.table_schema = :p1"
		params = append(params, statementParameter{Name: "p1", Value: schema, Type: "STRING"})
	}

	p := plan{sql: sql, parameters: params}
	res, err := c.execute(o, p)
	if err != nil {
		return nil, nil // Silently succeed with empty constraints if constraints table unavailable
	}

	var constraints []dbschema.ConstraintDef
	for _, row := range res.Rows {
		nameVal := row["constraint_name"]
		typeVal := row["constraint_type"]
		colVal := row["column_name"]
		if nameVal == nil {
			continue
		}
		cType := "check"
		if typeVal != nil {
			t := strings.ToUpper(*typeVal)
			if strings.Contains(t, "PRIMARY") {
				cType = "primary-key"
			} else if strings.Contains(t, "FOREIGN") {
				cType = "foreign-key"
			} else if strings.Contains(t, "UNIQUE") {
				cType = "unique"
			}
		}
		targetName := *nameVal
		if cType == "primary-key" && colVal != nil && *colVal != "" {
			targetName = *colVal
		}
		constraints = append(constraints, dbschema.ConstraintDef{
			Name: targetName,
			Type: cType,
		})
	}
	return constraints, nil
}

func (c *Client) ListReferrers(ctx context.Context, ref *dal.CollectionRef) ([]dbschema.Referrer, error) {
	if ref == nil || ref.Name() == "" {
		return nil, failure("input")
	}
	o, err := c.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer o.finish()

	tableName := ref.Name()
	sql := "SELECT tc.table_name, kcu.column_name FROM information_schema.referential_constraints rc " +
		"JOIN information_schema.table_constraints tc ON rc.constraint_name = tc.constraint_name " +
		"LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name " +
		"WHERE rc.unique_constraint_name IN (" +
		"  SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = :p0" +
		")"
	params := []statementParameter{{Name: "p0", Value: tableName, Type: "STRING"}}

	p := plan{sql: sql, parameters: params}
	res, err := c.execute(o, p)
	if err != nil {
		return nil, nil
	}

	var referrers []dbschema.Referrer
	for _, row := range res.Rows {
		if tbl := row["table_name"]; tbl != nil && *tbl != "" {
			var fields []dal.FieldName
			if col := row["column_name"]; col != nil && *col != "" {
				fields = append(fields, dal.FieldName(*col))
			}
			referrers = append(referrers, dbschema.Referrer{
				Collection: dal.NewRootCollectionRef(*tbl, ""),
				Fields:     fields,
			})
		}
	}
	return referrers, nil
}
