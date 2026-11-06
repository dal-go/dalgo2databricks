package dalgo2databricks

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

type plan struct {
	sql        string
	parameters []statementParameter
	collection string
	table      Table
	forRecords bool
}

func quoteIdent(s string) string {
	escaped := strings.ReplaceAll(s, "`", "``")
	return "`" + escaped + "`"
}

func qualifyTable(catalog, schema, table string) string {
	var parts []string
	if catalog != "" {
		parts = append(parts, quoteIdent(catalog))
	}
	if schema != "" {
		parts = append(parts, quoteIdent(schema))
	}
	parts = append(parts, quoteIdent(table))
	return strings.Join(parts, ".")
}

func (c *Client) pointPlan(o *operation, collection, id string) (plan, error) {
	if err := o.check(); err != nil {
		return plan{}, err
	}
	t, ok := c.options.Tables[collection]
	if !ok {
		return plan{}, failure("mapping")
	}
	if id == "" || len(id) > c.options.MaxRequestBytes || !utf8.ValidString(id) {
		return plan{}, failure("key")
	}
	target := qualifyTable(c.options.Catalog, c.options.Schema, t.Name)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE %s = :p0 LIMIT 1", target, quoteIdent(t.IDColumn))
	params := []statementParameter{
		{Name: "p0", Value: id, Type: "STRING"},
	}
	return plan{
		sql:        sql,
		parameters: params,
		collection: collection,
		table:      t,
	}, nil
}

func observe[T any](o *operation, fn func() T) (T, error) {
	var zero T
	if err := o.check(); err != nil {
		return zero, err
	}
	v := fn()
	if final := o.check(); final != nil {
		return zero, final
	}
	return v, nil
}

func (c *Client) compile(o *operation, query dal.Query, forRecords bool) (plan, error) {
	q, ok := query.(dal.StructuredQuery)
	if !ok || q == nil {
		return plan{}, dal.ErrNotSupported
	}

	from, err := observe(o, q.From)
	if err != nil {
		return plan{}, err
	}
	if from == nil {
		return plan{}, dal.ErrNotSupported
	}

	base, err := observe(o, from.Base)
	if err != nil {
		return plan{}, err
	}
	joins, err := observe(o, from.Joins)
	if err != nil {
		return plan{}, err
	}
	if len(joins) != 0 {
		return plan{}, dal.ErrNotSupported
	}

	ref, ok := base.(dal.CollectionRef)
	if !ok {
		return plan{}, dal.ErrNotSupported
	}

	collection := ref.Name()
	t, ok := c.options.Tables[collection]
	var tableName string
	if ok {
		tableName = t.Name
	} else {
		tableName = collection
		t = Table{Name: collection, IDColumn: "id"}
	}

	target := qualifyTable(c.options.Catalog, c.options.Schema, tableName)

	var sb strings.Builder
	sb.WriteString("SELECT ")

	cols, err := observe(o, q.Columns)
	if err != nil {
		return plan{}, err
	}

	if len(cols) == 0 {
		sb.WriteString("*")
	} else {
		for i, col := range cols {
			if i > 0 {
				sb.WriteString(", ")
			}
			if col.Wildcard != nil {
				sb.WriteString("*")
			} else if col.Expression == nil {
				sb.WriteString("NULL")
			} else {
				if fref, ok := col.Expression.(dal.FieldRef); ok {
					sb.WriteString(quoteIdent(fref.Name()))
				} else {
					sb.WriteString(col.Expression.String())
				}
			}
			if col.Alias != "" {
				sb.WriteString(" AS ")
				sb.WriteString(quoteIdent(col.Alias))
			}
		}
	}

	sb.WriteString(" FROM ")
	sb.WriteString(target)

	var params []statementParameter
	paramIdx := 0

	where, err := observe(o, q.Where)
	if err != nil {
		return plan{}, err
	}

	if where != nil {
		condSQL, condParams, err := c.compileCondition(o, where, &paramIdx)
		if err != nil {
			return plan{}, err
		}
		if condSQL != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(condSQL)
			params = append(params, condParams...)
		}
	}

	groupBy, err := observe(o, q.GroupBy)
	if err != nil {
		return plan{}, err
	}
	if len(groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, expr := range groupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			colRef, ok := expr.(dal.FieldRef)
			if !ok {
				return plan{}, dal.ErrNotSupported
			}
			sb.WriteString(quoteIdent(colRef.Name()))
		}
	}

	having, err := observe(o, q.Having)
	if err != nil {
		return plan{}, err
	}
	if having != nil {
		hSQL, hParams, err := c.compileCondition(o, having, &paramIdx)
		if err != nil {
			return plan{}, err
		}
		if hSQL != "" {
			sb.WriteString(" HAVING ")
			sb.WriteString(hSQL)
			params = append(params, hParams...)
		}
	}

	orderBy, err := observe(o, q.OrderBy)
	if err != nil {
		return plan{}, err
	}
	if len(orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, order := range orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			expr, err := observe(o, order.Expression)
			if err != nil {
				return plan{}, err
			}
			colRef, ok := expr.(dal.FieldRef)
			if !ok {
				return plan{}, dal.ErrNotSupported
			}
			sb.WriteString(quoteIdent(colRef.Name()))
			desc, err := observe(o, order.Descending)
			if err != nil {
				return plan{}, err
			}
			if desc {
				sb.WriteString(" DESC")
			} else {
				sb.WriteString(" ASC")
			}
		}
	}

	limit, err := observe(o, q.Limit)
	if err != nil {
		return plan{}, err
	}
	if limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	}

	offset, err := observe(o, q.Offset)
	if err != nil {
		return plan{}, err
	}
	if offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", offset))
	}

	return plan{
		sql:        sb.String(),
		parameters: params,
		collection: collection,
		table:      t,
		forRecords: forRecords,
	}, nil
}

func (c *Client) compileCondition(o *operation, cond dal.Condition, paramIdx *int) (string, []statementParameter, error) {
	if cond == nil {
		return "", nil, nil
	}

	switch v := cond.(type) {
	case dal.GroupCondition:
		var parts []string
		var allParams []statementParameter
		operator := " AND "
		if v.Operator() == dal.Or {
			operator = " OR "
		}
		for _, child := range v.Conditions() {
			s, p, err := c.compileCondition(o, child, paramIdx)
			if err != nil {
				return "", nil, err
			}
			if s != "" {
				parts = append(parts, s)
				allParams = append(allParams, p...)
			}
		}
		if len(parts) == 0 {
			return "", nil, nil
		}
		return "(" + strings.Join(parts, operator) + ")", allParams, nil

	case dal.Comparison:
		fieldRef, ok := v.Left.(dal.FieldRef)
		if !ok {
			return "", nil, dal.ErrNotSupported
		}
		colName := quoteIdent(fieldRef.Name())

		if v.Operator == dal.In || v.Operator == dal.NotIn {
			arr, ok := v.Right.(dal.Array)
			if !ok {
				return "", nil, dal.ErrNotSupported
			}
			rv := reflect.ValueOf(arr.Value)
			if !rv.IsValid() || rv.Kind() != reflect.Slice || rv.Len() == 0 {
				return "", nil, failure("input")
			}
			var inPlaceholders []string
			var inParams []statementParameter
			for i := 0; i < rv.Len(); i++ {
				val := rv.Index(i).Interface()
				pName := fmt.Sprintf("p%d", *paramIdx)
				*paramIdx++
				inPlaceholders = append(inPlaceholders, ":"+pName)
				inParams = append(inParams, statementParameter{
					Name:  pName,
					Value: fmt.Sprint(val),
					Type:  paramTypeFor(val),
				})
			}
			opStr := "IN"
			if v.Operator == dal.NotIn {
				opStr = "NOT IN"
			}
			sql := fmt.Sprintf("%s %s (%s)", colName, opStr, strings.Join(inPlaceholders, ", "))
			return sql, inParams, nil
		}

		opStr := ""
		switch v.Operator {
		case dal.Equal:
			opStr = "="
		case dal.Operator("!="):
			opStr = "!="
		case dal.LessThen:
			opStr = "<"
		case dal.LessOrEqual:
			opStr = "<="
		case dal.GreaterThen:
			opStr = ">"
		case dal.GreaterOrEqual:
			opStr = ">="
		default:
			return "", nil, dal.ErrNotSupported
		}

		constVal, ok := v.Right.(dal.Constant)
		if ok && constVal.Value == nil {
			if opStr == "=" {
				return colName + " IS NULL", nil, nil
			}
			if opStr == "!=" {
				return colName + " IS NOT NULL", nil, nil
			}
			return "", nil, failure("input")
		}

		var rawVal any
		if ok {
			rawVal = constVal.Value
		} else {
			rawVal = v.Right
		}

		pName := fmt.Sprintf("p%d", *paramIdx)
		*paramIdx++

		paramVal := fmt.Sprint(rawVal)
		paramType := paramTypeFor(rawVal)

		sql := fmt.Sprintf("%s %s :%s", colName, opStr, pName)
		params := []statementParameter{
			{Name: pName, Value: paramVal, Type: paramType},
		}
		return sql, params, nil

	default:
		return "", nil, dal.ErrNotSupported
	}
}

func paramTypeFor(val any) string {
	if val == nil {
		return "STRING"
	}
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "INT"
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "STRING"
		}
		return "DOUBLE"
	default:
		return "STRING"
	}
}
