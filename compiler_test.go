package dalgo2databricks

import (
	"context"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiler_PointPlan(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	p, err := c.pointPlan(o, "users", "u-100")
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM `main`.`analytics`.`users` WHERE `id` = :p0 LIMIT 1", p.sql)
	require.Len(t, p.parameters, 1)
	assert.Equal(t, "p0", p.parameters[0].Name)
	assert.Equal(t, "u-100", p.parameters[0].Value)
	assert.Equal(t, "STRING", p.parameters[0].Type)
}

func TestCompiler_StructuredQuery(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	// SELECT col1, col2 FROM users WHERE age >= 18 AND status = 'active' ORDER BY name ASC LIMIT 50 OFFSET 10
	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		Where(
			dal.NewGroupCondition(
				dal.And,
				dal.NewComparison(dal.NewFieldRef("", "age"), dal.GreaterOrEqual, dal.NewConstant(18)),
				dal.NewComparison(dal.NewFieldRef("", "status"), dal.Equal, dal.NewConstant("active")),
			),
		).
		OrderBy(dal.Ascending(dal.NewFieldRef("", "name"))).
		Limit(50).
		Offset(10).
		SelectColumns(
			dal.Column{Expression: dal.NewFieldRef("", "name")},
			dal.Column{Expression: dal.NewFieldRef("", "age")},
		)

	p, err := c.compile(o, q, false)
	require.NoError(t, err)

	expectedSQL := "SELECT `name`, `age` FROM `main`.`analytics`.`users` WHERE (`age` >= :p0 AND `status` = :p1) ORDER BY `name` ASC LIMIT 50 OFFSET 10"
	assert.Equal(t, expectedSQL, p.sql)
	require.Len(t, p.parameters, 2)
	assert.Equal(t, "p0", p.parameters[0].Name)
	assert.Equal(t, "18", p.parameters[0].Value)
	assert.Equal(t, "p1", p.parameters[1].Name)
	assert.Equal(t, "active", p.parameters[1].Value)
}

func TestCompiler_InCondition(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		Where(
			dal.NewComparison(
				dal.NewFieldRef("", "role"),
				dal.In,
				dal.NewArray([]string{"admin", "editor", "viewer"}),
			),
		).
		SelectIntoRecordset()

	p, err := c.compile(o, q, false)
	require.NoError(t, err)

	expectedSQL := "SELECT * FROM `main`.`analytics`.`users` WHERE `role` IN (:p0, :p1, :p2)"
	assert.Equal(t, expectedSQL, p.sql)
	require.Len(t, p.parameters, 3)
	assert.Equal(t, "admin", p.parameters[0].Value)
	assert.Equal(t, "editor", p.parameters[1].Value)
	assert.Equal(t, "viewer", p.parameters[2].Value)
}

func TestCompiler_NullCondition(t *testing.T) {
	c, err := New("https://adb-1234.azuredatabricks.net", validTestOptions())
	require.NoError(t, err)

	o, err := c.begin(context.Background())
	require.NoError(t, err)
	defer o.finish()

	q := dal.From(dal.NewRootCollectionRef("users", "")).NewQuery().
		Where(
			dal.NewComparison(
				dal.NewFieldRef("", "deleted_at"),
				dal.Equal,
				dal.NewConstant(nil),
			),
		).
		SelectIntoRecordset()

	p, err := c.compile(o, q, false)
	require.NoError(t, err)

	expectedSQL := "SELECT * FROM `main`.`analytics`.`users` WHERE `deleted_at` IS NULL"
	assert.Equal(t, expectedSQL, p.sql)
	assert.Empty(t, p.parameters)
}
