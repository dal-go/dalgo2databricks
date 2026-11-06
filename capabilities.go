package dalgo2databricks

import (
	"github.com/dal-go/dalgo/dal"
)

// QueryCapabilities advertises what the Databricks SQL engine can execute on the server.
func (c *Client) QueryCapabilities() dal.QueryCapabilities {
	return dal.QueryCapabilities{
		GroupBy: true,
		Having:  true,
		OrderBy: true,
		Aggregate: dal.AggregateCapabilities{
			Count:         true,
			CountDistinct: true,
			Sum:           true,
			SumDistinct:   true,
			Avg:           true,
			AvgDistinct:   true,
			Min:           true,
			Max:           true,
		},
	}
}

func (b *backend) QueryCapabilities() dal.QueryCapabilities {
	return b.client.QueryCapabilities()
}
