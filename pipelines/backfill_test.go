package pipelines_test

import (
	"testing"

	"github.com/nicola-strappazzon/dacfy/pipelines"

	"github.com/stretchr/testify/assert"
)

func TestBackfill_UsesViewBackfillQuery(t *testing.T) {
	p := pipelines.Pipelines{
		Database: pipelines.Database{Name: "analytics"},
		View: pipelines.View{
			Materialized: true,
			Populate:     pipelines.TypeBackfill,
			To:           "daily_events",
			Query:        "SELECT id, event_time FROM live_events",
			Backfill: pipelines.ViewBackfill{
				Query: "SELECT id, event_time FROM historical_events",
				Where: "event_time >= '{date_from}' AND event_time < '{date_to}'",
			},
		},
	}
	p.SetParents()

	assert.Equal(t,
		"INSERT INTO analytics.daily_events SELECT id, event_time FROM historical_events",
		p.Backfill.Do().SQL())
	assert.Equal(t,
		"INSERT INTO analytics.daily_events SELECT * FROM (SELECT id, event_time FROM historical_events) WHERE event_time >= '2026-05-01 00:00:00' AND event_time < '2026-05-02 00:00:00'",
		p.Backfill.DoChunk("2026-05-01 00:00:00", "2026-05-02 00:00:00").SQL())
}

func TestBackfill_FallsBackToViewQuery(t *testing.T) {
	p := pipelines.Pipelines{
		Database: pipelines.Database{Name: "analytics"},
		View: pipelines.View{
			Materialized: true,
			Populate:     pipelines.TypeBackfill,
			To:           "daily_events",
			Query:        "SELECT id FROM live_events",
		},
	}
	p.SetParents()

	assert.Equal(t,
		"INSERT INTO analytics.daily_events SELECT id FROM live_events",
		p.Backfill.Do().SQL())
}
