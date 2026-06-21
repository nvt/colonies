package postgresql

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	log "github.com/sirupsen/logrus"
)

// These benchmarks measure the database write cost of log ingestion under both a
// plain table (timescale=false) and a TimescaleDB hypertable (timescale=true, the
// production configuration). BenchmarkAddLog is the current production path (one
// autocommit single-row INSERT per log line); BenchmarkAddLogsBatch shows the
// achievable per-line cost when many lines go in a single multi-row INSERT.
//
// Run (requires Postgres/TimescaleDB on :5432 and TZ set):
//   TZ=Europe/Stockholm go test -bench='AddLog' -benchmem -run=^$ ./pkg/database/postgresql/ 2>/dev/null | grep ns/op

const benchLogLine = "2026-06-20T10:00:00Z INFO processing chunk 4213 of 9000, throughput=123MB/s"

func benchEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// benchPrepareDB connects and initializes a fresh schema with the given
// TimescaleDB setting. It skips the benchmark (rather than failing) when no
// database is reachable or the timescaledb extension is unavailable.
func benchPrepareDB(b *testing.B, timescale bool) *PQDatabase {
	log.SetLevel(log.PanicLevel) // silence connection/pool info logs in benchmark output

	host := benchEnv("COLONIES_DB_HOST", "localhost")
	user := benchEnv("COLONIES_DB_USER", "postgres")
	password := benchEnv("COLONIES_DB_PASSWORD", "rFcLGNkgsNtksg6Pgtn9CumL4xXBQ7")
	prefix := "BENCHFALSE_"
	if timescale {
		prefix = "BENCHTRUE_"
	}

	db := CreatePQDatabase(host, 5432, user, password, "postgres", prefix, timescale)
	if err := db.Connect(); err != nil {
		b.Skipf("no PostgreSQL on :5432: %v", err)
	}
	db.Drop()
	if err := db.Initialize(); err != nil {
		db.Drop()
		db.Close()
		b.Skipf("Initialize failed (timescale=%v) - is the timescaledb extension available? %v", timescale, err)
	}
	return db
}

// BenchmarkAddLog: current path, one single-row autocommit INSERT per line.
// ns/op is ns/line.
func BenchmarkAddLog(b *testing.B) {
	for _, timescale := range []bool{false, true} {
		b.Run(fmt.Sprintf("timescale=%v", timescale), func(b *testing.B) {
			db := benchPrepareDB(b, timescale)
			defer db.Close()
			defer db.Drop()

			ts := time.Now().UTC().UnixNano()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.AddLog("bench_proc", "bench_colony", "bench_exec", ts, benchLogLine); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAddLogsBatch: the production AddLogs path (one multi-row INSERT per
// batch). The reported ns/line metric is directly comparable to BenchmarkAddLog's
// ns/op.
func BenchmarkAddLogsBatch(b *testing.B) {
	for _, timescale := range []bool{false, true} {
		for _, batch := range []int{10, 100, 1000} {
			b.Run(fmt.Sprintf("timescale=%v/batch=%d", timescale, batch), func(b *testing.B) {
				db := benchPrepareDB(b, timescale)
				defer db.Close()
				defer db.Drop()

				ts := time.Now().UTC().UnixNano()
				logs := make([]*core.Log, batch)
				for r := 0; r < batch; r++ {
					logs[r] = &core.Log{
						ProcessID:    "bench_proc",
						ColonyName:   "bench_colony",
						ExecutorName: "bench_exec",
						Timestamp:    ts,
						Message:      benchLogLine,
					}
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := db.AddLogs(logs); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/line")
			})
		}
	}
}

// BenchmarkAddLogsCopyVsInsert compares the multi-row INSERT path against the
// COPY path at large batch sizes, to confirm COPY wins (and justify the
// copyLogRowsThreshold). ns/line is comparable to the other log benchmarks.
//
//	TZ=Europe/Stockholm go test -bench=CopyVsInsert -benchtime=1s -run=^$ ./pkg/database/postgresql/ 2>/dev/null | grep -E 'Benchmark|ns/line'
func BenchmarkAddLogsCopyVsInsert(b *testing.B) {
	ts := time.Now().UTC().UnixNano()
	mk := func(n int) []*core.Log {
		logs := make([]*core.Log, n)
		for i := range logs {
			logs[i] = &core.Log{ProcessID: "bp", ColonyName: "bc", ExecutorName: "be", Timestamp: ts, Message: benchLogLine}
		}
		return logs
	}

	for _, batch := range []int{1000, 5000, 10000} {
		logs := mk(batch)
		for _, mode := range []string{"insert", "copy"} {
			b.Run(fmt.Sprintf("%s/batch=%d", mode, batch), func(b *testing.B) {
				db := benchPrepareDB(b, false)
				defer db.Close()
				defer db.Drop()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					if mode == "copy" {
						err = db.addLogsCopy(logs)
					} else {
						err = db.addLogsInsert(logs)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/line")
			})
		}
	}
}
