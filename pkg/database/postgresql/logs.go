package postgresql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/colonyos/colonies/pkg/core"
	"github.com/lib/pq"
)

func (db *PQDatabase) AddLog(processID string, colonyName string, executorName string, timestamp int64, msg string) error {
	sqlStatement := `INSERT INTO  ` + db.dbPrefix + `LOGS (PROCESS_ID, COLONY_NAME, EXECUTOR_NAME, TS, MSG, ADDED) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := db.postgresql.Exec(sqlStatement, processID, colonyName, executorName, timestamp, msg, time.Now())
	if err != nil {
		return err
	}

	return nil
}

// logColumns is the number of columns inserted per log row.
const logColumns = 6

// maxLogRowsPerInsert bounds rows per multi-row INSERT so the parameter count
// stays well under PostgreSQL's 65535 limit (logColumns * rows).
const maxLogRowsPerInsert = 1000

// copyLogRowsThreshold is the batch size at or above which AddLogs uses a single
// COPY (one transaction) instead of chunked multi-row INSERTs. COPY has higher
// fixed setup cost but a much cheaper per-row path, so it wins for large batches
// (e.g. those produced by async coalescing); small batches stay on INSERT.
const copyLogRowsThreshold = 1000

// AddLogs persists many log rows in one call. Large batches use a single COPY in
// one transaction; smaller batches use chunked multi-row INSERTs. Each log's
// Timestamp is stored as TS (the client/event time); ADDED is the server insert
// time, shared across the batch.
func (db *PQDatabase) AddLogs(logs []*core.Log) error {
	if len(logs) == 0 {
		return nil
	}
	if len(logs) >= copyLogRowsThreshold {
		return db.addLogsCopy(logs)
	}
	return db.addLogsInsert(logs)
}

// addLogsInsert writes the batch using chunked multi-row INSERTs.
func (db *PQDatabase) addLogsInsert(logs []*core.Log) error {
	added := time.Now()
	for start := 0; start < len(logs); start += maxLogRowsPerInsert {
		end := start + maxLogRowsPerInsert
		if end > len(logs) {
			end = len(logs)
		}
		chunk := logs[start:end]

		var sb strings.Builder
		sb.WriteString("INSERT INTO " + db.dbPrefix + "LOGS (PROCESS_ID, COLONY_NAME, EXECUTOR_NAME, TS, MSG, ADDED) VALUES ")
		args := make([]any, 0, len(chunk)*logColumns)
		for i, l := range chunk {
			if i > 0 {
				sb.WriteString(",")
			}
			base := i * logColumns
			sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5, base+6))
			args = append(args, l.ProcessID, l.ColonyName, l.ExecutorName, l.Timestamp, l.Message, added)
		}

		if _, err := db.postgresql.Exec(sb.String(), args...); err != nil {
			return err
		}
	}

	return nil
}

// addLogsCopy writes the batch with a single COPY in one transaction. The table
// and column identifiers must be given in their stored (lowercase) form because
// pq.CopyIn quotes them.
func (db *PQDatabase) addLogsCopy(logs []*core.Log) error {
	added := time.Now()
	table := strings.ToLower(db.dbPrefix + "LOGS")

	txn, err := db.postgresql.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(table, "process_id", "colony_name", "executor_name", "ts", "msg", "added"))
	if err != nil {
		txn.Rollback()
		return err
	}

	for _, l := range logs {
		if _, err := stmt.Exec(l.ProcessID, l.ColonyName, l.ExecutorName, l.Timestamp, l.Message, added); err != nil {
			stmt.Close()
			txn.Rollback()
			return err
		}
	}

	// A final Exec with no arguments flushes the buffered COPY data.
	if _, err := stmt.Exec(); err != nil {
		stmt.Close()
		txn.Rollback()
		return err
	}
	if err := stmt.Close(); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

func (db *PQDatabase) addHistoricalLog(processID string, colonyName string, executorName string, timestamp int64, msg string, t time.Time) error {
	sqlStatement := `INSERT INTO  ` + db.dbPrefix + `LOGS (PROCESS_ID, COLONY_NAME, EXECUTOR_NAME, TS, MSG, ADDED) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := db.postgresql.Exec(sqlStatement, processID, colonyName, executorName, timestamp, msg, t)
	if err != nil {
		return err
	}

	return nil
}

func (db *PQDatabase) parseLogs(rows *sql.Rows) ([]*core.Log, error) {
	var logs []*core.Log

	for rows.Next() {
		var processID string
		var colonyName string
		var executorName string
		var ts int64
		var msg string
		var added time.Time
		if err := rows.Scan(&processID, &colonyName, &executorName, &ts, &msg, &added); err != nil {
			return nil, err
		}
		log := &core.Log{ProcessID: processID, ColonyName: colonyName, ExecutorName: executorName, Timestamp: ts, Message: msg}
		logs = append(logs, log)
	}

	return logs, nil
}

func (db *PQDatabase) GetLogsByProcessID(processID string, limit int) ([]*core.Log, error) {
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE PROCESS_ID=$1 ORDER BY TS ASC LIMIT $2`
	rows, err := db.postgresql.Query(sqlStatement, processID, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

func (db *PQDatabase) GetLogsByExecutor(executorName string, limit int) ([]*core.Log, error) {
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE EXECUTOR_NAME=$1 ORDER BY TS ASC LIMIT $2`
	rows, err := db.postgresql.Query(sqlStatement, executorName, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

func (db *PQDatabase) GetLogsByProcessIDSince(processID string, limit int, since int64) ([]*core.Log, error) {
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE PROCESS_ID=$1 AND TS>$2 ORDER BY TS ASC LIMIT $3`
	rows, err := db.postgresql.Query(sqlStatement, processID, since, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

// GetLogsByProcessIDLatest returns the latest logs for a process (newest first, then reversed for chronological display)
func (db *PQDatabase) GetLogsByProcessIDLatest(processID string, limit int) ([]*core.Log, error) {
	// Get latest logs in descending order, then reverse for chronological display
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE PROCESS_ID=$1 ORDER BY TS DESC LIMIT $2`
	rows, err := db.postgresql.Query(sqlStatement, processID, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	// Reverse to get chronological order (oldest of the latest first)
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}

	return logs, nil
}

func (db *PQDatabase) GetLogsByExecutorSince(executorName string, limit int, since int64) ([]*core.Log, error) {
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE EXECUTOR_NAME=$1 AND TS>$2 ORDER BY TS ASC LIMIT $3`
	rows, err := db.postgresql.Query(sqlStatement, executorName, since, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	return logs, nil
}

// GetLogsByExecutorLatest returns the latest logs for an executor (newest first, then reversed for chronological display)
func (db *PQDatabase) GetLogsByExecutorLatest(executorName string, limit int) ([]*core.Log, error) {
	// Get latest logs in descending order, then reverse for chronological display
	sqlStatement := `SELECT * FROM ` + db.dbPrefix + `LOGS WHERE EXECUTOR_NAME=$1 ORDER BY TS DESC LIMIT $2`
	rows, err := db.postgresql.Query(sqlStatement, executorName, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	logs, err := db.parseLogs(rows)
	if err != nil {
		return nil, err
	}

	// Reverse to get chronological order (oldest of the latest first)
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}

	return logs, nil
}

func (db *PQDatabase) RemoveLogsByColonyName(colonyName string) error {
	sqlStatement := `DELETE FROM ` + db.dbPrefix + `LOGS WHERE COLONY_NAME=$1`
	_, err := db.postgresql.Exec(sqlStatement, colonyName)
	if err != nil {
		return err
	}

	return nil
}

func (db *PQDatabase) CountLogs(colonyName string) (int, error) {
	sqlStatement := `SELECT COUNT(*) FROM ` + db.dbPrefix + `LOGS WHERE COLONY_NAME=$1`
	rows, err := db.postgresql.Query(sqlStatement, colonyName)
	if err != nil {
		return -1, err
	}

	defer rows.Close()

	rows.Next()
	var count int
	err = rows.Scan(&count)
	if err != nil {
		return -1, err
	}

	return count, nil
}

func (db *PQDatabase) SearchLogs(colonyName string, text string, days int, count int) ([]*core.Log, error) {
	sqlStatement := `SELECT *
                     FROM ` + db.dbPrefix + `LOGS
                     WHERE MSG LIKE '%' || $1 || '%' AND COLONY_NAME = $2 
                     AND ADDED > NOW() - make_interval(days => $3) LIMIT $4`

	rows, err := db.postgresql.Query(sqlStatement, text, colonyName, days, count)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var results []*core.Log
	for rows.Next() {
		var processID string
		var colonyName string
		var executorName string
		var timestamp int64
		var message string
		var added time.Time
		if err := rows.Scan(&processID, &colonyName, &executorName, &timestamp, &message, &added); err != nil {
			return nil, err
		}
		results = append(results, &core.Log{ProcessID: processID, ColonyName: colonyName, ExecutorName: executorName, Message: message, Timestamp: timestamp})
	}

	return results, nil
}
