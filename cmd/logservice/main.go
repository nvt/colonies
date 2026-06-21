// Command logservice runs the standalone Colonies log service: a process that
// serves only the log RPCs (add_logs/add_log/get_logs/search_logs), over the same
// signed-HTTP protocol as the main server, against the shared database. Running it
// separately keeps log handling off the colonies server. See docs/Logging.md.
package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/colonyos/colonies/pkg/database/postgresql"
	"github.com/colonyos/colonies/pkg/logservice"
	log "github.com/sirupsen/logrus"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.WithFields(log.Fields{"Key": key, "Value": v}).Warning("Ignoring unparseable integer env var")
	}
	return def
}

func main() {
	dbHost := env("COLONIES_DB_HOST", "localhost")
	dbPort := envInt("COLONIES_DB_PORT", 5432)
	dbUser := env("COLONIES_DB_USER", "postgres")
	dbPassword := os.Getenv("COLONIES_DB_PASSWORD")
	dbName := env("COLONIES_DB_NAME", "postgres")
	dbPrefix := env("COLONIES_DB_PREFIX", "PROD_") // must match the colonies server to share the DB
	timescale := os.Getenv("COLONIES_DB_TIMESCALEDB") == "true"

	port := envInt("COLONIES_LOGSERVICE_PORT", 50090)

	cfg := logservice.Config{
		Async:          os.Getenv("COLONIES_LOG_ASYNC") == "true",
		AsyncFlushRows: envInt("COLONIES_LOG_ASYNC_FLUSH_ROWS", 0),
		AsyncFlushMs:   envInt("COLONIES_LOG_ASYNC_FLUSH_MS", 0),
		AsyncQueueCap:  envInt("COLONIES_LOG_ASYNC_QUEUE", 0),
		MaxConcurrency: envInt("COLONIES_LOG_MAX_CONCURRENCY", 0),
	}

	// Share the colonies server's database. Do not Initialize: the colonies
	// server owns the schema; the log service only reads/writes the LOGS table
	// (and reads PROCESSES/EXECUTORS for authorization).
	db := postgresql.CreatePQDatabase(dbHost, dbPort, dbUser, dbPassword, dbName, dbPrefix, timescale)
	if err := db.Connect(); err != nil {
		log.WithFields(log.Fields{"Error": err, "Host": dbHost, "Port": dbPort}).Fatal("Log service failed to connect to the database")
	}
	defer db.Close()

	ls := logservice.NewLogService(db, port, cfg)

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Info("Log service shutting down")
		ls.Shutdown()
	}()

	log.WithFields(log.Fields{"Port": port, "DBHost": dbHost, "DBPrefix": dbPrefix}).Info("Starting Colonies log service")
	if err := ls.ServeForever(); err != nil {
		log.WithFields(log.Fields{"Error": err}).Info("Log service stopped")
	}
}
