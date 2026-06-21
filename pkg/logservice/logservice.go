// Package logservice is a standalone server that handles only the log RPCs
// (add_logs/add_log/get_logs/search_logs), over the same signed-HTTP/JSON
// protocol as the main Colonies server. Running it as a separate process keeps
// log handling (CPU, request parsing, connection-pool use) off the colonies
// server. See docs/Logging.md.
//
// It reuses the in-server log handler, so authorization is identical: signature
// recovery, colony membership, and the RUNNING and assigned-executor checks. It
// also reuses the log ingestor (synchronous, or COLONIES_LOG_ASYNC coalescing)
// and the log-write concurrency limit.
package logservice

import (
	"errors"
	"net/http"
	"time"

	"github.com/colonyos/colonies/pkg/backends"
	"github.com/colonyos/colonies/pkg/backends/gin"
	"github.com/colonyos/colonies/pkg/core"
	"github.com/colonyos/colonies/pkg/database"
	"github.com/colonyos/colonies/pkg/logbuffer"
	"github.com/colonyos/colonies/pkg/rpc"
	"github.com/colonyos/colonies/pkg/security"
	"github.com/colonyos/colonies/pkg/security/crypto"
	"github.com/colonyos/colonies/pkg/security/validator"
	loghandlers "github.com/colonyos/colonies/pkg/server/handlers/log"
	"github.com/colonyos/colonies/pkg/server/registry"
	log "github.com/sirupsen/logrus"
)

// Config tunes the log service. The zero value is a synchronous, unlimited
// service (matching the in-server default behavior).
type Config struct {
	// Async enables background coalescing of log writes (COLONIES_LOG_ASYNC).
	Async bool
	// AsyncFlushRows / AsyncFlushMs / AsyncQueueCap tune the async ingestor
	// (0 = library default).
	AsyncFlushRows int
	AsyncFlushMs   int
	AsyncQueueCap  int
	// MaxConcurrency bounds concurrent log-write handling (0 = unlimited).
	MaxConcurrency int
}

// LogService implements the log handler's Server interface against a shared
// database, validator, ingestor, and concurrency limit.
type LogService struct {
	db        database.Database
	validator security.Validator
	crypto    security.Crypto
	ingestor  logbuffer.Ingestor
	logSem    chan struct{} // nil = unlimited
	registry  *registry.HandlerRegistry
	engine    backends.Engine
	server    backends.Server
}

// NewLogService builds a log service bound to the given (shared) database.
func NewLogService(db database.Database, port int, cfg Config) *LogService {
	ls := &LogService{
		db:        db,
		validator: validator.CreateValidator(db),
		crypto:    crypto.CreateCrypto(),
		registry:  registry.NewHandlerRegistry(),
	}

	if cfg.Async {
		ls.ingestor = logbuffer.NewAsync(db.AddLogs, logbuffer.AsyncConfig{
			QueueCap:      cfg.AsyncQueueCap,
			FlushRows:     cfg.AsyncFlushRows,
			FlushInterval: time.Duration(cfg.AsyncFlushMs) * time.Millisecond,
		})
		log.Info("Log service: asynchronous coalescing enabled")
	} else {
		ls.ingestor = logbuffer.NewSync(db.AddLogs)
	}
	if cfg.MaxConcurrency > 0 {
		ls.logSem = make(chan struct{}, cfg.MaxConcurrency)
		log.WithFields(log.Fields{"MaxConcurrency": cfg.MaxConcurrency}).Info("Log service: log-write QoS limit enabled")
	}

	// Reuse the existing log handler and its payload-type registration.
	loghandlers.NewHandlers(ls).RegisterHandlers(ls.registry)

	ls.engine = gin.CreateEngineWithDefaults()
	ls.engine.Use(gin.CORS())
	ls.engine.POST("/api", ls.handleAPIRequest)
	ls.engine.GET("/health", func(c backends.Context) { c.String(http.StatusOK, "") })
	ls.server = gin.NewBackendServer(port, ls.engine)
	return ls
}

func (ls *LogService) handleAPIRequest(c backends.Context) {
	jsonBytes, err := c.ReadBody()
	if ls.HandleHTTPError(c, err, http.StatusBadRequest) {
		return
	}
	rpcMsg, err := rpc.CreateRPCMsgFromJSON(string(jsonBytes))
	if ls.HandleHTTPError(c, err, http.StatusBadRequest) {
		return
	}

	// Limit concurrent log-write handling before signature recovery, so excess
	// requests wait here instead of consuming CPU.
	if ls.logSem != nil && (rpcMsg.PayloadType == rpc.AddLogsPayloadType || rpcMsg.PayloadType == rpc.AddLogPayloadType) {
		ls.logSem <- struct{}{}
		defer func() { <-ls.logSem }()
	}

	recoveredID, err := ls.crypto.RecoverID(rpcMsg.Payload, rpcMsg.Signature)
	if ls.HandleHTTPError(c, err, http.StatusForbidden) {
		return
	}

	if ls.registry.HandleRequestWithRaw(c, recoveredID, rpcMsg.PayloadType, rpcMsg.DecodePayload(), string(jsonBytes)) {
		return
	}

	ls.HandleHTTPError(c, errors.New("unsupported payload type for log service: "+rpcMsg.PayloadType), http.StatusBadRequest)
}

// ServeForever starts the HTTP server (blocking).
func (ls *LogService) ServeForever() error {
	if ls.server == nil {
		return errors.New("no server configured")
	}
	return ls.server.ListenAndServe()
}

// Shutdown drains the ingestor and stops the HTTP server.
func (ls *LogService) Shutdown() {
	if ls.ingestor != nil {
		ls.ingestor.Close()
	}
	if ls.server != nil {
		if err := ls.server.ShutdownWithTimeout(5 * time.Second); err != nil {
			log.WithFields(log.Fields{"Error": err}).Warning("Log service forced to shutdown")
		}
	}
}

// --- log handler Server interface ---

func (ls *LogService) Validator() security.Validator         { return ls.validator }
func (ls *LogService) ExecutorDB() database.ExecutorDatabase { return ls.db }
func (ls *LogService) ProcessDB() database.ProcessDatabase   { return ls.db }
func (ls *LogService) LogDB() database.LogDatabase           { return ls.db }
func (ls *LogService) IngestLogs(logs []*core.Log) error     { return ls.ingestor.Ingest(logs) }

func (ls *LogService) HandleHTTPError(c backends.Context, err error, errorCode int) bool {
	if err == nil {
		return false
	}
	rpcReplyMsg, gerr := ls.generateRPCErrorMsg(err, errorCode)
	if gerr != nil {
		log.WithFields(log.Fields{"Error": gerr}).Error("Failed to generate RPC error reply")
	}
	rpcReplyMsgJSONString, jerr := rpcReplyMsg.ToJSON()
	if jerr != nil {
		log.WithFields(log.Fields{"Error": jerr}).Error("Failed to serialize RPC error reply")
	}
	c.String(errorCode, rpcReplyMsgJSONString)
	return true
}

func (ls *LogService) SendHTTPReply(c backends.Context, payloadType string, jsonString string) {
	rpcReplyMsg, err := rpc.CreateRPCReplyMsg(payloadType, jsonString)
	if ls.HandleHTTPError(c, err, http.StatusBadRequest) {
		return
	}
	rpcReplyMsgJSONString, err := rpcReplyMsg.ToJSON()
	if ls.HandleHTTPError(c, err, http.StatusBadRequest) {
		return
	}
	c.String(http.StatusOK, rpcReplyMsgJSONString)
}

func (ls *LogService) SendEmptyHTTPReply(c backends.Context, payloadType string) {
	ls.SendHTTPReply(c, payloadType, "{}")
}

func (ls *LogService) generateRPCErrorMsg(err error, errorCode int) (*rpc.RPCReplyMsg, error) {
	failure := core.CreateFailure(errorCode, err.Error())
	jsonString, ferr := failure.ToJSON()
	if ferr != nil {
		return nil, ferr
	}
	return rpc.CreateRPCErrorReplyMsg(rpc.ErrorPayloadType, jsonString)
}

// compile-time check that LogService satisfies the log handler's Server interface.
var _ loghandlers.Server = (*LogService)(nil)
