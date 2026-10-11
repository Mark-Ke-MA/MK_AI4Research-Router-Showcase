package mainhistory

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"codex-persona-router/internal/model"
	"codex-persona-router/internal/securefs"
)

const (
	requestSchema   = "codex.router.main-epoch-history.request"
	responseSchema  = "codex.router.main-epoch-history.response"
	errorSchema     = "codex.router.main-epoch-history.error"
	protocolVersion = 1
	maxRequestBytes = 4096
	maxEpochs       = 4096
	connectionTTL   = 5 * time.Second
)

type Reader interface {
	MainEpochHistory(fromNS, toNS int64) (model.MainEpochHistory, error)
}

type Server struct {
	path   string
	reader Reader
	ln     net.Listener
	ready  atomic.Bool
}

func New(path string, reader Reader) *Server {
	return &Server{path: path, reader: reader}
}

func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

func (s *Server) Start() error {
	if s.reader == nil || filepath.Base(s.path) != "history.sock" {
		return fmt.Errorf("history endpoint rejected")
	}
	if err := securefs.EnsurePrivateDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	if info, err := os.Lstat(s.path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("history path occupied")
		}
		if err := os.Remove(s.path); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.path, 0600); err != nil {
		_ = ln.Close()
		return err
	}
	s.ln = ln
	go s.loop()
	return nil
}

func (s *Server) loop() {
	for {
		connection, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(connection)
	}
}

type request struct {
	Schema     string `json:"schema"`
	Version    int    `json:"version"`
	FromUnixNS string `json:"from_unix_ns"`
	ToUnixNS   string `json:"to_unix_ns"`
}

type rangeObject struct {
	FromUnixNS string `json:"from_unix_ns"`
	ToUnixNS   string `json:"to_unix_ns"`
}

type coverageObject struct {
	Complete             bool   `json:"complete"`
	Origin               string `json:"origin"`
	AvailableFromUnixNS  string `json:"available_from_unix_ns"`
	PriorHistoryComplete bool   `json:"prior_history_complete"`
}

type baselineObject struct {
	Generation               int64   `json:"generation"`
	ThreadID                 string  `json:"thread_id"`
	ObservedActiveFromUnixNS string  `json:"observed_active_from_unix_ns"`
	ObservedActiveToUnixNS   *string `json:"observed_active_to_unix_ns"`
	PriorHistoryComplete     bool    `json:"prior_history_complete"`
	BaselineDigest           string  `json:"baseline_digest"`
}

type headObject struct {
	Generation  int64  `json:"generation"`
	EpochDigest string `json:"epoch_digest"`
}

type epochObject struct {
	Generation          int64   `json:"generation"`
	ThreadID            string  `json:"thread_id"`
	EffectiveFromUnixNS string  `json:"effective_from_unix_ns"`
	EffectiveToUnixNS   *string `json:"effective_to_unix_ns"`
	TransitionKind      string  `json:"transition_kind"`
	Disposition         string  `json:"disposition"`
	PreviousEpochDigest *string `json:"previous_epoch_digest"`
	EpochDigest         string  `json:"epoch_digest"`
}

type response struct {
	Schema      string          `json:"schema"`
	Version     int             `json:"version"`
	Status      string          `json:"status"`
	Persona     string          `json:"persona"`
	TimeUnit    string          `json:"time_unit"`
	Request     rangeObject     `json:"request"`
	Coverage    coverageObject  `json:"coverage"`
	Baseline    *baselineObject `json:"baseline"`
	HistoryHead headObject      `json:"history_head"`
	Epochs      []epochObject   `json:"epochs"`
}

type errorResponse struct {
	Schema    string `json:"schema"`
	Version   int    `json:"version"`
	Status    string `json:"status"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(connectionTTL))
	if !s.ready.Load() {
		writeError(connection, "not_ready", true)
		return
	}
	raw, err := readOneLine(connection)
	if err != nil {
		writeError(connection, "invalid_request", false)
		return
	}
	var input request
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || !jsonEOF(decoder) {
		writeError(connection, "invalid_request", false)
		return
	}
	if input.Schema != requestSchema {
		writeError(connection, "invalid_request", false)
		return
	}
	if input.Version != protocolVersion {
		writeError(connection, "unsupported_version", false)
		return
	}
	fromNS, fromOK := canonicalUnixNS(input.FromUnixNS)
	toNS, toOK := canonicalUnixNS(input.ToUnixNS)
	if !fromOK || !toOK || toNS <= fromNS {
		writeError(connection, "invalid_request", false)
		return
	}
	history, err := s.reader.MainEpochHistory(fromNS, toNS)
	if err != nil {
		switch {
		case errors.Is(err, model.ErrHistoryIncomplete):
			writeError(connection, "history_incomplete", false)
		case errors.Is(err, model.ErrHistoryCorrupt):
			writeError(connection, "history_corrupt", false)
		case errors.Is(err, model.ErrHistoryBusy):
			writeError(connection, "busy", true)
		default:
			writeError(connection, "internal_unavailable", true)
		}
		return
	}
	if len(history.Epochs) > maxEpochs {
		writeError(connection, "response_too_large", false)
		return
	}
	output := encodeHistory(history, input.FromUnixNS, input.ToUnixNS)
	_ = json.NewEncoder(connection).Encode(output)
}

func readOneLine(reader io.Reader) ([]byte, error) {
	buffered := bufio.NewReader(io.LimitReader(reader, maxRequestBytes+1))
	line, err := buffered.ReadBytes('\n')
	if err != nil || len(line) > maxRequestBytes {
		return nil, fmt.Errorf("invalid request")
	}
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, fmt.Errorf("invalid request")
	}
	if buffered.Buffered() != 0 {
		return nil, fmt.Errorf("invalid request")
	}
	return bytes.TrimSpace(line), nil
}

func jsonEOF(decoder *json.Decoder) bool {
	var extra any
	return errors.Is(decoder.Decode(&extra), io.EOF)
}

func canonicalUnixNS(value string) (int64, bool) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func encodeHistory(history model.MainEpochHistory, fromNS, toNS string) response {
	output := response{
		Schema:   responseSchema,
		Version:  protocolVersion,
		Status:   "ok",
		Persona:  history.Persona,
		TimeUnit: "unix_ns_utc",
		Request:  rangeObject{FromUnixNS: fromNS, ToUnixNS: toNS},
		Coverage: coverageObject{
			Complete: true, Origin: history.Origin,
			AvailableFromUnixNS:  strconv.FormatInt(history.AvailableFromNS, 10),
			PriorHistoryComplete: history.PriorHistoryComplete,
		},
		HistoryHead: headObject{Generation: history.HeadGeneration, EpochDigest: history.HeadDigest},
		Epochs:      make([]epochObject, 0, len(history.Epochs)),
	}
	if history.Baseline != nil {
		output.Baseline = &baselineObject{
			Generation: history.Baseline.Generation, ThreadID: history.Baseline.ThreadID,
			ObservedActiveFromUnixNS: strconv.FormatInt(history.Baseline.ObservedFromNS, 10),
			ObservedActiveToUnixNS:   optionalUnixNS(history.Baseline.ObservedToNS),
			PriorHistoryComplete:     false,
			BaselineDigest:           history.Baseline.Digest,
		}
	}
	for _, epoch := range history.Epochs {
		output.Epochs = append(output.Epochs, epochObject{
			Generation: epoch.Generation, ThreadID: epoch.ThreadID,
			EffectiveFromUnixNS: strconv.FormatInt(epoch.EffectiveFromNS, 10),
			EffectiveToUnixNS:   optionalUnixNS(epoch.EffectiveToNS),
			TransitionKind:      epoch.TransitionKind, Disposition: "committed",
			PreviousEpochDigest: optionalString(epoch.PreviousDigest), EpochDigest: epoch.Digest,
		})
	}
	return output
}

func optionalUnixNS(value int64) *string {
	if value == 0 {
		return nil
	}
	result := strconv.FormatInt(value, 10)
	return &result
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	result := value
	return &result
}

func writeError(writer io.Writer, code string, retryable bool) {
	_ = json.NewEncoder(writer).Encode(errorResponse{
		Schema: errorSchema, Version: protocolVersion, Status: "error",
		Code: code, Retryable: retryable,
	})
}

func (s *Server) Close() error {
	s.ready.Store(false)
	if s.ln == nil {
		return nil
	}
	err := s.ln.Close()
	_ = os.Remove(s.path)
	return err
}
