package main

import (
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/acrlabs/prom2parquet/pkg/backends"
)

const (
	prefixFlag      = "prefix"
	serverPortFlag  = "server-port"
	backendFlag     = "backend"
	backendRootFlag = "backend-root"
	verbosityFlag   = "verbosity"
)

//nolint:gochecknoglobals
var supportedBackendIDs = map[backends.StorageBackend][]string{
	backends.Local: {"local"},
	backends.S3:    {"s3", "aws"},
}

//nolint:gochecknoglobals
var logLevelIDs = map[log.Level][]string{
	log.TraceLevel: {"trace"},
	log.DebugLevel: {"debug"},
	log.InfoLevel:  {"info"},
	log.WarnLevel:  {"warning", "warn"},
	log.ErrorLevel: {"error"},
	log.FatalLevel: {"fatal"},
	log.PanicLevel: {"panic"},
}

type options struct {
	port int

	backend     backends.StorageBackend
	backendRoot string

	verbosity log.Level
}

func validArgs[K comparable](supportedIDs map[K][]string) string {
	valids := []string{}
	for _, l := range supportedIDs {
		valids = append(valids, strings.Join(l, "/"))
	}
	sort.Strings(valids)
	return strings.Join(valids, ", ")
}
