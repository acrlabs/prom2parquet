package main

import (
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/acrlabs/prom2parquet/pkg/remotes"
)

const (
	prefixFlag            = "prefix"
	serverPortFlag        = "server-port"
	cleanLocalStorageFlag = "clean-local-storage"
	remoteFlag            = "remote"
	verbosityFlag         = "verbosity"
)

//nolint:gochecknoglobals
var supportedRemoteIDs = map[remotes.Endpoint][]string{
	remotes.None: {"none"},
	remotes.S3:   {"s3", "aws"},
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
	prefix string
	port   int

	cleanLocalStorage bool
	remote            remotes.Endpoint

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
