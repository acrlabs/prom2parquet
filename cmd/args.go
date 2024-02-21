package main

import (
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/thediveo/enumflag/v2"
)

type RemoteEndpoint enumflag.Flag

const (
	None RemoteEndpoint = iota
	S3
)

//nolint:gochecknoglobals
var supportedRemoteIDs = map[RemoteEndpoint][]string{
	None: {"none"},
	S3:   {"s3", "aws"},
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

type Options struct {
	port      int
	remote    RemoteEndpoint
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
