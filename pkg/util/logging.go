package util

import (
	"fmt"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

//nolint:gochecknoglobals
var logLevels = []log.Level{
	log.ErrorLevel,
	log.WarnLevel,
	log.InfoLevel,
}

func SetupLogging(level int) {
	prettyfier := func(f *runtime.Frame) (string, string) {
		// Build with -trimpath to hide info about the devel environment
		// Strip off the leading package name for "pretty" output
		filename := strings.SplitN(f.File, "/", 2)[1]
		return f.Function, fmt.Sprintf("%s:%d", filename, f.Line)
	}
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    true,
		PadLevelText:     true,
		CallerPrettyfier: prettyfier,
	})

	if level >= len(logLevels) {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(logLevels[level])
	}
	log.SetReportCaller(true)
}
