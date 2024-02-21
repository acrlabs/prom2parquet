package util

import (
	"fmt"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

func SetupLogging(level log.Level) {
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

	log.SetLevel(level)
	log.SetReportCaller(true)
}
