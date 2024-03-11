package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"

	"github.com/acrlabs/prom2parquet/pkg/util"
)

const progname = "prom2parquet"

func rootCmd() *cobra.Command {
	opts := options{
		verbosity: log.InfoLevel,
	}

	root := &cobra.Command{
		Use:   progname,
		Short: "Prometheus remote write endpoint for saving Prometheus metrics to Parquet files",
		Run: func(_ *cobra.Command, _ []string) {
			start(&opts)
		},
	}

	root.PersistentFlags().IntVarP(
		&opts.port,
		serverPortFlag,
		"p",
		1234,
		"port for the remote write endpoint to listen on",
	)

	root.PersistentFlags().Var(
		enumflag.New(&opts.backend, backendFlag, supportedBackendIDs, enumflag.EnumCaseInsensitive),
		backendFlag,
		fmt.Sprintf(
			"supported remote backends for saving parquet files\n(valid options: %s)",
			validArgs(supportedBackendIDs),
		),
	)

	root.PersistentFlags().StringVar(
		&opts.backendRoot,
		backendRootFlag,
		"/data",
		"root path/location for the specified backend (e.g. bucket name for AWS S3)",
	)

	root.PersistentFlags().VarP(
		enumflag.New(&opts.verbosity, verbosityFlag, logLevelIDs, enumflag.EnumCaseInsensitive),
		verbosityFlag,
		"v",
		fmt.Sprintf("log level (valid options: %s)", validArgs(logLevelIDs)),
	)
	return root
}

func start(opts *options) {
	util.SetupLogging(opts.verbosity)
	log.Infof("running with options: %v", opts)

	server := newServer(opts)
	server.run()
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
