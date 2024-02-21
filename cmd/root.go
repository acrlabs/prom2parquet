package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/thediveo/enumflag/v2"

	"github.com/acrlabs/prom2parquet/pkg/util"
)

const (
	progname = "prom2parquet"

	serverPortFlag = "server-port"
	remoteFlag     = "remote"
	verbosityFlag  = "verbosity"
)

func rootCmd() *cobra.Command {
	opts := Options{
		verbosity: log.InfoLevel,
	}

	root := &cobra.Command{
		Use:   progname,
		Short: "Prometheus remote write endpoint for writing Parquet files",
		Run: func(_ *cobra.Command, _ []string) {
			start(opts)
		},
	}

	root.PersistentFlags().IntP(serverPortFlag, "p", 1234, "port for the remote write endpoint to listen on")
	root.PersistentFlags().Var(
		enumflag.New(&opts.remote, remoteFlag, supportedRemoteIDs, enumflag.EnumCaseInsensitive),
		remoteFlag,
		fmt.Sprintf(
			"supported remote endpoints for saving parquet files\n(valid options: %s)",
			validArgs(supportedRemoteIDs),
		),
	)
	root.PersistentFlags().VarP(
		enumflag.New(&opts.verbosity, verbosityFlag, logLevelIDs, enumflag.EnumCaseInsensitive),
		verbosityFlag, "v",
		fmt.Sprintf("log level (valid options: %s)", validArgs(logLevelIDs)),
	)
	return root
}

func start(opts Options) {
	util.SetupLogging(opts.verbosity)

	server := newServer(opts.port)
	server.run()
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
