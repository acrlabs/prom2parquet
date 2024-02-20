package main

import (
	"os"

	"github.com/acrlabs/prom2parquet/pkg/util"
	"github.com/spf13/cobra"
)

const (
	progname = "prom2parquet"

	verbosityFlag  = "verbosity"
	serverPortFlag = "serverPort"
)

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   progname,
		Short: "Prometheus remote write endpoint for writing Parquet files",
		Run:   start,
	}

	root.PersistentFlags().IntP(verbosityFlag, "v", 2, "log level output (higher is more verbose)")
	root.PersistentFlags().Int(serverPortFlag, 1234, "port for the remote write endpoint to listen on")
	return root
}

func start(cmd *cobra.Command, _ []string) {
	level, err := cmd.PersistentFlags().GetInt(verbosityFlag)
	if err != nil {
		panic(err)
	}

	port, err := cmd.PersistentFlags().GetInt(serverPortFlag)
	if err != nil {
		panic(err)
	}

	util.SetupLogging(level)

	server := newServer(port)
	server.run()
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
