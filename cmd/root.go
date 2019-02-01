// Copyright © 2017 Naveego

package cmd

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/pkg/errors"
	"io"
	"os"
	"time"

	"github.com/hashicorp/go-plugin"
	"github.com/naveego/dataflow-contracts/plugins"
	"github.com/naveego/plugin-pub-mssql/internal"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/version"
	"github.com/spf13/cobra"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "plugin-pub-mssql",
	Short: "A publisher that pulls data from SQL.",
	Long: fmt.Sprintf(`Version %s
Runs the publisher in externally controlled mode.`, version.Version.String()),
	RunE: func(cmd *cobra.Command, args []string)  error {

		logf, err := rotatelogs.New(
			"./log.%Y%m%d%H%M",
			rotatelogs.WithLinkName("./log"),
			rotatelogs.WithMaxAge(24 * time.Hour),
			rotatelogs.WithRotationTime(time.Hour),
		)
		if err != nil {
			return errors.Wrap(err, "log file")
		}

		log := hclog.New(&hclog.LoggerOptions{
			Level:      hclog.Trace,
			Output:     io.MultiWriter(os.Stderr, logf),
			JSONFormat: true,
		})

		log.Info("Starting.")

		defer func(){
			if err := recover(); err != nil{
				log.Error("panic", "error", err)
			}
		}()

		server := internal.NewServer(log)

		plugin.Serve(&plugin.ServeConfig{
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion: plugins.PublisherProtocolVersion,
				MagicCookieKey:plugins.PublisherMagicCookieKey,
				MagicCookieValue:plugins.PublisherMagicCookieValue,
			},
			Plugins: map[string]plugin.Plugin{
				"publisher": pub.NewServerPlugin(server),
			},
			Logger: log,

			// A non-nil value here enables gRPC serving for this plugin...
			GRPCServer: plugin.DefaultGRPCServer,
		})

		return nil
	}}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	verbose = RootCmd.Flags().BoolP("verbose", "v", false, "enable verbose logging")
}
