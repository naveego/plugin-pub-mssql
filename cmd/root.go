// Copyright © 2017 Naveego

package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
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
			"./log.%Y%m%d%H%M%S",
			// rotatelogs.WithLinkName("./log"),
			rotatelogs.WithMaxAge(7 * 24 * time.Hour),
			rotatelogs.WithRotationTime(time.Hour),
		)
		if err != nil {
			recordCrashFile( fmt.Sprintf("Could not create log file: %s", err))
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
				recordCrashFile( fmt.Sprintf("Panic: %s", err))
				log.Error("panic", "error", err)
			}
		}()

		go func(){
			_, _ = ioutil.ReadAll(os.Stdin)
			_, _ = fmt.Fprintf(logf, "Stdin has been closed. This probably means the agent has exited. Plugin will now exit.\n")
			os.Exit(0)
		}()


		originalPPID := os.Getppid()
		go func(){
			for {
				<-time.After(5 * time.Second)
				ppid := os.Getppid()
				if ppid != originalPPID {
				_, _ = fmt.Fprintf(logf, "Parent process appears to have exited (ppid changed from %d to %d). This probably means the agent has exited. Plugin will now exit.\n",					originalPPID, ppid)
					os.Exit(0)
				}
			}
		}()

		go func(){
			sigCh := make(chan os.Signal)
			signal.Notify(sigCh, os.Interrupt, os.Kill)
			sig:=<-sigCh
			_, _ = fmt.Fprintf(logf, "Got %s signal. This probably means the agent wants us to exit. Plugin will now exit.\n", sig)
			os.Exit(0)
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

func recordCrashFile(message string) {
	startupFailureErrorPath := fmt.Sprintf("./crash-%d-%s.log", time.Now().Unix(), uuid.New().String())
	_ = ioutil.WriteFile(startupFailureErrorPath, []byte(message), 0666)
}