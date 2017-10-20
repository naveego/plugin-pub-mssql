// Copyright © 2017 Naveego

package cmd

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/naveego/navigator-go/publishers/server"
	"github.com/spf13/cobra"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "mssql",
	Short: "A publisher that pulls data from MS SQL Server",
	Args:  cobra.ExactArgs(1),
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {

		logrus.SetOutput(os.Stdout)

		addr := args[0]

		if *verbose {
			fmt.Println("Verbose logging")
			logrus.SetLevel(logrus.DebugLevel)
		}

		publisher := NewClient()

		srv := server.NewPublisherServer(addr, publisher)

		err := srv.ListenAndServe()

		return err
	}}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	verbose = RootCmd.Flags().BoolP("verbose", "v", false, "enable verbose logging")
}
