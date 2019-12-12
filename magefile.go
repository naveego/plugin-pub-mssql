// +build mage

package main

import (
	"github.com/magefile/mage/sh"
	"log"
	"path/filepath"

	"github.com/naveego/ci/go/build"
	"github.com/naveego/plugin-pub-mssql/version"
)

func Build() error {
	cfg := build.PluginConfig{
		Package: build.Package{
			VersionString: version.Version.String(),
			PackagePath:   "github.com/naveego/plugin-pub-mssql",
			Name:          "plugin-pub-mssql",
			Shrink:        true,
		},
		Targets: []build.PackageTarget{
			build.TargetLinuxAmd64,
			build.TargetDarwinAmd64,
			build.TargetWindowsAmd64,
		},
	}

	err := build.BuildPlugin(cfg)
	return err
}

// Generates the client for the plugin using the dataflow-contracts/plugins grpc.
func GenerateGRPC() error {

	toDir := "./internal/pub"
	dcDir := getDataflowContractsDir()

	fromDir := filepath.Join(dcDir, "plugins")

	err := sh.RunV("protoc",
		"-I",
		fromDir,
		"--go_out=plugins=grpc:"+toDir,
		"publisher.proto")

	return err
}

func getDataflowContractsDir() string {
	dcDir, err := sh.Output("bosun", "app", "repo-path", "dataflow-contracts")
	if err != nil {
		log.Fatal(err)
	}
	return dcDir
}
