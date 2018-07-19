// +build mage

package main

import (
	"fmt"
	"os"
	"github.com/magefile/mage/sh"
	"github.com/naveego/dataflow-contracts/plugins"

	"github.com/naveego/ci/go/build"
)

var oses = []string{
	"windows",
	"linux",
	"darwin",
}

// Default target to run when none is specified
// If not set, running mage will list available targets
// var Default = Build

// A build step that requires additional params, or platform specific steps for example
func Build() error {
	fmt.Println("Building...")
	for _, os := range oses {
		if err := buildForOS(os); err != nil {
			return err
		}
	}
	return nil
}

func buildForOS(os string) error {
	fmt.Println("Building for OS", os)
	return sh.RunWith(map[string]string{
		"GOOS": os,
	}, "go", "build", "-o", "bin/"+os+"/plugin-pub-mssql", ".")
}

func PublishToNavget() error {

	for _, os := range oses {
		if err := buildAndPublish(os); err != nil {
			return err
		}
	}

	return nil
}

func buildAndPublish(os string) error {

	navget, err := build.NewNavgetClient()
	if err != nil {
		return err
	}

	defer sh.Rm("plugin-pub-mssql")

	env := map[string]string{
		"GOOS":        os,
		"CGO_ENABLED": "0",
	}

	if err := sh.RunWith(env, "go", "build", "-o", "plugin-pub-mssql", "."); err != nil {
		return err
	}

	err = navget.Upload(build.NavgetParams{
		Arch:"amd64",
		OS:os,
		Files:[]string{"plugin-pub-mssql", "icon.png"},
	})

	return err
}



// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("bin")
}



func GenerateGRPC() error {
	destDir := "./internal/pub"
	return plugins.GeneratePublisher(destDir)
}