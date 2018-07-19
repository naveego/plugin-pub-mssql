// +build mage

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
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
	mg.Deps(InstallDeps)
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

	//ver := fmt.Sprintf(`"version": "%s",`, version.Version.String())

	// manifest, err := replaceInFile("manifest.json", `"version":.*,`, ver)
	// if err != nil {
	// 	return err
	// }
	// defer ioutil.WriteFile("manifest.json", []byte(manifest), 0777)

	for _, os := range oses {
		if err := buildAndPublish(os); err != nil {
			return err
		}
	}

	return nil
}

func buildAndPublish(os string) error {
	defer sh.Rm("plugin-pub-mssql.exe")
	defer sh.Rm("package.zip")

	env := map[string]string{
		"GOOS":        os,
		"CGO_ENABLED": "0",
	}

	if err := sh.RunWith(env, "go", "build", "-o", "plugin-pub-mssql.exe", "."); err != nil {
		return err
	}

	if err := sh.Run("navget-cli", "publish", "--os", os, "-f", "plugin-pub-mssql.exe icon.png"); err != nil {
		log.Printf("Error publishing plugin. Will wait 5 seconds and try again. Error was: %s", err)
		<-time.After(time.Second * 5)
		if err = sh.Run("navget-cli", "publish", "--os", os, "-f", "plugin-pub-mssql.exe icon.png"); err != nil {
			return err
		}
		return err
	}

	return nil
}

func replaceInFile(file, regex, replacement string) (string, error) {
	input, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile(regex)

	output := re.ReplaceAllString(string(input), replacement)

	err = ioutil.WriteFile(file, []byte(output), 0644)
	return string(input), err
}


// Clean up after yourself
func Clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("bin")
}