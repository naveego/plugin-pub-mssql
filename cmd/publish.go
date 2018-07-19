// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/naveego/navigator-go/publishers/protocol"
)

// publishCmd represents the publish command
var publishCmd = &cobra.Command{
	Use:   "publish [--file file | --config json]",
	Short: "Runs a publish using the specified configuration.",
	Long:  `.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("publish called")
	},
}

func init() {
	RootCmd.AddCommand(publishCmd)

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// publishCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type encoderClient struct {
	encoder json.Encoder
}

func (s encoderClient) SendDataPoints(sendRequest protocol.SendDataPointsRequest) (protocol.SendDataPointsResponse, error) {
	s.encoder.Encode(sendRequest.DataPoints)
	return protocol.SendDataPointsResponse{}, nil
}

func (s encoderClient) Done(r protocol.DoneRequest) (protocol.DoneResponse, error) {
	s.encoder.Encode(r)
	return protocol.DoneResponse{}, nil
}
