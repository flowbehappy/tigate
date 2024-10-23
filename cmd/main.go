// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"

	"github.com/flowbehappy/tigate/cmd/cli"
	"github.com/flowbehappy/tigate/cmd/server"
	"github.com/flowbehappy/tigate/cmd/version"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/pingcap/log"
	tiflowCmd "github.com/pingcap/tiflow/pkg/cmd"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCmd creates the root command.
func NewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cdc",
		Short: "CDC",
		Long:  `Change Data Capture`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
}

func addNewArchCommandTo(cmd *cobra.Command) {
	cmd.AddCommand(server.NewCmdServer())
	cmd.AddCommand(cli.NewCmdCli())
	cmd.AddCommand(version.NewCmdVersion())
}

func isNewArchEnabledByConfig(serverConfigFilePath string) bool {
	cfg := config.GetDefaultServerConfig()
	if len(serverConfigFilePath) > 0 {
		// strict decode config file, but ignore debug item
		if err := util.StrictDecodeFile(serverConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem); err != nil {
			log.Error("failed to parse server configuration, please check the config file for errors and try again.", zap.Error(err))
			return false
		}
	}

	return cfg.Newarch
}

// Run runs the root command.
func main() {
	cmd := NewCmd()

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	newarch := false
	cmd.PersistentFlags().BoolVar(&newarch, "experimental", false, "Run TiGate (experimental feature)")
	var serverConfigFilePath string
	cmd.PersistentFlags().StringVar(&serverConfigFilePath, "config", "", "Path of the configuration file")
	cmd.PersistentFlags().Lookup("config").Hidden = true

	// The command-line flags are parsed in advance to check whether to launch the new architecture.
	cmd.ParseFlags(os.Args[1:])

	if newarch || isNewArchEnabledByConfig(serverConfigFilePath) {
		addNewArchCommandTo(cmd)
	} else {
		tiflowCmd.AddTiCDCCommandTo(cmd)
	}

	if err := cmd.Execute(); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}
