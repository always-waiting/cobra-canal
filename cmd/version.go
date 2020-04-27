package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const (
	VERSION = "2.0.0"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of cmdb_cobra",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(VERSION)
	},
}
