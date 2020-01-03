package rule

import (
	"fmt"
	"strings"

	"github.com/always-waiting/cobra-canal/rules"
	"github.com/spf13/cobra"
)

const (
	LIST_CMD_TEMPLATE = "%-30s\t%-15s"
)

var listCmd = &cobra.Command{
	Use:     "list",
	Short:   "展示可以使用的规则",
	Version: "1.0.0",
	Run:     listCmdRun,
}

func listCmdRun(cmd *cobra.Command, args []string) {
	infos := rules.GetRuleMakerBaseInfo()
	header := []interface{}{"Rulename", "Description"}
	rets := []string{fmt.Sprintf(LIST_CMD_TEMPLATE, header...)}
	for _, info := range infos {
		rets = append(rets, fmt.Sprintf(LIST_CMD_TEMPLATE, info...))
	}
	fmt.Printf("%s\n", strings.Join(rets, "\n"))
}
