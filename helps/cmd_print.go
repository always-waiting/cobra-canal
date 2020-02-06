package helps

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func CmdPrint(resp *http.Response) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
