package helps

import (
	"encoding/json"
	"net/http"
)

const (
	HTTPSUCCESS = "success"
	HTTPFAIL    = "fail"
)

type StdReturn struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Err     interface{} `json:"err,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (this StdReturn) Json(req *http.Request) ([]byte, error) {
	q := req.URL.Query()
	_, flag := q["pretty"]
	if flag {
		return json.MarshalIndent(this, "", "\t")
	}
	return json.Marshal(this)
}
