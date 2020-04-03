package errors

import (
	//"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/juju/errors"
)

type SentinelConfig struct {
	TypeId  string `toml:"type_id" json:"type_id"`
	LevelId string `toml:"level_id" json:"level_id"`
	Url     string `toml:"url" json:"url"`
	Token   string `toml:"token" json:"token"`
}

func (s *SentinelConfig) Send(doc string) (str string, err error) {
	str = "空"
	v := url.Values{}
	v.Set("type_id", s.TypeId)
	v.Set("level_id", s.LevelId)
	v.Set("message", doc)
	bodySend := strings.NewReader(v.Encode())
	request, err := http.NewRequest("POST", s.Url, bodySend)
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	request.Header.Set("Authorization", "token "+s.Token)
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		str = string(body)
		sendMap := map[string]string{"type_id": s.TypeId, "level_id": s.LevelId, "message": doc}
		respMap := make(map[string]string)
		err = json.Unmarshal(body, &respMap)
		if err != nil {
			return
		} else {
			flag := reflect.DeepEqual(sendMap, respMap)
			if flag {
				return
			} else {
				err = errors.Errorf("报告哨兵出错: %s", str)
				return
			}
		}
	}
}
