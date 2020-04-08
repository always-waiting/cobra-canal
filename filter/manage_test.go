package filter

import (
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
	"os"
)

const (
	EXAMPLE1 = `{"Table":{"Schema":"db_cmdb","Name":"t_device_basic","Columns":["id","device_sn","ad_asset_number","op_asset_number","rfid","height","rated_power","maintenance_start","maintenance_end","dict_manufacturer_id","dict_vendor_id","device_model_id","device_model","dict_device_type_id","package_id","asset_source","asset_book","tag_number","input_method","app_id","arrival_at","created_at","updated_at","deleted_at","username","action_id","is_delete","is_sync"]},"RawData":[[2,"6CU528WWS7","ZB-03-FWQ-42319A","ZB2015-01-B6769","1",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:17","2000-01-01 00:00:00","system","system20190816",0,1],[2,"6CU528WWS7","ZB-03-FWQ-42319A","ZB2015-01-B6769","0",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:49","2000-01-01 00:00:00","system","system20190816",0,1]],"Type":"OnRow","Action":"update","Err":null,"DDLSql":""}`
	EXAMPLE2 = `{"Table":{"Schema":"db_cmdb","Name":"t_device_basic","Columns":["id","device_sn","ad_asset_number","op_asset_number","rfid","height","rated_power","maintenance_start","maintenance_end","dict_manufacturer_id","dict_vendor_id","device_model_id","device_model","dict_device_type_id","package_id","asset_source","asset_book","tag_number","input_method","app_id","arrival_at","created_at","updated_at","deleted_at","username","action_id","is_delete","is_sync"]},"RawData":[[1,"6CU528WWNW","ZB-03-FWQ-42318A","ZB2015-01-B6672","1",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:17","2000-01-01 00:00:00","system","system20190816",0,0],[1,"6CU528WWNW","ZB-03-FWQ-42318A","ZB2015-01-B6672","0",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:49","2000-01-01 00:00:00","system","system20190816",0,0]],"Type":"OnRow","Action":"update","Err":null,"DDLSql":""}`
	EXAMPLE3 = `{"Table":{"Schema":"db_cmdb","Name":"t_device_config","Columns":["id","device_sn","device_name","os_version","os_kernel","install_params","hostname","last_install_at","remarks","op_remarks","pending_asset","budget_type","created_at","updated_at","deleted_at","username","action_id","is_delete","is_sync","original_cost","percent_salvage_value","salvage_value","current_cost","deprn_amount","deprn_reserve","retire_cost","net_value"]},"RawData":[[1,"6CU528WWNW",""," CentOS release 6.6 (Final)"," 2.6.32-504.16.2.el6.x86_64","","","0000-00-00 00:00:00","","",1,1,"2018-03-07 09:11:29","2019-08-28 14:22:46","2000-01-01 00:00:00","","",0,1,0,0,0,0,0,0,0,0],[1,"6CU528WWNW",""," CentOS release 6.6 (Final)"," 2.6.32-504.16.2.el6.x86_64","","","0000-00-00 00:00:00","a","",1,1,"2018-03-07 09:11:29","2020-04-09 15:31:42","2000-01-01 00:00:00","","",0,1,0,0,0,0,0,0,0,0]],"Type":"OnRow","Action":"update","Err":null,"DDLSql":""}`
	Cfg00    = "00"
)

var (
	confMap = map[string]string{
		Cfg00: "../examples/config/00-example.toml",
	}
	cfgMark string
)

func TestMain(m *testing.M) {
	flag.StringVar(&cfgMark, "cfgmark", "", "配置文件标记")
	flag.Parse()
	if cfgMark == "" {
		fmt.Println("输入配置文件标记")
		os.Exit(1)
	}
	filename, ok := confMap[cfgMark]
	if !ok {
		fmt.Printf("没有定义配置文件标记%s\n", cfgMark)
		os.Exit(1)
	}
	err := config.LoadTestCfg(filename)
	if err != nil {
		fmt.Printf("配置加载错误: %s\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestManager(t *testing.T) {
	if cfgMark == Cfg00 {
		//testManager_Cfg00_1(t)
		testManager_Cfg00_2(t)
	}
}

func testManager_Cfg00_1(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManager(ruleCfg)
	if err != nil {
		t.Errorf("创建filter manager失败: %s", err)
	}
	if manager.Name() != "filter_1_filtername" {
		t.Errorf("filter manager的名字解析失败, got(%s), expected(%s)", manager.Name(), "filter_1_filtername")
	}
	{
		e1 := event.EventV2{
			Table: &event.Table{Schema: "db", Name: "name"},
		}
		if manager.IsTablePass(e1) {
			t.Errorf("%s通过了基础表过滤", e1)
		}

		e2 := event.EventV2{
			Table: &event.Table{Schema: "db_cmdb", Name: "t_device_basic"},
		}
		if !manager.IsTablePass(e2) {
			t.Errorf("%s没有通过了基础表过滤", e2)
		}
	}
	manager.Close()
}

func testManager_Cfg00_2(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManager(ruleCfg)
	if err != nil {
		t.Errorf("创建filter manager失败: %s", err)
	}
	err = manager.SetNextManager()
	if err != nil {
		t.Errorf("创建下游队列失败: %s", err)
	}
	if err = manager.SetAggregator(); err != nil {
		t.Errorf("创建聚合器失败: %s", err)
	}
	if err := manager.SetWorker(); err != nil {
		t.Errorf("生成worker失败: %s", err)
	}
	{
		for i := 0; i < 2; i++ {
			e1 := event.EventV2{}
			e1.FromJSON([]byte(EXAMPLE1))
			if err := manager.Push(e1); err != nil {
				t.Errorf("放入事件失败: %s", err)
			}
			e2 := event.EventV2{}
			e2.FromJSON([]byte(EXAMPLE2))
			if err := manager.Push(e2); err != nil {
				t.Errorf("放入事件失败: %s", err)
			}
		}
		for i := 0; i < 1; i++ {
			e3 := event.EventV2{}
			e3.FromJSON([]byte(EXAMPLE3))
			if err := manager.Push(e3); err != nil {
				t.Errorf("放入事件失败: %s", err)
			}
		}
	}
	manager.Start()
	time.Sleep(5 * time.Second)
	manager.Close()
}
