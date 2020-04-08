package event

import (
	"encoding/base64"
	"reflect"
	"testing"
)

const (
	EXAMPLE1      = `{"Table":{"Schema":"db_cmdb","Name":"t_device_basic","Columns":["id","device_sn","ad_asset_number","op_asset_number","rfid","height","rated_power","maintenance_start","maintenance_end","dict_manufacturer_id","dict_vendor_id","device_model_id","device_model","dict_device_type_id","package_id","asset_source","asset_book","tag_number","input_method","app_id","arrival_at","created_at","updated_at","deleted_at","username","action_id","is_delete","is_sync"]},"RawData":[[2,"6CU528WWS7","ZB-03-FWQ-42319A","ZB2015-01-B6769","1",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:17","2000-01-01 00:00:00","system","system20190816",0,1],[2,"6CU528WWS7","ZB-03-FWQ-42319A","ZB2015-01-B6769","0",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:49","2000-01-01 00:00:00","system","system20190816",0,1]],"Type":"OnRow","Action":"update","Err":null,"DDLSql":""}`
	EXAMPLE2      = `{"Table":{"Schema":"db_cmdb","Name":"t_device_basic","Columns":["id","device_sn","ad_asset_number","op_asset_number","rfid","height","rated_power","maintenance_start","maintenance_end","dict_manufacturer_id","dict_vendor_id","device_model_id","device_model","dict_device_type_id","package_id","asset_source","asset_book","tag_number","input_method","app_id","arrival_at","created_at","updated_at","deleted_at","username","action_id","is_delete","is_sync"]},"RawData":[[1,"6CU528WWNW","ZB-03-FWQ-42318A","ZB2015-01-B6672","1",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:17","2000-01-01 00:00:00","system","system20190816",0,0],[1,"6CU528WWNW","ZB-03-FWQ-42318A","ZB2015-01-B6672","0",1,0,"2015-10-14 00:00:00","2018-07-20 00:00:00",12,0,150,"DL360 Gen9",57,0,0,"1035_FA","",1,0,"2015-10-14 00:00:00","2016-08-29 00:00:00","2020-04-08 11:01:49","2000-01-01 00:00:00","system","system20190816",0,0]],"Type":"OnRow","Action":"update","Err":null,"DDLSql":""}`
	COMPRESSINFO1 = `zJNBa+M+EMW/SnnnEYycxIl9a5t//5eyy7ZdChuCkG01MbVlrywnhNLvvlhOk273sNDTQg56v5l5mpnIL3jQWWWQvuA+35paI0WRqbwuMhC+6NoghVeF2ZW5UZnuyhyE66bqa9shXaEsQDiGOwuCLpTuOuOV7evMOBCa9iNxT6Fsa8rN1g9ae1OottmHaK1L643VdrD02vkPzNhwZ5l7VWvbP+nc9844VZ7wztiieQNjb3VTmOpP8lZxZP7QmjGp1fmz3hzF2H7X9C43J5k1zTMIXm/Og5W27b2qjd82oa5tjwbOlTtdKT3MkjsT5g2ib4uzKExlzpHOODv8AQSd+7Kxo1XZqTFtPHcHm2P9SrjT+6X2GulqFRHi6++zaPH4eD8H4ceV4Im4efwmptFEJpcBRSxngqW4iudxAoIESWJC4JKFnF4wp+GHQBeC5yLid1RGxCRnTFjeTmK++N/YBDSbEw9GkiczdTNc9jfnWPBCRMnvNGLBU8GLCylTlqmcB8o89MzyfW536LypT4eIZcILGWNobk2f2wb/29uYJp/bxprwcGiHb/qrvWv2IFyGp4X0+BBB+M85pLavKsJyeXv/s0IKvP4CAAD//w==`
	COMPRESSINFO2 = `zJPRT9swEMb/FfQ9n6Vz2qZp3oCOvSCmAVOlIWQ5iWkjEidznFYV4n+f4pSWsYdJPE3qg7/f3X2+uzovuNdZZZC+4C7fmFojRZGpvC4yEG50bZDCq8Jsy9yoTHdlDsJlU/W17ZA+oCxAOIQ7C4IulO4645Xt68w4EJr2I3FPoWxjyvXGD1p7U6i22YVorUvrjdV2sPTa+Q/M2HBnmXtVa9s/6dz3zjhVHvHW2KJ5A2NvdVOY6m/yVnFgft+aManV+bNeH8TYftf0LjdHmTXNMwher0+DlbbtvaqN3zShrm0PBs6VW10pPcySOxPmDaJvi5MoTGVOkc44O/wBBJ37srGjVdmpMW08d3ub4/GVcKt3S+010ocHSYgvf8yiZLW6WYHw80LwRFytvotpNJHJeUARy5lgKS7ieB6BIEGSmBC4ZCGnZ8xp+CHQRPBcRPyOyoiY5IwJy+tJzGdfjV2AZnPiwUjyZKauhsv+5RwLTkS0+JNGLHgqODmTMmWZynmgzEPPLN/ndvvOm/p4iFguOJExhjYe6XPb4P97G9PF57bxSLjft8M3/c3eNjsQzsPTQnp4iCB8cQ6p7auKsFxe3/2qkAKvvwEAAP//`
)

func TestEventV2Compress_00(t *testing.T) {
	e1 := &EventV2{}
	if err := e1.FromJSON([]byte(EXAMPLE1)); err != nil {
		t.Errorf("生成事件1出错: %s", err)
	}
	e2 := &EventV2{}
	if err := e2.FromJSON([]byte(EXAMPLE2)); err != nil {
		t.Errorf("生成事件2出错: %s", err)
	}
	e1c, err := e1.Compress()
	if err != nil {
		t.Errorf("压缩事件1出错: %s", err)
	}
	e2c, err := e2.Compress()
	if err != nil {
		t.Errorf("压缩事件2出错: %s", err)
	}
	if string(e1c) == string(e2c) && !reflect.DeepEqual(e1, e2) {
		t.Errorf("压缩结果相同,但事件本身不同")
	}
}

func TestEventV2DeCompress_00(t *testing.T) {
	e1 := &EventV2{}
	decoded, _ := base64.StdEncoding.DecodeString(COMPRESSINFO1)
	if err := e1.Decompress(decoded); err != nil {
		t.Errorf("事件1解压失败: %s", err)
	}
	e1json, _ := e1.ToJSON()
	if string(e1json) != EXAMPLE1 {
		t.Errorf("解压结果不对, got(%s), expected(%s)", string(e1json), EXAMPLE1)
	}
	e2 := &EventV2{}
	decoded, _ = base64.StdEncoding.DecodeString(COMPRESSINFO2)
	if err := e2.Decompress(decoded); err != nil {
		t.Errorf("事件2解压失败: %s", err)
	}
	e2json, _ := e2.ToJSON()
	if string(e2json) != EXAMPLE2 {
		t.Errorf("解压结果不对, got(%s), expected(%s)", string(e2json), EXAMPLE2)
	}

}
