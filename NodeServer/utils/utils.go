package utils

import "unsafe"

func SayHello() string {
	return "hello"
}

const (
	INDEXSTORE_NAME = iota
	IDS_STORE_NAME
	EDGESTORE_NAME
	SYSTEM_PROPERTIES_STORE_NAME
	SYSTEM_MGMT_LOG_NAME
	SYSTEM_TX_LOG_NAME
	LOCK_STORE_SUFFIX
)

var janusCfMap = map[int]string{
	INDEXSTORE_NAME: "g",
	INDEXSTORE_NAME + LOCK_STORE_SUFFIX: "h",
	IDS_STORE_NAME: "i",
	EDGESTORE_NAME: "e",
	EDGESTORE_NAME + LOCK_STORE_SUFFIX: "f",
	SYSTEM_PROPERTIES_STORE_NAME: "s",
	SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX: "t",
	SYSTEM_MGMT_LOG_NAME: "m",
	SYSTEM_TX_LOG_NAME: "l",
}

func GetShortCf(key int) string {
	if v, ok := janusCfMap[key]; ok {
		return v
	}
	return "not found"
}

func GetShortCfMapCopy() map[int]string {
	res := map[int]string{}
	for k, v := range janusCfMap{
		res[k] = v
	}
	return res
}

func unsafeGetBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func MapValStrToBytes(m map[string]string) map[string][]byte {
	var res = map[string][]byte{}
	for k, v :=range m {
		res[k] = unsafeGetBytes(v)
	}
	return res
}