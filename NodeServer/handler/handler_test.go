package handler

import (
	"Puzzle/Storage"
	"Puzzle/conf"
	"Puzzle/idl"
	"context"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var h *Handler

var testSetValReqs = []*idl.SetValuesRequest{{
	Cells:[]*idl.HCell{
		{
			Row:    []byte("12"),
			ColumnFamily: []byte("e"),
			Column: []byte("col1"),
			Value:  []byte("val1"),
		},
	}},{
	Cells:[]*idl.HCell{{
		Row:    []byte("12"),
		ColumnFamily:[]byte("i"),
		Column: []byte("col2"),
		Value:  []byte("val2"),
	},
	}},
}

var testGetRowReqs = []*idl.GetRowRequest{
	{
		Key:[]byte("12"),
	},
	{
		Key:[]byte("888"),
	},
}

func TestHandler_SetValue(t *testing.T) {
	res, err := h.SetValues(context.Background(), testSetValReqs[0])
	assert.Nil(t, err)
	assert.Equal(t, res.Message, "ok")
	res, err = h.SetValues(context.Background(), testSetValReqs[1])
	assert.Nil(t, err)
	assert.Equal(t, res.Code, int32(0))
}

func TestHandler_GetRow(t *testing.T) {
	res, err := h.GetRow(context.Background(), testGetRowReqs[0])
	assert.Nil(t, err)
	expectedRes := &idl.GetRowResponse{
		Code:0,
		Message:"ok",
		Result: []*idl.HCell{testSetValReqs[0].Cells[0], testSetValReqs[1].Cells[0]},
	}
	assert.EqualValues(t, res, expectedRes)
	res, err = h.GetRow(context.Background(), testGetRowReqs[1])
	assert.Nil(t, err)
	assert.Equal(t, res, &idl.GetRowResponse{Code:1, Message:"no records in cache", Result:nil})
}

func TestMain(m *testing.M) {
	testConf, _ := conf.LoadConf("")
	storageService := Storage.NewStorageService(testConf)
	h = &Handler{StorageService: storageService}
	val := m.Run()
	os.Exit(val)
}
