// // Copyright 2022 PingCAP, Inc.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // See the License for the specific language governing permissions and
// // limitations under the License.

package canal

// import (
// 	"github.com/pingcap/tiflow/cdc/model"
// 	canal "github.com/pingcap/tiflow/proto/canal"
// )

const tidbWaterMarkType = "TIDB_WATERMARK"

// // The TiCDC Canal-JSON implementation extend the official format with a TiDB extension field.
// // canalJSONMessageInterface is used to support this without affect the original format.
type canalJSONMessageInterface interface {
	// getSchema() *string
	// getTable() *string
	// getCommitTs() uint64
	// getQuery() string
	// getOld() map[string]interface{}
	// getData() map[string]interface{}
	// getMySQLType() map[string]string
	// getJavaSQLType() map[string]int32
	// messageType() model.MessageType
	// eventType() canal.EventType
	// pkNameSet() map[string]struct{}
}

// JSONMessage adapted from https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/protocol/src/main/java/com/alibaba/otter/canal/protocol/FlatMessage.java#L1
type JSONMessage struct {
	// ignored by consumers
	ID        int64    `json:"id"`
	Schema    string   `json:"database"`
	Table     string   `json:"table"`
	PKNames   []string `json:"pkNames"`
	IsDDL     bool     `json:"isDdl"`
	EventType string   `json:"type"`
	// officially the timestamp of the event-time of the message, in milliseconds since Epoch.
	ExecutionTime int64 `json:"es"`
	// officially the timestamp of building the message, in milliseconds since Epoch.
	BuildTime int64 `json:"ts"`
	// SQL that generated the change event, DDL or Query
	Query string `json:"sql"`
	// only works for INSERT / UPDATE / DELETE events, records each column's java representation type.
	SQLType map[string]int32 `json:"sqlType"`
	// only works for INSERT / UPDATE / DELETE events, records each column's mysql representation type.
	MySQLType map[string]string `json:"mysqlType"`
	// A Datum should be a string or nil
	Data []map[string]interface{} `json:"data"`
	Old  []map[string]interface{} `json:"old"`
}

// func (c *JSONMessage) getSchema() *string {
// 	return &c.Schema
// }

// func (c *JSONMessage) getTable() *string {
// 	return &c.Table
// }

// // for JSONMessage, we lost the commitTs.
// func (c *JSONMessage) getCommitTs() uint64 {
// 	return 0
// }

// func (c *JSONMessage) getQuery() string {
// 	return c.Query
// }

// func (c *JSONMessage) getOld() map[string]interface{} {
// 	if c.Old == nil {
// 		return nil
// 	}
// 	return c.Old[0]
// }

// func (c *JSONMessage) getData() map[string]interface{} {
// 	if c.Data == nil {
// 		return nil
// 	}
// 	return c.Data[0]
// }

// func (c *JSONMessage) getMySQLType() map[string]string {
// 	return c.MySQLType
// }

// func (c *JSONMessage) getJavaSQLType() map[string]int32 {
// 	return c.SQLType
// }

// func (c *JSONMessage) messageType() model.MessageType {
// 	if c.IsDDL {
// 		return model.MessageTypeDDL
// 	}

// 	if c.EventType == tidbWaterMarkType {
// 		return model.MessageTypeResolved
// 	}

// 	return model.MessageTypeRow
// }

// func (c *JSONMessage) eventType() canal.EventType {
// 	return canal.EventType(canal.EventType_value[c.EventType])
// }

// func (c *JSONMessage) pkNameSet() map[string]struct{} {
// 	result := make(map[string]struct{}, len(c.PKNames))
// 	for _, item := range c.PKNames {
// 		result[item] = struct{}{}
// 	}
// 	return result
// }

type tidbExtension struct {
	CommitTs           uint64 `json:"commitTs,omitempty"`
	WatermarkTs        uint64 `json:"watermarkTs,omitempty"`
	OnlyHandleKey      bool   `json:"onlyHandleKey,omitempty"`
	ClaimCheckLocation string `json:"claimCheckLocation,omitempty"`
}

type canalJSONMessageWithTiDBExtension struct {
	*JSONMessage
	// Extensions is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event,
	// which is useful if the message consumer needs to restore the original transactions.
	Extensions *tidbExtension `json:"_tidb"`
}

// func (c *canalJSONMessageWithTiDBExtension) getCommitTs() uint64 {
// 	return c.Extensions.CommitTs
// }
