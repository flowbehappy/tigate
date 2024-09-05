// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package decoder

import (
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
)

// RowEventDecoder is an abstraction for events decoder
// this interface is only for testing now
type RowEventDecoder interface {
	// AddKeyValue add the received key and values to the decoder,
	// should be called before `HasNext`
	// decoder decode the key and value into the event format.
	AddKeyValue(key, value []byte) error

	// HasNext returns
	//     1. the type of the next event
	//     2. a bool if the next event is exist
	//     3. error
	HasNext() (model.MessageType, bool, error)
	// NextResolvedEvent returns the next resolved event if exists
	NextResolvedEvent() (uint64, error)
	// NextRowChangedEvent returns the next row changed event if exists
	NextRowChangedEvent() (*common.RowChangedEvent, error)
	// NextDDLEvent returns the next DDL event if exists
	NextDDLEvent() (*model.DDLEvent, error)
}
