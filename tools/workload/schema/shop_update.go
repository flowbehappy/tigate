// Copyright 2024 PingCAP, Inc.
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

package schema

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type ShopItemWorkloadU struct {
	r             *rand.Rand
	jsonFieldSize int
	totalRow      uint64
}

func NewShopItemWorkloadU(totalCount uint64, jsonFieldSize int) Workload {
	return &ShopItemWorkloadU{
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		jsonFieldSize: jsonFieldSize,
		totalRow:      totalCount,
	}
}

func (s *ShopItemWorkloadU) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createShopItemTable, n)
}

func (s *ShopItemWorkloadU) BuildInsertSql(tableN int, rowCount int) string {
	tableName := fmt.Sprintf("shop_item_%d", tableN)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", tableName))

	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		row := s.generateRow()
		sb.WriteString(fmt.Sprintf("(%s)", row))
	}

	return sb.String()
}

func (s *ShopItemWorkloadU) BuildUpdateSql(opt UpdateOption) string {
	return s.buildUpsertSql(opt)
	// return s.buildUpdateSql(opt)
}

func (s *ShopItemWorkloadU) buildUpsertSql(opt UpdateOption) string {
	tableName := fmt.Sprintf("shop_item_%d", opt.Table)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", tableName))

	for i := 0; i < opt.RowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		row := s.generateRow()
		sb.WriteString(fmt.Sprintf("(%s)", row))
	}

	sb.WriteString(" ON DUPLICATE KEY UPDATE item_id=VALUES(item_id)")
	return sb.String()
}

func (s *ShopItemWorkloadU) buildUpdateSql(opt UpdateOption) string {
	tableName := fmt.Sprintf("0x%x", uint64(s.r.Int63())%s.totalRow)
	var sb strings.Builder

	for i := 0; i < opt.RowCount; i++ {
		if i > 0 {
			sb.WriteString(";")
		}
		primaryKey := "0xdeadbeef" // Fixed value for item_primary_key
		sb.WriteString(fmt.Sprintf("UPDATE %s SET updated_time = updated_time + 1 WHERE item_primary_key = '%s'", tableName, primaryKey))
	}

	return sb.String()
}

func (s *ShopItemWorkloadU) generateRow() string {
	primaryKey := fmt.Sprintf("0x%x", uint64(rand.Int63())%s.totalRow)
	itemID := "fixed_item_id"                         // Fixed value for item_id
	itemSetID := "fixed_item_set_id"                  // Fixed value for item_set_id
	productID := "fixed_product_id"                   // Fixed value for product_id
	productSetID := "fixed_product_set_id"            // Fixed value for product_set_id
	country := "US"                                   // Fixed value for point_of_sale_country
	merchantID := 123456                              // Fixed value for merchant_id
	merchantItemID := "fixed_merchant_item_id"        // Fixed value for merchant_item_id
	merchantItemSetID := "fixed_merchant_item_set_id" // Fixed value for merchant_item_set_id
	jsonField := randomJSONString(s.jsonFieldSize)
	timestamps := randomJSONString(s.jsonFieldSize)

	return fmt.Sprintf("'%s','%s','%s','%s','%s','%s',%d,'%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%d,'%s'",
		primaryKey, itemID, itemSetID, productID, productSetID, country,
		merchantID, merchantItemID, merchantItemSetID,
		jsonField, jsonField, jsonField, jsonField, jsonField,
		jsonField, jsonField, jsonField, jsonField, jsonField, jsonField,
		rand.Int63(), rand.Int63(), 0, timestamps)
}
