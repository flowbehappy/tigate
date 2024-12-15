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

	"github.com/google/uuid"
)

const createShopItemTable = `
CREATE TABLE if not exists shop_item_%d (
  item_primary_key varbinary(255) NOT NULL,
  item_id varchar(45) DEFAULT NULL,
  item_set_id varchar(45) DEFAULT NULL,
  product_id varchar(45) DEFAULT NULL,
  product_set_id varchar(45) DEFAULT NULL,
  point_of_sale_country varchar(2) DEFAULT NULL,
  merchant_id bigint(20) DEFAULT NULL,
  merchant_item_id varchar(127) DEFAULT NULL,
  merchant_item_set_id varchar(127) DEFAULT NULL,
  domains json DEFAULT NULL,
  product_sources json DEFAULT NULL,
  image_signatures json DEFAULT NULL,
  normalized_short_link_clusters json DEFAULT NULL,
  canonical_links json DEFAULT NULL,
  feed_item_ids json DEFAULT NULL,
  feed_profile_ids json DEFAULT NULL,
  reconciled_data json DEFAULT NULL,
  source_data json DEFAULT NULL,
  cdc_change_indicator json DEFAULT NULL,
  cdc_new_values json DEFAULT NULL,
  cdc_old_values json DEFAULT NULL,
  created_time bigint(20) DEFAULT NULL,
  arrival_time bigint(20) DEFAULT NULL,
  updated_time bigint(20) DEFAULT NULL,
  timestamp_data json DEFAULT NULL,
  PRIMARY KEY (item_primary_key)
  /*T![clustered_index] CLUSTERED */ 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PRE_SPLIT_REGIONS = 8   
`

type ShopItemWorkload struct {
	jsonFieldSize int
	totalRow      uint64
}

func NewShopItemWorkload(totalCount uint64, rowSize int) Workload {
	jsonFieldSize := rowSize / 13
	return &ShopItemWorkload{
		jsonFieldSize: jsonFieldSize,
		totalRow:      totalCount,
	}
}

func (s *ShopItemWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createShopItemTable, n)
}

func (s *ShopItemWorkload) BuildInsertSql(tableN int, rowCount int) string {
	tableName := fmt.Sprintf("shop_item_%d", tableN)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", tableName))

	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		row := s.generateRow(i, false)
		sb.WriteString(fmt.Sprintf("(%s)", row))
	}

	return sb.String()
}

func (s *ShopItemWorkload) BuildUpdateSql(opt UpdateOption) string {
	// return s.buildUpsertSql(opt)
	return s.buildUpdateSql(opt)
}

func (s *ShopItemWorkload) buildUpsertSql(opt UpdateOption) string {
	tableName := fmt.Sprintf("shop_item_%d", opt.Table)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES ", tableName))

	for i := 0; i < opt.RowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		row := s.generateRow(i, true)
		sb.WriteString(fmt.Sprintf("(%s)", row))
	}

	sb.WriteString(" ON DUPLICATE KEY UPDATE item_id=VALUES(item_id)")
	return sb.String()
}

func (s *ShopItemWorkload) buildUpdateSql(opt UpdateOption) string {
	tableName := fmt.Sprintf("shop_item_%d", opt.Table)
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

func (s *ShopItemWorkload) generateRow(suffix int, forUpdate bool) string {
	var primaryKey string
	if forUpdate {
		primaryKey = fmt.Sprintf("0x%x", uint64(rand.Int63())%s.totalRow)
	} else {
		primaryKey = uuid.New().String() // UUID for item_primary_key
	}
	itemID := "fixed_item_id"                         // Fixed value for item_id
	itemSetID := "fixed_item_set_id"                  // Fixed value for item_set_id
	productID := "fixed_product_id"                   // Fixed value for product_id
	productSetID := "fixed_product_set_id"            // Fixed value for product_set_id
	country := "US"                                   // Fixed value for point_of_sale_country
	merchantID := 123456                              // Fixed value for merchant_id
	merchantItemID := "fixed_merchant_item_id"        // Fixed value for merchant_item_id
	merchantItemSetID := "fixed_merchant_item_set_id" // Fixed value for merchant_item_set_id
	jsonField := randomJSONString(s.jsonFieldSize)
	randTime := rand.Int63()
	updateTime := 0

	return fmt.Sprintf("'%s-%d','%s','%s','%s','%s','%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%d,%d,%d,'%s'",
		primaryKey, suffix, itemID, itemSetID, productID, productSetID, country,
		merchantID, merchantItemID, merchantItemSetID,
		jsonField, jsonField, jsonField, jsonField, jsonField, jsonField,
		jsonField, jsonField, jsonField, jsonField, jsonField, jsonField,
		randTime, randTime, updateTime, jsonField)
}

func randomJSONString(size int) string {
	keyBuf := make([]byte, 5)
	valueBuf := make([]byte, 10)
	var sb strings.Builder
	sb.WriteString("{")
	remaining := size - 2 // account for braces
	idx := 0
	for remaining > 0 {
		if sb.Len() > 1 {
			sb.WriteString(",")
			remaining-- // account for comma
		}
		randomBytes(nil, keyBuf)
		randomBytes(nil, valueBuf)
		entry := fmt.Sprintf("\"%s-%d-%s\":\"%s-%d-%s\"", "key", idx, keyBuf, "value", idx, valueBuf)
		sb.WriteString(entry)
		remaining -= len(entry)
		idx++
	}
	sb.WriteString("}")
	return sb.String()
}
