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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin       
`

type ShopItemWorkload struct {
	r             *rand.Rand
	jsonFieldSize int
}

func NewShopItemWorkload(totalCount uint64, rowSize int) Workload {
	jsonFieldSize := rowSize / 13
	return &ShopItemWorkload{
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
		jsonFieldSize: jsonFieldSize,
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
		row := s.generateRow()
		sb.WriteString(fmt.Sprintf("(%s)", row))
	}

	return sb.String()
}

func (s *ShopItemWorkload) BuildUpdateSql(opt UpdateOption) string {
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

	sb.WriteString(" ON DUPLICATE KEY UPDATE updated_time=updated_time+1")
	return sb.String()
}

func (s *ShopItemWorkload) generateRow() string {
	jetter := rand.Int31n(1000) // in case for duplicate primary key
	id := uint64(s.r.Int63()) + uint64(jetter)
	primaryKey := fmt.Sprintf("0x%x", id)
	itemID := "fixed_item_id"                         // Fixed value for item_id
	itemSetID := "fixed_item_set_id"                  // Fixed value for item_set_id
	productID := "fixed_product_id"                   // Fixed value for product_id
	productSetID := "fixed_product_set_id"            // Fixed value for product_set_id
	country := "US"                                   // Fixed value for point_of_sale_country
	merchantID := 123456                              // Fixed value for merchant_id
	merchantItemID := "fixed_merchant_item_id"        // Fixed value for merchant_item_id
	merchantItemSetID := "fixed_merchant_item_set_id" // Fixed value for merchant_item_set_id
	jsonField := randomJSONString(s.r, s.jsonFieldSize)

	return fmt.Sprintf("'%s','%s','%s','%s','%s','%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%d,%d,%d,'%s'",
		primaryKey, itemID, itemSetID, productID, productSetID, country,
		merchantID, merchantItemID, merchantItemSetID,
		jsonField, jsonField, jsonField, jsonField, jsonField, jsonField,
		jsonField, jsonField, jsonField, jsonField, jsonField, jsonField,
		s.r.Int63(), s.r.Int63(), 0, jsonField)
}

func randomJSONString(r *rand.Rand, size int) string {
	var sb strings.Builder
	sb.WriteString("{")
	remaining := size - 2 // account for braces
	idx := 0
	for remaining > 0 {
		if sb.Len() > 1 {
			sb.WriteString(",")
			remaining-- // account for comma
		}
		entry := fmt.Sprintf("\"%s-%d\":\"%s-%d\"", "key", idx, "value", idx)
		sb.WriteString(entry)
		remaining -= len(entry)
		idx++
	}
	sb.WriteString("}")
	return sb.String()
}
