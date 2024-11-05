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

package v2

import (
	"fmt"
	"net/http"
)

func (h *OpenAPIV2) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	ctx, cli := req.Context(), h.server.GetEtcdClient()
	fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	kvs, err := cli.GetAllCDCInfo(ctx)
	if err != nil {
		fmt.Fprintf(w, "failed to get info: %s\n\n", err.Error())
		return
	}

	for _, kv := range kvs {
		fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), string(kv.Value))
	}
}
