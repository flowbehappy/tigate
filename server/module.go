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

package server

import "context"

// SubModule identify the modules will be started when the server is starting
type SubModule interface {
	// Name returns the SubModule's Name
	Name() string
	// Run runs the wacher, it's a block call, only return when finished or error occurs
	Run(ctx context.Context) error
	// Close closes the module, it's a block call
	Close(ctx context.Context) error
}
