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

package checker

// Checker is the interface for all checkers,
// checker are used to check the status of the system, like balance, split, merge etc.
// checker will be scheduled by the controller periodically and submit the result to the operator
// checker is not an urgent task, don't have to be executed timely, it's ok to be delayed
type Checker interface {
	// Name returns the name of the checker
	Name() string
	// Check the main function of the checker, it will be called by the controller periodically
	Check()
}
