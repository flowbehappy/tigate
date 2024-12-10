package utils

import "hash/fnv"

func StringHashUInt64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
