package utils

import (
	"crypto/rand"
	"encoding/hex"
)

func NewWorkerID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "w-" + hex.EncodeToString(b)
}
