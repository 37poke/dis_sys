package utils

import (
    "crypto/rand"
    "encoding/hex"
)

// NewToken 生成一个安全的随机 token（16 bytes）
func NewToken() string {
    b := make([]byte, 16)
    if _, err := rand.Read(b); err != nil {
        return ""
    }
    return hex.EncodeToString(b)
}
