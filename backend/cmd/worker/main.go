package main

import (
    "flag"
    "log"
    "net"
    "net/http"
    "time"

    myrpc "dis_sys/backend/internal/rpc"
)

func main() {
    coordAddr := flag.String("coord", "127.0.0.1:9000", "coordinator rpc address")
    flag.Parse()

    // 1) 启动一个 HTTP server，并让 OS 自动分配端口
    ln, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        log.Fatalf("[worker] listen error: %v", err)
    }
    workerAddr := ln.Addr().String() // 例如 127.0.0.1:42997

    mux := http.NewServeMux()
    mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte("ok"))
    })

    srv := &http.Server{Handler: mux}
    go func() {
        log.Printf("[worker] http listening on %s", workerAddr)
        if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
            log.Printf("[worker] http serve error: %v", err)
        }
    }()

    // 2) RPC 注册（把 workerAddr 注册出去）
    client, err := myrpc.NewClient(*coordAddr)
    if err != nil {
        log.Fatalf("[worker] dial coordinator error: %v", err)
    }
    defer client.Close()

    workerID, err := client.RegisterWorker(workerAddr)
    if err != nil {
        log.Fatalf("[worker] register error: %v", err)
    }
    log.Printf("[worker] registered: workerID=%s addr=%s", workerID, workerAddr)

    // 3) 心跳 loop
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        if err := client.Heartbeat(workerID); err != nil {
            log.Printf("[worker] heartbeat error: %v", err)
        }
    }
}
