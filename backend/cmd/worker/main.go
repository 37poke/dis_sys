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
	log.Printf("[worker] BUILD=M2-reportdone-2026-01-03-0025")

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


	// 3) 拉任务 loop（M2：模拟执行）
	go func() {
		poll := time.NewTicker(800 * time.Millisecond)
		defer poll.Stop()

		for range poll.C {
			reply, err := client.RequestTask(workerID)
			if err != nil {
				log.Printf("[worker] request task error: %v", err)
				continue
			}
			if reply == nil || !reply.HasTask {
				continue
			}

			log.Printf("[worker] got task id=%s payload=%q state=%s", reply.TaskID, reply.Payload, reply.State)

			// M2：先不做真实计算，只模拟执行
			time.Sleep(300 * time.Millisecond)

			log.Printf("[worker] will report done task=%s", reply.TaskID)

			if err := client.ReportTaskDone(workerID, reply.TaskID); err != nil {
				log.Printf("[worker] report done FAILED task=%s err=%v", reply.TaskID, err)
			} else {
				log.Printf("[worker] report done OK task=%s", reply.TaskID)
			}

		}
	}()

		// 4) 心跳 loop
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if err := client.Heartbeat(workerID); err != nil {
				log.Printf("[worker] heartbeat error: %v", err)
			}
		}
		
	}
