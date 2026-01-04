package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	myrpc "dis_sys/backend/internal/rpc"
	"dis_sys/backend/internal/node"
	"dis_sys/backend/internal/task"
)

func main() {
	rpcAddr := flag.String("rpc", ":9000", "coordinator rpc listen addr")
	spawn := flag.Int("spawn", 3, "spawn N workers automatically")
	workerBin := flag.String("worker-bin", "./bin/worker", "path to worker binary")
	flag.Parse()

	nodes := node.NewManager()
	tasks := task.NewManager()

	// ✅ 注入 Tasks（M2 必须）
	svc := &myrpc.CoordinatorRPC{
		Nodes: nodes,
		Tasks: tasks,
	}

	server := rpc.NewServer()
	if err := server.RegisterName("CoordinatorRPC", svc); err != nil {
		log.Fatalf("rpc register error: %v", err)
	}
	log.Printf("[coordinator] BUILD=%s has=RequestTask", "M2-2026-01-02-2250")
	log.Printf("[coordinator] rpc service registered: CoordinatorRPC")

	ln, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("listen %s error: %v", *rpcAddr, err)
	}
	log.Printf("[coordinator] rpc listening on %s", *rpcAddr)

	// ✅ M2：启动时塞一些任务（验证闭环）
	for i := 0; i < 6; i++ {
		id := tasks.Submit(fmt.Sprintf("demo-%d", i))
		log.Printf("[coordinator] submitted task=%s", id)
	}

	// 监听退出信号，优雅关闭 worker 子进程
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	// 打印 worker/task 列表（调试用）
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for range t.C {
			ws := nodes.List()
			log.Printf("[coordinator] workers=%d", len(ws))
			for _, w := range ws {
				log.Printf("  - %s addr=%s status=%s last=%s",
					w.ID, w.Addr, w.Status, w.LastHeartbeat.Format(time.RFC3339))
			}

			ts := tasks.List()
			log.Printf("[coordinator] tasks=%d", len(ts))
			for _, tt := range ts {
				log.Printf("  - %s state=%s worker=%s payload=%q",
					tt.ID, tt.State, tt.WorkerID, tt.Payload)
			}
		}
	}()

	// 自动拉起 worker（Plan B）
	var procs []*exec.Cmd
	if *spawn > 0 {
		coordAddr := "127.0.0.1" + *rpcAddr
		procs = spawnWorkers(*spawn, *workerBin, coordAddr)
	}

	// RPC accept loop（主线程阻塞）
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept error: %v", err)
				continue
			}
			go server.ServeConn(conn)
		}
	}()

	// 等待退出信号
	<-stopCh
	log.Printf("[coordinator] shutting down...")

	// 关闭 worker 子进程
	for _, cmd := range procs {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}
}

func spawnWorkers(n int, workerBin string, coordAddr string) []*exec.Cmd {
	var procs []*exec.Cmd

	wd, _ := os.Getwd()
	log.Printf("[coordinator] cwd=%s", wd)

	absWorkerBin, err := filepath.Abs(workerBin)
	if err != nil {
		log.Printf("[coordinator] resolve workerBin failed: %v", err)
		return procs
	}
	log.Printf("[coordinator] workerBin=%s", absWorkerBin)

	for i := 0; i < n; i++ {
		cmd := exec.Command(absWorkerBin, "-coord", coordAddr)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Printf("[coordinator] spawn worker %d failed: %v (bin=%s)", i, err, absWorkerBin)
			continue
		}
		log.Printf("[coordinator] spawned worker pid=%d", cmd.Process.Pid)
		procs = append(procs, cmd)
	}
	return procs
}
