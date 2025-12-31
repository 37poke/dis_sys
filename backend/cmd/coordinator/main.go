package main

import (
    "flag"
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
)


func main() {
	rpcAddr := flag.String("rpc", ":9000", "coordinator rpc listen addr")
	spawn := flag.Int("spawn", 3, "spawn N workers automatically")

	workerBin := flag.String("worker-bin", "./bin/worker", "path to worker binary")
	nodes := node.NewManager()
	svc := &myrpc.CoordinatorRPC{Nodes: nodes}

	server := rpc.NewServer()
	if err := server.RegisterName("CoordinatorRPC", svc); err != nil {
		log.Fatalf("rpc register error: %v", err)
	}

	ln, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("listen %s error: %v", *rpcAddr, err)
	}
	log.Printf("[coordinator] rpc listening on %s", *rpcAddr)

	// 监听退出信号，优雅关闭 worker 子进程
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	// 打印 worker 列表（M1 调试用）
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
		}
	}()

	// 自动拉起 worker（Plan B）
	var procs []*exec.Cmd
	if *spawn > 0 {
		procs = spawnWorkers(*spawn, *workerBin, "127.0.0.1"+*rpcAddr)
	}

	// RPC accept loop
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

	// 1️⃣ 打印 coordinator 当前工作目录（排错用）
	wd, _ := os.Getwd()
	log.Printf("[coordinator] cwd=%s", wd)

	// 2️⃣ 把 workerBin 转成绝对路径（关键修复点）
	absWorkerBin, err := filepath.Abs(workerBin)
	if err != nil {
		log.Printf("[coordinator] resolve workerBin failed: %v", err)
		return procs
	}
	log.Printf("[coordinator] workerBin=%s", absWorkerBin)

	// 3️⃣ spawn workers
	for i := 0; i < n; i++ {
		cmd := exec.Command(absWorkerBin, "-coord", coordAddr)

		// 让 worker 日志直接打到 coordinator 终端
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Printf(
				"[coordinator] spawn worker %d failed: %v (bin=%s)",
				i, err, absWorkerBin,
			)
			continue
		}

		log.Printf("[coordinator] spawned worker pid=%d", cmd.Process.Pid)
		procs = append(procs, cmd)
	}

	return procs

	func spawnWorkers(n int, workerBin string, coordAddr string) []*exec.Cmd {
    var procs []*exec.Cmd
    for i := 0; i < n; i++ {
        cmd, err := spawnOneWorker(workerBin, coordAddr)
        if err != nil {
            log.Printf("[coordinator] spawn worker %d failed: %v", i, err)
            continue
        }
        procs = append(procs, cmd)
        watchAndRespawn(workerBin, coordAddr, cmd)
    }
    return procs
}


	func watchAndRespawn(workerBin, coordAddr string, cmd *exec.Cmd) {
		go func() {
			err := cmd.Wait()
			log.Printf("[coordinator] worker exited pid=%d err=%v -> respawn", cmd.Process.Pid, err)

			// 简单防抖：避免秒退无限刷
			time.Sleep(500 * time.Millisecond)

			newCmd, e := spawnOneWorker(workerBin, coordAddr)
			if e != nil {
				log.Printf("[coordinator] respawn failed: %v", e)
				return
			}
			watchAndRespawn(workerBin, coordAddr, newCmd)
		}()
	}

}
