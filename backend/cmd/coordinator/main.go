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

	// 自定义RPC包：存放协调器与工作节点的RPC通信定义
	myrpc "dis_sys/backend/internal/rpc"
	// 节点管理包：管理worker节点的注册、状态、心跳等
	"dis_sys/backend/internal/node"
	// 任务管理包：管理任务的提交、状态、重试、过期重入队列等
	"dis_sys/backend/internal/task"
)

// main 协调器主函数：
// 1. 解析启动参数
// 2. 初始化节点/任务管理器
// 3. 注册RPC服务
// 4. 启动定时任务（任务过期回收）
// 5. 自动拉起指定数量的worker进程
// 6. 监听RPC端口并处理请求
// 7. 监听退出信号，实现优雅关闭
func main() {
	// 解析命令行参数
	// rpc：协调器RPC服务监听地址，默认:9000
	rpcAddr := flag.String("rpc", ":9000", "coordinator rpc listen addr")
	// spawn：启动时自动拉起的worker进程数量，默认3个
	spawn := flag.Int("spawn", 3, "spawn N workers automatically")
	// worker-bin：worker可执行文件的路径，默认./bin/worker
	workerBin := flag.String("worker-bin", "./bin/worker", "path to worker binary")
	flag.Parse() // 解析参数

	// 初始化节点管理器：用于管理所有注册的worker节点（状态、心跳、地址等）
	nodes := node.NewManager()
	// 初始化任务管理器：用于管理所有提交的任务（状态、重试次数、执行节点等）
	tasks := task.NewManager()

	// ✅ 注入 Tasks（M2 必须）：初始化RPC服务实例，关联节点和任务管理器
	// 使得RPC接口可以操作节点和任务数据
	svc := &myrpc.CoordinatorRPC{
		Nodes: nodes,   // 节点管理器实例
		Tasks: tasks,   // 任务管理器实例
	}
	// 任务租约时间：worker获取任务后，需在该时间内上报心跳，否则任务视为过期
	lease := 3 * time.Second
	// 任务最大重试次数：过期任务重试次数超过该值则不再重入队列
	maxRetry := 3

	// 启动定时协程：每秒检查并回收过期任务
	go func() {
		// 创建1秒间隔的定时器
		tk := time.NewTicker(1 * time.Second)
		defer tk.Stop() // 协程退出时停止定时器

		// 循环读取定时器信号（每秒执行一次）
		for now := range tk.C {
			// 回收过期任务：将超过租约时间未完成的任务重入队列
			// 返回值为被回收的任务ID列表
			recycled := tasks.RequeueExpired(now, lease, maxRetry)
			if len(recycled) > 0 {
				log.Printf("[coordinator] recycled expired tasks=%v", recycled)
			}
		}
	}()

	// 创建RPC服务器实例
	server := rpc.NewServer()
	// 注册RPC服务：将CoordinatorRPC服务注册到RPC服务器，服务名CoordinatorRPC
	if err := server.RegisterName("CoordinatorRPC", svc); err != nil {
		log.Fatalf("rpc register error: %v", err) // 注册失败则退出程序
	}
	log.Printf("[coordinator] BUILD=%s has=RequestTask", "M2-2026-01-02-2250")
	log.Printf("[coordinator] rpc service registered: CoordinatorRPC")

	// 监听指定的TCP端口，用于接收worker的RPC请求
	ln, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("listen %s error: %v", *rpcAddr, err) // 监听失败则退出程序
	}
	log.Printf("[coordinator] rpc listening on %s", *rpcAddr)

	// ✅ M2：启动时提交6个测试任务（验证任务调度闭环）
	for i := 0; i < 6; i++ {
		// 提交任务：任务内容为"demo-0/demo-1..."，返回任务ID
		id := tasks.Submit(fmt.Sprintf("demo-%d", i))
		log.Printf("[coordinator] submitted task=%s", id)
	}

	// 创建退出信号通道：用于监听系统退出信号（SIGINT/Ctrl+C、SIGTERM）
	stopCh := make(chan os.Signal, 1)
	// 注册需要监听的信号：SIGINT（终端中断）、SIGTERM（进程终止）
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动调试协程：每3秒打印一次worker和task的状态（方便调试）
	go func() {
		// 创建3秒间隔的定时器
		t := time.NewTicker(3 * time.Second)
		defer t.Stop() // 协程退出时停止定时器
		for range t.C {
			// 获取所有worker节点列表
			ws := nodes.List()
			log.Printf("[coordinator] workers=%d", len(ws))
			// 遍历打印每个worker的ID、地址、状态、最后心跳时间
			for _, w := range ws {
				log.Printf("  - %s addr=%s status=%s last=%s",
					w.ID, w.Addr, w.Status, w.LastHeartbeat.Format(time.RFC3339))
			}

			// 获取所有任务列表
			ts := tasks.List()
			log.Printf("[coordinator] tasks=%d", len(ts))
			// 遍历打印每个任务的ID、状态、执行节点ID、任务内容
			for _, tt := range ts {
				log.Printf("  - %s state=%s worker=%s payload=%q",
					tt.ID, tt.State, tt.WorkerID, tt.Payload)
			}
		}
	}()

	// 自动拉起worker进程（Plan B）：如果spawn参数>0，则启动指定数量的worker
	var procs []*exec.Cmd // 存储拉起的worker进程实例
	if *spawn > 0 {
		// 拼接协调器地址（worker需要连接该地址注册）
		coordAddr := "127.0.0.1" + *rpcAddr
		// 调用spawnWorkers函数拉起worker进程
		procs = spawnWorkers(*spawn, *workerBin, coordAddr)
	}

	// RPC连接接收循环（主线程阻塞）：异步处理每个RPC连接
	go func() {
		for {
			// 接收新的TCP连接
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept error: %v", err)
				continue
			}
			// 启动协程处理该连接的RPC请求
			go server.ServeConn(conn)
		}
	}()

	// 等待退出信号：阻塞直到接收到SIGINT/SIGTERM
	<-stopCh
	log.Printf("[coordinator] shutting down...")

	// 优雅关闭：向所有拉起的worker进程发送终止信号
	for _, cmd := range procs {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}
}

// spawnWorkers 自动拉起指定数量的worker进程
// 参数：
//   n: 要拉起的worker数量
//   workerBin: worker可执行文件路径
//   coordAddr: 协调器RPC地址（worker需要连接该地址）
// 返回值：成功拉起的worker进程实例列表
func spawnWorkers(n int, workerBin string, coordAddr string) []*exec.Cmd {
	var procs []*exec.Cmd // 存储成功启动的worker进程

	// 获取当前工作目录（用于调试）
	wd, _ := os.Getwd()
	log.Printf("[coordinator] cwd=%s", wd)

	// 将workerBin转换为绝对路径（避免相对路径问题）
	absWorkerBin, err := filepath.Abs(workerBin)
	if err != nil {
		log.Printf("[coordinator] resolve workerBin failed: %v", err)
		return procs
	}
	log.Printf("[coordinator] workerBin=%s", absWorkerBin)

	// 循环拉起n个worker进程
	for i := 0; i < n; i++ {
		// 构建启动worker的命令：./bin/worker -coord 127.0.0.1:9000
		cmd := exec.Command(absWorkerBin, "-coord", coordAddr)
		// 将worker的标准输出/错误重定向到协调器的标准输出/错误（方便查看日志）
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// 启动worker进程（非阻塞）
		if err := cmd.Start(); err != nil {
			log.Printf("[coordinator] spawn worker %d failed: %v (bin=%s)", i, err, absWorkerBin)
			continue // 启动失败则跳过当前worker
		}
		log.Printf("[coordinator] spawned worker pid=%d", cmd.Process.Pid)
		// 将成功启动的进程实例加入列表
		procs = append(procs, cmd)
	}
	return procs
}