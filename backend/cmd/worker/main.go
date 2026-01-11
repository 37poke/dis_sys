package main

import (
	"flag"
	"log"
	"math/rand" // 用于模拟任务执行失败
	"net"
	"net/http"
	"time"

	// 自定义RPC包：封装与协调器的RPC通信逻辑
	myrpc "dis_sys/backend/internal/rpc"
)

// main Worker节点主函数：
// 1. 启动HTTP服务（用于健康检查）
// 2. 向协调器注册自身节点信息
// 3. 循环拉取任务并模拟执行（含失败模拟）
// 4. 定时发送心跳保活
func main() {
	log.Printf("[worker] BUILD=M2-reportdone-2026-01-03-0025")

	// 解析命令行参数：指定协调器的RPC地址，默认127.0.0.1:9000
	coordAddr := flag.String("coord", "127.0.0.1:9000", "coordinator rpc address")
	flag.Parse()

	// 1) 启动HTTP服务器（用于协调器健康检查），让操作系统自动分配端口
	// 监听127.0.0.1的随机可用端口（:0表示自动分配）
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("[worker] listen error: %v", err) // 监听失败则退出程序
	}
	// 获取自动分配的端口地址（例如 127.0.0.1:42997），作为Worker的唯一可访问地址
	workerAddr := ln.Addr().String()

	// 创建HTTP请求多路复用器（路由管理器）
	mux := http.NewServeMux()
	// 注册/ping接口：用于协调器健康检查，返回200 OK和"ok"
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)       // 设置响应状态码200
		_, _ = w.Write([]byte("ok"))       // 响应体返回"ok"
	})

	// 创建HTTP服务器实例，绑定路由管理器
	srv := &http.Server{Handler: mux}
	// 异步启动HTTP服务器（不阻塞主线程）
	go func() {
		log.Printf("[worker] http listening on %s", workerAddr)
		// 启动HTTP服务，监听自动分配的端口
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[worker] http serve error: %v", err)
		}
	}()

	// 2) 向协调器注册Worker节点（把workerAddr上报给协调器）
	// 创建与协调器通信的RPC客户端
	client, err := myrpc.NewClient(*coordAddr)
	if err != nil {
		log.Fatalf("[worker] dial coordinator error: %v", err) // 连接协调器失败则退出
	}
	defer client.Close() // 程序退出时关闭RPC客户端连接

	// 向协调器注册当前Worker节点，传入HTTP服务地址，获取唯一的workerID
	workerID, err := client.RegisterWorker(workerAddr)
	if err != nil {
		log.Fatalf("[worker] register error: %v", err) // 注册失败则退出
	}
	log.Printf("[worker] registered: workerID=%s addr=%s", workerID, workerAddr)

	// 3) 任务拉取循环（M2：模拟任务执行，含失败场景）
	go func() {
		// 创建800毫秒间隔的定时器：定时向协调器请求任务
		poll := time.NewTicker(800 * time.Millisecond)
		defer poll.Stop() // 协程退出时停止定时器

		// 循环读取定时器信号（每800ms执行一次）
		for range poll.C {
			// 向协调器请求分配任务，传入自身workerID
			reply, err := client.RequestTask(workerID)
			if err != nil {
				log.Printf("[worker] request task error: %v", err)
				continue // 请求失败则跳过本次循环，下次再试
			}
			// 无任务可执行（HasTask=false），跳过
			if reply == nil || !reply.HasTask {
				continue
			}

			// 成功获取任务，打印任务信息
			log.Printf("[worker] got task id=%s payload=%q state=%s attempts=%d leaseUntil=%d",
    reply.TaskID, reply.Payload, reply.State, reply.Attempts, reply.LeaseUntilMs)


			// M2：模拟任务执行耗时（真实场景下此处是业务逻辑，如宠物健康分析计算）
			time.Sleep(300 * time.Millisecond)
			// 在 time.Sleep(300ms) 之后加
			if rand.Intn(10) < 1 { // 10% 概率：模拟卡死/丢失
				log.Printf("[worker] simulate hang task=%s (no report)", reply.TaskID)
				time.Sleep(10 * time.Second) // 必须大于 lease
				continue
			}

			// 模拟任务执行失败：10%的概率触发（rand.Intn(5)生成0-4的随机数，等于0时触发）
			if rand.Intn(10) < 1 {
				log.Printf("[worker] simulate fail task=%s", reply.TaskID)

				// 构造失败信息
				msg := "simulated error"
				// 向协调器上报任务执行失败，返回是否重新入队（根据最大重试次数判断）
				requeued, err := client.ReportTaskFailed(workerID, reply.TaskID, msg, reply.LeaseToken)
				if err != nil {
					log.Printf("[worker] report failed error: %v", err)
				} else {
					log.Printf("[worker] report failed ok task=%s requeued=%v", reply.TaskID, requeued)
				}
				continue // ✅ 失败后直接进入下一轮，不再执行后续的成功上报逻辑
			}
			

			// 任务执行成功，准备上报
			log.Printf("[worker] will report done task=%s", reply.TaskID)

			// 向协调器上报任务执行完成
			if err := client.ReportTaskDone(workerID, reply.TaskID,reply.LeaseToken); err != nil {
				log.Printf("[worker] report done FAILED task=%s err=%v", reply.TaskID, err)
			} else {
				log.Printf("[worker] report done OK task=%s", reply.TaskID)
			}
		}
	}()

	// 4) 心跳保活循环：定时向协调器发送心跳，证明节点存活
	ticker := time.NewTicker(1 * time.Second) // 1秒发送一次心跳
	defer ticker.Stop()                       // 程序退出时停止定时器

	// 循环读取定时器信号（每秒执行一次）
	for range ticker.C {
		// 发送心跳请求，传入workerID
		if err := client.Heartbeat(workerID); err != nil {
			log.Printf("[worker] heartbeat error: %v", err)
		}
	}
}