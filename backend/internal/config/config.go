package config

type Config struct {
    RPCListenAddr      string        // coordinator RPC addr
    HTTPListenAddr     string        // coordinator HTTP addr
    HeartbeatInterval  time.Duration // worker heartbeat interval
    WorkerDeadTimeout  time.Duration // coordinator marks worker dead
    TaskLeaseTimeout   time.Duration // running task timeout/lease
    MaxTaskRetries     int
}

func Load() Config
