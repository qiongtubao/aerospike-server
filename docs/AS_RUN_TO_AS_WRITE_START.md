# 从 as_run 启动到 as_write_start 的调用链解析

本文档说明：**as_run 启动后经历了什么**，以及**外部请求如何触发 as_write_start**。

---

## 一、从 as_run 开始的启动流程

### 1. 入口

```
main()  [as/src/base/main.c]
   └── as_run(argc, argv)  [as/src/base/as.c]
```

### 2. as_run 内部顺序（节选）

| 阶段 | 调用 | 说明 |
|------|------|------|
| 命令行 | getopt_long | 解析 --config-file、--foreground、--cold-start 等 |
| 早期初始化 | cf_log_init, cf_alloc_init, cf_trace_init, cf_thread_init, as_signal_setup, cf_fips_init, cf_tls_init | 日志、分配器、线程、信号、TLS |
| 配置 | as_config_init(config_file) | 解析配置文件，创建 namespace 等 |
| 权限/拓扑 | cf_topo_config, cf_process_privsep | NUMA/CPU 绑定、降权 |
| 日志/守护进程 | cf_log_activate_sinks, cf_process_daemonize（可选） | 激活 log sink、可选 daemon 化 |
| 目录/PID | validate_directory, write_pidfile | 校验工作目录、写 PID 文件 |
| 子系统初始化 | as_json_init, as_index_tree_gc_init, as_nsup_init, as_xdr_init, as_roster_init | JSON、索引 GC、nsup、XDR、roster |
| Namespace | as_namespaces_setup, as_namespaces_init | 冷/热启动、分区与索引树 |
| 存储 | as_storage_init, as_sindex_resume, as_storage_load, as_sindex_load, as_storage_activate | 存储引擎初始化、加载、激活 |
| 网络/集群前奏 | cf_dns_init, as_security_init, **as_service_init** | DNS、安全、**服务层初始化（创建 service 线程）** |
| 集群与通信 | as_hb_init, as_fabric_init, as_exchange_init, as_clustering_init, as_proxy_init, **as_rw_init** | 心跳、fabric、exchange、集群、代理、**读写服务** |
| 其他服务 | as_info_init, as_migrate_init, as_udf_init, as_batch_init, as_set_index_init | Info、迁移、UDF、批处理、set 索引 |
| 启动子系统 | as_fabric_start, as_hb_start, as_exchange_start, as_clustering_start, **as_service_start** | 开始收发包、**开始监听并处理客户端连接** |
| 主线程阻塞 | pthread_mutex_lock(&g_main_deadlock) | 主线程在此等待关机信号 |

**as_service_init()** 会为每个 service 线程调用 **create_service_thread(sid)**，内部 **cf_thread_create_transient(run_service, ctx)**，即每个线程跑的是 **run_service**。  
**as_service_start()** 里会建监听 socket、再按配置创建更多 service 线程（若需要），并 **start_reaper()**。

---

## 二、as_write_start 是如何被触发的（外部调用链）

### 1. 两条主要路径

- **路径 A：客户端直连本节点、单条写请求**（最常见）  
- **路径 B：内部入队的事务**（proxy、batch 子请求、UDF 子请求等）  
两条路径最终都会进入 **as_tsvc_process_transaction(tr)**，在 `tr` 为“写且非 delete/udf/read_touch/re_repl”时调用 **as_write_start(tr)**。

### 2. 路径 A：客户端单条写

```
客户端 TCP 连接
    │
    ▼
run_accept()  [service.c]
    └── accept() → assign_socket(fd_h)  // 把连接分配给某个 service 线程的 poll
    │
    ▼
run_service()  [service.c]   // 该线程 cf_poll_wait() 等待
    │
    ├── 事件 = 客户端 socket 可读 (CF_POLL_DATA_CLIENT_IO)
    │       └── process_readable(fd_h)   // 读满一条 as_proto 消息
    │               └── 若整条消息读完 (proto_unread == 0)
    │                       └── start_transaction(fd_h)
    │
    ▼
start_transaction(fd_h)  [service.c]
    │
    ├── proto->type == PROTO_TYPE_INFO     → as_info(); return
    ├── proto->type == PROTO_TYPE_SECURITY → as_security_transact(); return
    ├── tr.msgp->msg.info1 & AS_MSG_INFO1_BATCH → as_batch_queue_task(&tr); return  // 批请求走路径 B 的 batch 子路径
    │
    └── as_transaction_prepare(&tr, true)
            └── as_tsvc_process_transaction(&tr)
```

在 **start_transaction** 里会构造 **as_transaction tr**（origin = FROM_CLIENT），用 fd_h 上的 **as_proto**（即一条客户端消息）填好 **tr.msgp**，若不是 batch 则直接 **as_tsvc_process_transaction(&tr)**。

### 3. 路径 B：内部入队的事务（含 proxy、batch 子请求等）

其他模块把已经构造好的 **as_transaction** 入队到某个 service 线程的 **trans_q**：

- **as_service_enqueue_internal(tr)** 或 **as_service_enqueue_internal_keyd(tr)**  
  将 `tr` push 到 `ctx->trans_q`（按策略选 sid）。

Service 线程在 **run_service()** 的 poll 循环里：

- 事件类型为 **CF_POLL_DATA_EPOLL_QUEUE**（即 trans_q 有数据）  
  → **start_internal_transaction(ctx)**  
    → **cf_epoll_queue_pop(&ctx->trans_q, &tr)**  
    → **as_tsvc_process_transaction(&tr)**

例如：

- **Proxy**：本节点不是主副本时，as_partition_reserve_write 失败，**as_proxy_divert(dest, tr, ns)** 把请求发到 dest 节点；对端收到后在对端的 service 里会以 FROM_PROXY 身份再次走 reserve + **as_tsvc_process_transaction**，其中写请求会 **as_write_start(tr)**。
- **Batch**：**as_batch_queue_task(&tr)** 会把 batch 拆成多条子请求，子请求最终也会通过 enqueue 到 trans_q，由 **start_internal_transaction** 取出并 **as_tsvc_process_transaction**，写子请求会 **as_write_start**。
- **UDF**：UDF 触发的写在 thr_tsvc 里走 **as_udf_start(tr)**，内部仍可能产生写；thr_tsvc 里还有 **FROM_IUDF / as_transaction_is_udf(tr)** 时直接 **as_write_start(tr)** 的分支（见下）。

所以：**“外部”调用 as_write_start 的唯一起点就是“某个 as_transaction 被交给 as_tsvc_process_transaction，且该事务被判定为写且走写路径”**；外部要么是客户端连上来的一条写消息（路径 A），要么是经 proxy/batch/内部入队后由 trans_q 弹出的写事务（路径 B）。

---

## 三、as_tsvc_process_transaction 里何时调用 as_write_start

在 **as_tsvc_process_transaction(tr)**（**as/src/base/thr_tsvc.c**）中逻辑概要如下：

1. **PROTO_TYPE_INTERNAL_XDR** → as_xdr_read(tr); return  
2. 校验 namespace、partition balance 等；**as_transaction_is_query(tr)** → as_query(...); return  
3. 单条事务：根据消息 **m->info2 & AS_MSG_INFO2_WRITE** 得到 **is_write**，**as_partition_reserve_write** 或 **as_partition_reserve_read_tr**。  
4. 若 **reservation 成功** 且 **is_write**：
   - **as_transaction_is_delete(tr)** → 根据 convert_to_write 选 **as_write_start(tr)** 或 **as_delete_start(tr)**
   - **FROM_IUDF 或 as_transaction_is_udf(tr)** → **as_udf_start(tr)**（UDF 内部可能再触发写或直接走下面）
   - **FROM_READ_TOUCH** → as_read_touch_start(tr)
   - **FROM_RE_REPL** → as_re_replicate_start(tr)
   - **否则** → **as_write_start(tr)**  ← **普通客户端/Proxy/Batch 的写都在这里触发**

5. 若 reservation 失败（例如本节点不是主）：根据 tr->origin 做 **as_proxy_divert** 或 **as_proxy_return_to_sender**，请求会被转发或回源，最终在“正确”的节点上再次进入 **as_tsvc_process_transaction** 并可能 **as_write_start(tr)**。

因此：**as_write_start 的“外部”调用方就是 thr_tsvc 的 as_tsvc_process_transaction**；外部请求（客户端、proxy、batch 等）都是先变成 **as_transaction**，再通过 **start_transaction** 或 **start_internal_transaction** 进入 **as_tsvc_process_transaction**，从而在写路径上调用 **as_write_start**。

---

## 四、调用链小结（写请求从进来到 as_write_start）

```
客户端发送写请求 (TCP + Aerospike 协议)
    │
    ▼
run_accept → assign_socket → 某 service 线程的 cf_poll
    │
    ▼
run_service(): cf_poll_wait() 返回 → 客户端 fd 可读
    │
    ▼
process_readable(fd_h) 读满一条 as_proto
    │
    ▼
start_transaction(fd_h)
    │
    ├── 构造 as_transaction tr (FROM_CLIENT), tr.msgp = proto
    └── as_tsvc_process_transaction(&tr)
            │
            ├── is_write = (m->info2 & AS_MSG_INFO2_WRITE)
            ├── as_partition_reserve_write(ns, pid, &tr->rsv, &dest)
            │       └── 成功 → 本节点为主
            │
            └── is_write 且非 delete/udf/read_touch/re_repl
                    └── as_write_start(tr)   ← 写请求在此进入写流水线
```

**内部/Proxy/Batch 写**：  
其他模块构造 **as_transaction**（origin 可为 FROM_PROXY、FROM_BATCH、FROM_IUDF 等），通过 **as_service_enqueue_internal** 或 **as_service_enqueue_internal_keyd** 放入某个 **trans_q** → 该线程 **run_service** 中 **CF_POLL_DATA_EPOLL_QUEUE** → **start_internal_transaction** → **as_tsvc_process_transaction(&tr)** → 同上，写分支 **as_write_start(tr)**。

---

## 五、关键文件与函数索引

| 阶段 | 文件 | 函数/位置 | 说明 |
|------|------|-----------|------|
| 进程入口 | as/src/base/main.c | main() | 调用 as_run() |
| 服务入口 | as/src/base/as.c | as_run() | 解析参数、配置、初始化各子系统、as_service_init/start、主线程阻塞 |
| 服务初始化 | as/src/base/service.c | as_service_init() | 创建 service 线程（run_service） |
| 服务启动 | as/src/base/service.c | as_service_start() | 监听 socket、start_reaper、可再创建线程 |
| 线程主循环 | as/src/base/service.c | run_service() | cf_poll_wait；处理客户端 IO 与 trans_q |
| 客户端可读 | as/src/base/service.c | process_readable() | 读取 as_proto 消息 |
| 开始单条事务 | as/src/base/service.c | start_transaction() | 构造 tr，调用 as_tsvc_process_transaction（或 as_batch_queue_task） |
| 内部事务 | as/src/base/service.c | start_internal_transaction() | 从 trans_q pop tr，调用 as_tsvc_process_transaction |
| 入队内部事务 | as/src/base/service.c | as_service_enqueue_internal / as_service_enqueue_internal_keyd | 将 tr 压入某线程 trans_q |
| 事务分发 | as/src/base/thr_tsvc.c | as_tsvc_process_transaction() | 按类型/权限/reserve 结果分发；写路径调用 as_write_start |
| 写入口 | as/src/transaction/write.c | as_write_start() | 执行写事务（见 WRITE_AND_STORAGE_FLOW.md） |

---

以上即从 **as_run 启动** 到 **as_write_start 被触发** 的完整路径与外部调用方式。
