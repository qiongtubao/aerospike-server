/*
 * service.c - Aerospike 客户端服务层与网络 I/O 管理模块
 *
 * 模块职责：
 * 1. 客户端连接管理：接受、分配、监控客户端连接
 * 2. 服务线程池管理：创建和管理多个 service 线程
 * 3. 网络 I/O 事件处理：epoll 事件循环，处理客户端请求
 * 4. 协议解析与事务分发：解析 as_proto 消息，创建 as_transaction
 * 5. 连接生命周期管理：空闲连接回收，错误连接清理
 * 6. 请求队列调度：内部事务队列，负载均衡分发
 *
 * 核心架构：
 * 1. Accept 线程：监听服务端口，接受新的客户端连接
 * 2. Service 线程池：每个线程运行独立的 epoll 事件循环
 * 3. Reaper 线程：定期清理空闲和异常连接
 *
 * 线程池与负载均衡：
 * - 支持多种连接分配策略：轮询、CPU 亲和性、NUMA 感知、ADQ
 * - 每个 service 线程维护独立的 epoll 实例和事务队列
 * - 动态调整线程池大小，支持运行时扩容缩容
 *
 * 协议处理流程：
 * 连接建立 -> TLS 握手（如需要）-> 协议头解析 -> 消息体接收 ->
 * 事务创建 -> 分发到事务处理器（批处理/单条事务）
 *
 * 内存和连接池管理：
 * - 文件句柄池：预分配固定大小的连接槽位
 * - 引用计数：连接对象的生命周期管理
 * - 连接复用：epoll 实例间的连接迁移和重分配
 *
 * Copyright (C) 2018-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#include "base/service.h"

#include <errno.h>
#include <sched.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <zlib.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "epoll_queue.h"
#include "hardware.h"
#include "log.h"
#include "socket.h"
#include "tls.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/thr_info.h"
#include "base/thr_tsvc.h"
#include "base/transaction.h"
#include "fabric/partition.h"

#include "warnings.h"


//==========================================================
// 类型定义与常量
//

// epoll 事件数组大小：每次 epoll_wait 最多处理的事件数
#define N_EVENTS 1024

// XDR 连接的缓冲区大小配置
#define XDR_WRITE_BUFFER_SIZE (5 * 1024 * 1024)   // 5MB 写缓冲区
#define XDR_READ_BUFFER_SIZE (15 * 1024 * 1024)   // 15MB 读缓冲区

/**
 * Service 线程上下文结构
 *
 * 每个 service 线程维护一个独立的上下文，包含：
 * - CPU 绑定信息（如果启用了 CPU 亲和性）
 * - 线程锁（用于线程安全操作）
 * - epoll 实例（处理该线程的所有客户端连接）
 * - 事务队列（接收内部事务，如 batch 子请求、proxy 请求等）
 */
typedef struct thread_ctx_s {
	cf_topo_cpu_index i_cpu;    // 绑定的 CPU 索引（仅在 CPU 绑定模式下使用）
	cf_mutex* lock;             // 线程锁，保护线程上下文的并发访问
	cf_poll poll;               // epoll 实例，处理客户端连接的 I/O 事件
	cf_epoll_queue trans_q;     // 内部事务队列，接收其他模块分发的事务
} thread_ctx;


//==========================================================
// 全局变量
//

// 服务访问配置：客户端连接的监听地址和端口配置
// 包含普通服务、备用服务、TLS 服务、备用 TLS 服务的地址端口信息
as_service_access g_access = {
	.service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.alt_service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.tls_service = { .addrs = { .n_addrs = 0 }, .port = 0 },
	.alt_tls_service = { .addrs = { .n_addrs = 0 }, .port = 0 }
};

// 服务绑定配置：监听套接字的配置信息
cf_serv_cfg g_service_bind = { .n_cfgs = 0 };

// TLS 配置信息指针
cf_tls_info* g_service_tls;

// 监听套接字集合：包含所有监听端口的套接字
static cf_sockets g_sockets;

// Service 线程锁数组：每个线程对应一个锁
static cf_mutex g_thread_locks[MAX_SERVICE_THREADS];

// Service 线程上下文数组：存储所有 service 线程的上下文
static thread_ctx* g_thread_ctxs[MAX_SERVICE_THREADS];

// 连接回收器相关的全局变量
static cf_mutex g_reaper_lock = CF_MUTEX_INIT;  // 保护文件句柄数组的锁
static uint32_t g_n_slots;                      // 文件句柄槽位总数
static as_file_handle** g_file_handles;         // 文件句柄数组（连接池）
static cf_queue g_free_slots;                   // 空闲槽位队列


//==========================================================
// Forward declarations.
//

// Setup.
static void create_service_thread(uint32_t sid);
static void add_localhost(cf_serv_cfg* serv_cfg, cf_sock_owner owner);

// Accept client connections.
static void* run_accept(void* udata);

// Assign connections to threads.
static void assign_socket(as_file_handle* fd_h);
static uint32_t select_sid(void);
static uint32_t select_sid_pinned(cf_topo_cpu_index i_cpu);
static uint32_t select_sid_adq(cf_topo_napi_id id);
static uint32_t select_sid_keyd(const cf_digest* keyd);
static void schedule_redistribution(void);

// Demarshal requests.
static void* run_service(void* udata);
static void stop_service(thread_ctx* ctx);
static void service_release_file_handle(as_file_handle* fd_h);
static bool process_readable(as_file_handle* fd_h);
static void start_transaction(as_file_handle* fd_h);
static void config_xdr_socket(cf_socket* sock);

// Reap idle and bad connections.
static void start_reaper(void);
static void* run_reaper(void* udata);

// Transaction queue.
static bool start_internal_transaction(thread_ctx* ctx);


//==========================================================
// Inlines & macros.
//

static inline void
rearm(as_file_handle* fd_h, uint32_t events)
{
	cf_poll_modify_socket(fd_h->poll, &fd_h->sock,
			events | EPOLLONESHOT | EPOLLRDHUP, fd_h);
}


//==========================================================
// 公共 API - 服务层初始化与管理
//

/**
 * 初始化客户端服务系统
 *
 * 功能描述：
 * 创建并初始化所有 service 线程和相关的数据结构。
 * 每个线程会创建独立的 epoll 实例和事务队列。
 *
 * 初始化内容：
 * 1. 初始化所有线程锁
 * 2. 根据配置创建指定数量的 service 线程
 * 3. 每个线程绑定到特定的 CPU（如果启用了 CPU 亲和性）
 * 4. 为每个线程创建独立的 epoll 实例和事务队列
 *
 * 注意事项：
 * - 此时只创建线程和数据结构，还未开始监听端口
 * - 线程创建后立即进入事件循环，等待连接分配
 */
void
as_service_init(void)
{
	//------------------------------------------------------
	// 创建 epoll 实例和 service 线程
	//------------------------------------------------------

	cf_info(AS_SERVICE, "starting %u service threads",
			g_config.n_service_threads);

	// 初始化所有线程锁（即使某些线程可能不会被创建）
	for (uint32_t i = 0; i < MAX_SERVICE_THREADS; i++) {
		cf_mutex_init(&g_thread_locks[i]);
	}

	// 根据配置的线程数创建 service 线程
	for (uint32_t i = 0; i < g_config.n_service_threads; i++) {
		create_service_thread(i);
	}
}

/**
 * 启动客户端服务系统
 *
 * 功能描述：
 * 启动服务的网络监听部分，开始接受客户端连接。
 *
 * 启动流程：
 * 1. 启动连接回收线程（reaper）
 * 2. 创建监听套接字（包括 localhost 和配置的地址）
 * 3. 启动 accept 线程开始接受客户端连接
 *
 * 网络配置：
 * - 如果未禁用 localhost，会自动添加 localhost 监听
 * - 支持普通连接和 TLS 加密连接
 * - 显示所有监听的服务地址和端口
 */
void
as_service_start(void)
{
	// 启动连接回收线程（清理空闲和异常连接）
	start_reaper();

	//------------------------------------------------------
	// 创建监听套接字
	//------------------------------------------------------

	// 如果未禁用 localhost，添加 localhost 监听地址
	if (! g_config.service_localhost_disabled) {
		add_localhost(&g_service_bind, CF_SOCK_OWNER_SERVICE);      // 普通服务
		add_localhost(&g_service_bind, CF_SOCK_OWNER_SERVICE_TLS);  // TLS 服务
	}

	// 初始化服务器监听套接字
	if (cf_socket_init_server(&g_service_bind, &g_sockets) < 0) {
		cf_crash(AS_SERVICE, "couldn't initialize service socket");
	}

	// 显示客户端服务监听信息
	cf_socket_show_server(AS_SERVICE, "client", &g_sockets);

	//------------------------------------------------------
	// 创建 accept 线程
	//------------------------------------------------------

	cf_info(AS_SERVICE, "starting accept thread");

	// 创建分离线程运行 accept 循环（接受新的客户端连接）
	cf_thread_create_detached(run_accept, NULL);
}

/**
 * 动态设置 service 线程数量
 *
 * 功能描述：
 * 运行时动态调整 service 线程池的大小，支持扩容和缩容。
 *
 * 扩容流程（增加线程）：
 * 1. 创建新的 service 线程
 * 2. 更新全局线程数配置
 * 3. 触发连接重分配以平衡负载
 *
 * 缩容流程（减少线程）：
 * 1. 更新全局线程数配置
 * 2. 向要停止的线程发送终止信号（空事务）
 * 3. 线程接收到空事务后会自动清理并退出
 *
 * 参数说明：
 * @n_threads: 新的线程数量，必须 <= MAX_SERVICE_THREADS
 *
 * 线程安全：
 * 使用线程锁保护线程上下文的修改操作
 */
void
as_service_set_threads(uint32_t n_threads)
{
	uint32_t old_n_threads = g_config.n_service_threads;

	if (n_threads > old_n_threads) {
		//------------------------------------------------------
		// 扩容：增加新的 service 线程
		//------------------------------------------------------

		// 创建新增的线程
		for (uint32_t sid = old_n_threads; sid < n_threads; sid++) {
			create_service_thread(sid);
		}

		// 更新全局配置
		g_config.n_service_threads = n_threads;

		// 触发连接重分配以平衡新线程的负载
		schedule_redistribution();
	}
	else if (n_threads < old_n_threads) {
		//------------------------------------------------------
		// 缩容：停止多余的 service 线程
		//------------------------------------------------------

		// 先更新配置，防止新连接分配到即将停止的线程
		g_config.n_service_threads = n_threads;

		// 向要停止的线程发送终止信号
		for (uint32_t sid = n_threads; sid < old_n_threads; sid++) {
			cf_mutex_lock(&g_thread_locks[sid]);

			thread_ctx* ctx = g_thread_ctxs[sid];

			cf_detail(AS_SERVICE, "sending terminator sid %u ctx %p", sid, ctx);

			// 创建空事务作为终止信号
			as_transaction tr;
			as_transaction_init_head(&tr, NULL, NULL);

			// 将终止信号发送到线程的事务队列
			cf_epoll_queue_push(&ctx->trans_q, &tr);
			g_thread_ctxs[sid] = NULL;  // 标记线程即将停止

			cf_mutex_unlock(&g_thread_locks[sid]);
		}
	}
	// else: 线程数没有变化，不做任何操作
}

bool
as_service_set_proto_fd_max(uint32_t val)
{
	struct rlimit rl;

	if (getrlimit(RLIMIT_NOFILE, &rl) < 0) {
		cf_crash(AS_SERVICE, "getrlimit() failed: %s", cf_strerror(errno));
	}

	if (val > (uint32_t)rl.rlim_cur) {
		cf_warning(AS_SERVICE, "can't set proto-fd-max %u > system limit %lu",
				val, rl.rlim_cur);
		return false;
	}

	if (val <= g_n_slots) {
		g_config.n_proto_fd_max = val;
		return true; // never shrink slots
	}

	size_t old_sz = g_n_slots * sizeof(as_file_handle*);
	size_t new_sz = val * sizeof(as_file_handle*);

	cf_mutex_lock(&g_reaper_lock);

	g_file_handles = cf_realloc(g_file_handles, new_sz);
	memset((uint8_t*)g_file_handles + old_sz, 0, new_sz - old_sz);

	for (uint32_t i = g_n_slots; i < val; i++) {
		cf_queue_push(&g_free_slots, &i);
	}

	g_n_slots = val;

	cf_mutex_unlock(&g_reaper_lock);

	g_config.n_proto_fd_max = val; // set *after* expanding slots

	return true;
}

void
as_service_rearm(as_file_handle* fd_h)
{
	if (fd_h->move_me) {
		cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
		assign_socket(fd_h); // rearms (EPOLLIN)

		fd_h->move_me = false;
		return;
	}

	rearm(fd_h, EPOLLIN);
}

/**
 * 将内部事务入队到 service 线程
 *
 * 功能描述：
 * 将内部产生的事务（如 proxy 请求、batch 子请求等）分发到 service 线程进行处理。
 * 使用轮询或 CPU 亲和性策略选择目标线程。
 *
 * 使用场景：
 * - Proxy 模块转发来自其他节点的请求
 * - Batch 模块分发批处理的子请求
 * - XDR 模块的跨数据中心复制请求
 * - 内部操作产生的二级事务
 *
 * 线程选择策略：
 * - CPU 绑定模式：选择当前 CPU 对应的线程
 * - 普通模式：轮询选择线程
 *
 * 参数说明：
 * @tr: 要入队的事务指针
 *
 * 线程安全：
 * 使用线程锁保护队列操作，支持多线程并发调用
 */
void
as_service_enqueue_internal(as_transaction* tr)
{
	while (true) {
		// 根据 CPU 绑定策略选择目标线程
		uint32_t sid = as_config_is_cpu_pinned() ?
				select_sid_pinned(cf_topo_current_cpu()) : select_sid();

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			// 线程存在且活跃，将事务入队
			cf_epoll_queue_push(&ctx->trans_q, tr);
			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		// 线程不存在或正在停止，尝试下一个线程
		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}

/**
 * 基于 key digest 将内部事务入队到特定 service 线程
 *
 * 功能描述：
 * 根据记录的 key digest 选择特定的 service 线程，确保相同 key 的操作
 * 在同一个线程中处理，有利于缓存局部性和减少锁竞争。
 *
 * 线程选择策略：
 * 使用 key digest 对应的分区 ID 对线程数取模，确保：
 * - 相同 key 的请求总是路由到同一线程
 * - 分区级别的操作可以在固定线程中串行化
 * - 提高缓存命中率和减少内存竞争
 *
 * 参数说明：
 * @tr: 包含 keyd 字段的事务指针
 *
 * 适用场景：
 * - 需要 key 级别亲和性的内部事务
 * - 分区级别的串行化操作
 * - 缓存友好的数据访问模式
 */
void
as_service_enqueue_internal_keyd(as_transaction* tr)
{
	while (true) {
		uint32_t sid = select_sid_keyd(&tr->keyd);

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			cf_epoll_queue_push(&ctx->trans_q, tr);
			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}


//==========================================================
// Local helpers - setup.
//

void
create_service_thread(uint32_t sid)
{
	thread_ctx* ctx = cf_malloc(sizeof(thread_ctx));

	cf_detail(AS_SERVICE, "starting sid %u ctx %p", sid, ctx);

	if (as_config_is_cpu_pinned()) {
		ctx->i_cpu = (cf_topo_cpu_index)(sid % cf_topo_count_cpus());
	}

	ctx->lock = &g_thread_locks[sid];
	cf_poll_create(&ctx->poll);
	cf_epoll_queue_init(&ctx->trans_q, AS_TRANSACTION_HEAD_SIZE, 64);

	cf_thread_create_transient(run_service, ctx);

	cf_mutex_lock(&g_thread_locks[sid]);

	g_thread_ctxs[sid] = ctx;

	cf_mutex_unlock(&g_thread_locks[sid]);
}

static void
add_localhost(cf_serv_cfg* serv_cfg, cf_sock_owner owner)
{
	// Localhost will only be added to the addresses, if we're not yet listening
	// on wildcard ("any") or localhost.

	cf_ip_port port = 0;

	for (uint32_t i = 0; i < serv_cfg->n_cfgs; i++) {
		if (serv_cfg->cfgs[i].owner != owner) {
			continue;
		}

		port = serv_cfg->cfgs[i].port;

		if (cf_ip_addr_is_any(&serv_cfg->cfgs[i].addr) ||
				cf_ip_addr_is_local(&serv_cfg->cfgs[i].addr)) {
			return;
		}
	}

	if (port == 0) {
		return;
	}

	cf_sock_cfg sock_cfg;

	cf_sock_cfg_init(&sock_cfg, owner);
	sock_cfg.port = port;
	cf_ip_addr_set_local(&sock_cfg.addr);

	if (cf_serv_cfg_add_sock_cfg(serv_cfg, &sock_cfg) < 0) {
		cf_crash(AS_SERVICE, "couldn't add localhost listening address");
	}
}


//==========================================================
// Local helpers - accept client connections.
//

static void*
run_accept(void* udata)
{
	(void)udata;

	cf_poll poll;
	cf_poll_create(&poll);

	cf_poll_add_sockets(poll, &g_sockets, EPOLLIN);

	while (true) {
		cf_poll_event events[N_EVENTS];
		int32_t n_events = cf_poll_wait(poll, events, N_EVENTS, -1);

		cf_assert(n_events >= 0, AS_SERVICE, "unexpected EINTR");

		for (uint32_t i = 0; i < (uint32_t)n_events; i++) {
			cf_socket* ssock = events[i].data;
			cf_socket csock;
			cf_sock_addr caddr;

			if (cf_socket_accept(ssock, &csock, &caddr) < 0) {
				if (errno == EMFILE || errno == ENFILE) {
					cf_ticker_warning(AS_SERVICE, "out of file descriptors");
					continue;
				}

				cf_crash(AS_SERVICE, "accept() failed: %d (%s)", errno,
						cf_strerror(errno));
			}

			cf_sock_cfg* cfg = ssock->cfg;

			// Ensure that proto_connections_closed is read first.
			uint64_t n_closed =
					as_load_uint64(&g_stats.proto_connections_closed);
			// TODO - ARM TSO plugin - will need barrier.
			uint64_t n_opened =
					as_load_uint64(&g_stats.proto_connections_opened);

			if (n_opened - n_closed >= g_config.n_proto_fd_max) {
				cf_ticker_warning(AS_SERVICE,
						"refusing client connection - proto-fd-max %u",
						g_config.n_proto_fd_max);

				cf_socket_close(&csock);
				cf_socket_term(&csock);
				continue;
			}

			cf_socket_keep_alive(&csock, 60, 60, 2);

			if (cfg->owner == CF_SOCK_OWNER_SERVICE_TLS) {
				tls_socket_prepare_server(&csock, g_service_tls);
			}

			as_file_handle* fd_h = cf_rc_alloc(sizeof(as_file_handle));
			// Ref for epoll instance.

			fd_h->poll_data_type = CF_POLL_DATA_CLIENT_IO;

			cf_sock_addr_to_string_safe(&caddr, fd_h->client,
					sizeof(fd_h->client));
			cf_socket_copy(&csock, &fd_h->sock);

			fd_h->last_used = cf_getns();
			fd_h->in_transaction = 0;
			fd_h->move_me = false;
			fd_h->reap_me = false;
			fd_h->is_xdr = false;
			fd_h->proto = NULL;
			fd_h->proto_unread = sizeof(as_proto);
			fd_h->security_filter = as_security_filter_create();

			cf_rc_reserve(fd_h); // ref for reaper

			cf_mutex_lock(&g_reaper_lock);

			uint32_t slot;

			if (cf_queue_pop(&g_free_slots, &slot, CF_QUEUE_NOWAIT) !=
					CF_QUEUE_OK) {
				cf_crash(AS_SERVICE, "cannot get free slot");
			}

			g_file_handles[slot] = fd_h;

			cf_mutex_unlock(&g_reaper_lock);

			assign_socket(fd_h); // arms (EPOLLIN)

			as_incr_uint64(&g_stats.proto_connections_opened);
		}
	}

	return NULL;
}


//==========================================================
// Local helpers - assign client connections to threads.
//

static void
assign_socket(as_file_handle* fd_h)
{
	while (true) {
		uint32_t sid;

		switch (g_config.auto_pin) {
		case CF_TOPO_AUTO_PIN_NONE:
			sid = select_sid();
			break;
		case CF_TOPO_AUTO_PIN_CPU:
		case CF_TOPO_AUTO_PIN_NUMA:
			sid = select_sid_pinned(cf_topo_socket_cpu(&fd_h->sock));
			break;
		case CF_TOPO_AUTO_PIN_ADQ:
			sid = select_sid_adq(cf_topo_socket_napi_id(&fd_h->sock));
			break;
		default:
			cf_crash(AS_SERVICE, "bad auto-pin %d", g_config.auto_pin);
			return;
		}

		cf_mutex_lock(&g_thread_locks[sid]);

		thread_ctx* ctx = g_thread_ctxs[sid];

		if (ctx != NULL) {
			fd_h->poll = ctx->poll;

			cf_poll_add_socket(fd_h->poll, &fd_h->sock,
					EPOLLIN | EPOLLONESHOT | EPOLLRDHUP, fd_h);

			cf_mutex_unlock(&g_thread_locks[sid]);
			break;
		}

		cf_mutex_unlock(&g_thread_locks[sid]);
	}
}

static uint32_t
select_sid(void)
{
	static uint32_t rr = 0;

	return rr++ % g_config.n_service_threads;
}

static uint32_t
select_sid_pinned(cf_topo_cpu_index i_cpu)
{
	static uint32_t rr[CPU_SETSIZE] = { 0 };

	uint16_t n_cpus = cf_topo_count_cpus();
	uint32_t threads_per_cpu = g_config.n_service_threads / n_cpus;

	uint32_t thread_ix = rr[i_cpu]++ % threads_per_cpu;

	return (thread_ix * n_cpus) + i_cpu;
}

static uint32_t
select_sid_adq(cf_topo_napi_id id)
{
	return id == 0 ? select_sid() : id % g_config.n_service_threads;
}

static uint32_t
select_sid_keyd(const cf_digest* keyd)
{
	return as_partition_getid(keyd) % g_config.n_service_threads;
}

static void
schedule_redistribution(void)
{
	cf_mutex_lock(&g_reaper_lock);

	uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

	for (uint32_t i = 0; n_remaining != 0; i++) {
		as_file_handle* fd_h = g_file_handles[i];

		if (fd_h != NULL) {
			fd_h->move_me = true;
			n_remaining--;
		}
	}

	cf_mutex_unlock(&g_reaper_lock);
}


//==========================================================
// Local helpers - demarshal client requests.
//

/**
 * Service 线程主循环函数
 *
 * 功能描述：
 * 每个 service 线程的主要工作函数，运行 epoll 事件循环处理客户端 I/O 和内部事务。
 *
 * 主要职责：
 * 1. 客户端连接 I/O 事件处理：读取客户端请求，解析协议，创建事务
 * 2. 内部事务处理：处理来自其他模块的内部事务（batch、proxy 等）
 * 3. XDR I/O 事件处理：处理跨数据中心复制的网络 I/O
 * 4. TLS 握手处理：处理加密连接的 SSL/TLS 协商
 *
 * 事件循环架构：
 * - 使用 epoll 机制监控多种类型的文件描述符
 * - 支持客户端连接、内部事务队列、XDR 连接、定时器等
 * - 事件驱动的异步 I/O 处理模型
 *
 * 线程绑定：
 * - 如果启用了 CPU 绑定，线程会绑定到特定的 CPU 核心
 * - 提高缓存局部性和减少 CPU 迁移开销
 *
 * 参数说明：
 * @udata: 线程上下文指针 (thread_ctx*)
 *
 * 返回值：
 * @return: NULL（线程退出时返回）
 *
 * 事件类型处理：
 * - CF_POLL_DATA_EPOLL_QUEUE: 内部事务队列事件
 * - CF_POLL_DATA_XDR_IO: XDR I/O 事件
 * - CF_POLL_DATA_XDR_TIMER: XDR 定时器事件
 * - CF_POLL_DATA_CLIENT_IO: 客户端连接 I/O 事件
 */
static void*
run_service(void* udata)
{
	thread_ctx* ctx = (thread_ctx*)udata;

	cf_detail(AS_SERVICE, "running ctx %p", ctx);

	//------------------------------------------------------
	// 线程初始化：CPU 绑定和 epoll 设置
	//------------------------------------------------------

	// 如果启用了 CPU 绑定，将线程绑定到指定的 CPU 核心
	if (as_config_is_cpu_pinned()) {
		cf_topo_pin_to_cpu(ctx->i_cpu);
	}

	// 获取本线程的 epoll 实例和事务队列
	cf_poll poll = ctx->poll;
	cf_epoll_queue* trans_q = &ctx->trans_q;

	// 将事务队列的事件文件描述符加入 epoll 监控
	cf_poll_add_fd(poll, trans_q->event_fd, EPOLLIN, trans_q);

	// 初始化 XDR 相关的 epoll 监控
	as_xdr_init_poll(poll);

	//------------------------------------------------------
	// 主事件循环：处理各种类型的 I/O 事件
	//------------------------------------------------------

	while (true) {
		cf_poll_event events[N_EVENTS];
		int32_t n_events = cf_poll_wait(poll, events, N_EVENTS, -1);
		uint64_t events_ns = cf_getns();  // 记录事件处理开始时间

		// 遍历所有触发的事件
		for (uint32_t i = 0; i < (uint32_t)n_events; i++) {
			uint32_t mask = events[i].events;    // 事件类型掩码
			void* data = events[i].data;         // 事件关联的数据指针

			uint8_t type = *(uint8_t*)data;      // 数据类型标识

			//------------------------------------------------------
			// 处理内部事务队列事件
			//------------------------------------------------------
			if (type == CF_POLL_DATA_EPOLL_QUEUE) {
				cf_assert(mask == EPOLLIN, AS_SERVICE,
						"unexpected event: 0x%0x", mask);

				// 处理内部事务，如果返回 false 表示收到终止信号
				if (start_internal_transaction(ctx)) {
					continue;
				}

				// 收到终止信号，停止线程并清理资源
				stop_service(ctx);
				return NULL;
			}

			//------------------------------------------------------
			// 处理 XDR I/O 事件
			//------------------------------------------------------
			if (type == CF_POLL_DATA_XDR_IO) {
				as_xdr_io_event(mask, data);
				continue;
			}

			//------------------------------------------------------
			// 处理 XDR 定时器事件
			//------------------------------------------------------
			if (type == CF_POLL_DATA_XDR_TIMER) {
				as_xdr_timer_event(events, n_events, i);
				continue;
			}

			//------------------------------------------------------
			// 处理客户端连接 I/O 事件
			//------------------------------------------------------
			// else - type == CF_POLL_DATA_CLIENT_IO

			as_file_handle* fd_h = data;

			// 检查连接错误事件
			if ((mask & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) != 0) {
				service_release_file_handle(fd_h);
				continue;
			}

			//------------------------------------------------------
			// 处理 TLS 握手
			//------------------------------------------------------
			if (tls_socket_needs_handshake(&fd_h->sock)) {
				int32_t tls_ev = tls_socket_accept(&fd_h->sock);

				if (tls_ev == EPOLLERR) {
					service_release_file_handle(fd_h);
					continue;
				}

				if (tls_ev == 0) {
					tls_socket_must_not_have_data(&fd_h->sock,
							"service handshake");
					tls_ev = EPOLLIN;
				}

				rearm(fd_h, (uint32_t)tls_ev);
				continue;
			}

			//------------------------------------------------------
			// 记录请求开始时间（用于延迟统计）
			//------------------------------------------------------
			if (fd_h->proto == NULL && fd_h->proto_unread == sizeof(as_proto)) {
				// 重载 last_used 为请求开始时间
				// 注意：延迟会包括此循环中该事件之前的无关事件
				fd_h->last_used = events_ns;
			}

			//------------------------------------------------------
			// 读取客户端数据并解析协议
			//------------------------------------------------------
			if (! process_readable(fd_h)) {
				service_release_file_handle(fd_h);
				continue;
			}

			// 确保 TLS 套接字没有剩余数据
			tls_socket_must_not_have_data(&fd_h->sock, "full client read");

			// 如果协议消息还未读取完整，重新监听读事件
			if (fd_h->proto_unread != 0) {
				rearm(fd_h, EPOLLIN);
				continue;
			}

			//------------------------------------------------------
			// 协议消息读取完成，创建事务进行处理
			//------------------------------------------------------
			// 注意：epoll 在事务期间不能再次触发此文件句柄
			// 我们会在事务结束时重新启用监听
			// 客户端消息已读满一条 as_proto：构造 as_transaction 并进入事务处理，最终可能走到 as_write_start
			start_transaction(fd_h);
		}
	}

	return NULL;
}

static void
stop_service(thread_ctx* ctx)
{
	cf_detail(AS_SERVICE, "stopping ctx %p", ctx);

	as_xdr_shutdown_poll();
	as_xdr_cleanup_tl_stats();

	while (true) {
		bool any_in_transaction = false;

		cf_mutex_lock(&g_reaper_lock);

		uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

		for (uint32_t i = 0; n_remaining != 0; i++) {
			as_file_handle* fd_h = g_file_handles[i];

			if (fd_h == NULL) {
				continue;
			}

			n_remaining--;

			// Ignore, if another thread's or INVALID_POLL.
			if (! cf_poll_equal(fd_h->poll, ctx->poll)) {
				continue;
			}

			// Don't transfer during TLS handshake - might need EPOLLOUT.
			if (tls_socket_needs_handshake(&fd_h->sock)) {
				service_release_file_handle(fd_h);
				continue;
			}

			if (fd_h->in_transaction != 0) {
				any_in_transaction = true;
				continue;
			}

			cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
			assign_socket(fd_h); // keeps armed (EPOLLIN)
		}

		cf_mutex_unlock(&g_reaper_lock);

		if (! any_in_transaction) {
			break;
		}

		sleep(1);
	}

	cf_poll_destroy(ctx->poll);
	cf_epoll_queue_destroy(&ctx->trans_q);

	cf_detail(AS_SERVICE, "stopped ctx %p", ctx);

	cf_free(ctx);
}

static void
service_release_file_handle(as_file_handle* fd_h)
{
	cf_poll_delete_socket(fd_h->poll, &fd_h->sock);
	fd_h->poll = INVALID_POLL;
	fd_h->reap_me = true;
	as_release_file_handle(fd_h);
}

/**
 * 处理客户端连接的可读数据
 *
 * 功能描述：
 * 这是Aerospike协议解析的核心函数，负责从客户端socket读取数据
 * 并按照as_proto协议格式进行解析。采用两阶段读取：先读协议头，
 * 再读消息体，确保完整接收一个请求后才返回。
 *
 * 协议解析流程：
 * 1. 第一阶段：读取协议头（as_proto结构，固定大小）
 *    - 验证协议版本
 *    - 检查消息类型
 *    - 验证消息体大小
 *    - 进行字节序转换
 * 2. 第二阶段：读取消息体（变长，大小由协议头指定）
 *    - 分配内存存储完整消息
 *    - 逐步读取直到完成
 *
 * 错误检测：
 * - TLS协议误用检测：检查是否在非TLS端口收到TLS握手
 * - 版本兼容性检查：支持当前版本和向后兼容
 * - 消息大小验证：防止过大消息导致内存耗尽
 * - 协议类型验证：确保消息类型有效
 *
 * 网络I/O特性：
 * - 非阻塞读取：使用EAGAIN处理暂无数据情况
 * - 连接断开检测：sz=0表示对端关闭连接
 * - 分片读取支持：支持TCP分片传输的消息
 * - SSL缓冲区处理：确保OpenSSL内部缓冲区被完全读取
 *
 * @param fd_h 客户端文件句柄，包含：
 *             - sock: 客户端socket连接
 *             - proto_hdr: 协议头缓冲区
 *             - proto: 完整协议消息指针
 *             - proto_unread: 剩余未读字节数
 *
 * @return true: 成功读取完整消息，可以开始事务处理
 *         false: 读取失败或连接错误，需要关闭连接
 *
 * 内存管理：
 * - 协议头使用栈缓冲区（proto_hdr）
 * - 完整消息使用堆内存（cf_malloc分配）
 * - 调用者负责释放proto指向的内存
 */
static bool
process_readable(as_file_handle* fd_h)
{
	// 确定当前读取的目标缓冲区位置
	// 如果proto为NULL，说明正在读取协议头；否则读取消息体
	uint8_t* end = fd_h->proto == NULL ?
			(uint8_t*)&fd_h->proto_hdr + sizeof(as_proto) : // header
			fd_h->proto->body + fd_h->proto->sz; // body

	while (true) {
		//------------------------------------------------------
		// 从socket读取数据
		//------------------------------------------------------
		int32_t sz = cf_socket_recv(&fd_h->sock, end - fd_h->proto_unread,
				fd_h->proto_unread, 0);

		// 处理读取错误
		if (sz < 0) {
			// EAGAIN/EWOULDBLOCK表示暂时无数据，属于正常情况
			return errno == EAGAIN || errno == EWOULDBLOCK;
		}

		// sz == 0表示对端关闭连接
		if (sz == 0) {
			return false;
		}

		// 更新剩余未读字节数
		fd_h->proto_unread -= (uint64_t)sz;

		// 如果还有数据未读完，继续读取
		if (fd_h->proto_unread != 0) {
			continue; // 继续读取socket（及OpenSSL内部缓冲区）直到读完
		}

		// 检查是否已完成整个请求的读取
		if (fd_h->proto != NULL) {
			return true; // 完整请求读取完成
		}

		//------------------------------------------------------
		// 协议头读取完成，开始处理和验证
		//------------------------------------------------------

		// 检查是否是发送到非TLS端口的TLS ClientHello，启发式检测：
		//   - tls[0] == ContentType.handshake (22)
		//   - tls[1] == ProtocolVersion.major (3)
		//   - tls[5] == HandshakeType.client_hello (1)
		uint8_t* tls = (uint8_t*)&fd_h->proto_hdr;

		if (tls[0] == 22 && tls[1] == 3 && tls[5] == 1) {
			cf_warning(AS_SERVICE, "ignoring TLS connection from %s",
					fd_h->client);
			return false;
		}

		// 版本兼容性检查
		// 为了向后兼容，允许版本0的安全消息
		if (fd_h->proto_hdr.version != PROTO_VERSION &&
				! (fd_h->proto_hdr.version == 0 &&
						fd_h->proto_hdr.type == PROTO_TYPE_SECURITY)) {
			cf_warning(AS_SERVICE, "unsupported proto version %d from %s",
					fd_h->proto_hdr.version, fd_h->client);
			return false;
		}

		// 验证协议类型
		if (! as_proto_is_valid_type(&fd_h->proto_hdr)) {
			cf_warning(AS_SERVICE, "unsupported proto type %d from %s",
					fd_h->proto_hdr.type, fd_h->client);
			return false;
		}

		// 字节序转换（网络字节序转主机字节序）
		as_proto_swap(&fd_h->proto_hdr);

		// 验证消息体大小，防止过大消息
		if (fd_h->proto_hdr.sz > PROTO_SIZE_MAX) {
			cf_warning(AS_SERVICE, "invalid proto size %lu from %s",
					(uint64_t)fd_h->proto_hdr.sz, fd_h->client);
			return false;
		}

		//------------------------------------------------------
		// 分配内存并准备读取消息体
		//------------------------------------------------------

		// 分配完整协议消息的内存（头部+消息体）
		fd_h->proto = cf_malloc(sizeof(as_proto) + fd_h->proto_hdr.sz);
		memcpy(fd_h->proto, &fd_h->proto_hdr, sizeof(as_proto));

		// 设置消息体读取参数
		fd_h->proto_unread = fd_h->proto->sz;
		end = fd_h->proto->body + fd_h->proto->sz;
	}
}

/**
 * 启动事务处理
 *
 * 功能描述：
 * 将完整接收的客户端协议消息转换为 as_transaction 对象并分发处理。
 * 这是从网络 I/O 到事务处理的关键转换点。
 *
 * 处理流程：
 * 1. 增加连接的事务计数器（防止连接在事务期间被回收）
 * 2. 根据协议类型进行分类处理：
 *    - INFO 类型：创建 info 事务直接处理
 *    - SECURITY 类型：创建安全认证事务
 *    - 压缩消息：解压缩后继续处理
 *    - 数据操作：创建标准数据事务
 * 3. 批处理检测：如果是批处理请求，分发到批处理模块
 * 4. 单条事务：准备分区信息后分发到事务处理器
 *
 * 事务类型分发：
 * - as_info(): 信息查询事务（端口、状态、配置等）
 * - as_security_transact(): 安全认证事务
 * - as_batch_queue_task(): 批处理事务
 * - as_tsvc_process_transaction(): 单条数据事务
 *
 * 协议处理特性：
 * - 支持消息压缩/解压缩
 * - XDR 连接的特殊配置（缓冲区大小等）
 * - 事务超时和错误处理
 *
 * 参数说明：
 * @fd_h: 客户端连接的文件句柄，包含完整的协议消息
 *
 * 内存管理：
 * - 协议消息的所有权转移到事务对象
 * - 连接引用计数管理防止提前释放
 */
static void
start_transaction(as_file_handle* fd_h)
{
	//------------------------------------------------------
	// 事务开始：增加连接的事务计数器
	//------------------------------------------------------

	// as_end_of_transaction() 会重新启用监听然后递减计数器，所以这里可能 > 1
	as_incr_uint32(&fd_h->in_transaction);

	uint64_t start_ns = fd_h->last_used;  // 请求开始时间
	as_proto* proto = fd_h->proto;        // 协议消息

	// 重置连接状态以准备接收下一条消息
	fd_h->proto = NULL;
	fd_h->proto_unread = sizeof(as_proto);

	//------------------------------------------------------
	// INFO 类型事务处理
	//------------------------------------------------------
	if (proto->type == PROTO_TYPE_INFO) {
		// 创建 info 事务结构
		as_info_transaction it = {
			.fd_h = fd_h,
			.proto = proto,
			.start_time = start_ns
		};

		// 直接处理 info 请求（查询服务器状态、配置等）
		as_info(&it);
		return;
	}

	//------------------------------------------------------
	// 创建标准数据事务对象
	//------------------------------------------------------

	// 单条数据事务：用当前 proto 构造 as_transaction，origin=FROM_CLIENT，然后交给事务处理
	// 若是 batch 则入队批处理；否则直接 as_tsvc_process_transaction -> 写请求会 as_write_start(tr)
	as_transaction tr;
	as_transaction_init_head(&tr, NULL, (cl_msg*)proto);

	tr.origin = FROM_CLIENT;       // 标记事务来源为客户端
	tr.from.proto_fd_h = fd_h;     // 关联客户端连接
	tr.start_time = start_ns;      // 设置请求开始时间

	//------------------------------------------------------
	// SECURITY 类型事务处理
	//------------------------------------------------------
	if (proto->type == PROTO_TYPE_SECURITY) {
		as_security_transact(&tr);
		return;
	}

	//------------------------------------------------------
	// 压缩消息解压处理
	//------------------------------------------------------
	if (proto->type == PROTO_TYPE_AS_MSG_COMPRESSED) {
		// 解压缩消息
		uint32_t result = as_proto_uncompress((as_comp_proto*)proto,
				(as_proto**)&tr.msgp);

		if (result != AS_OK) {
			as_transaction_demarshal_error(&tr, result);
			return;
		}

		cf_free(proto);  // 释放原始压缩消息
	}

	//------------------------------------------------------
	// XDR 连接特殊配置
	//------------------------------------------------------
	if (as_transaction_is_xdr(&tr) && ! fd_h->is_xdr) {
		config_xdr_socket(&fd_h->sock);  // 配置 XDR 专用的缓冲区大小
		fd_h->is_xdr = true;
	}

	//------------------------------------------------------
	// 批处理事务分发
	//------------------------------------------------------
	if (tr.msgp->msg.info1 & AS_MSG_INFO1_BATCH) {
		as_batch_queue_task(&tr);
		return;
	}

	//------------------------------------------------------
	// 单条事务处理
	//------------------------------------------------------

	// 单条非 batch：准备 partition 等后直接进入事务处理；写请求在此链会调用 as_write_start(tr)
	if (! as_transaction_prepare(&tr, true)) {
		as_transaction_demarshal_error(&tr, AS_ERR_PARAMETER);
		return;
	}

	// 分发到事务服务模块进行处理
	as_tsvc_process_transaction(&tr);
}

static void
config_xdr_socket(cf_socket* sock)
{
	cf_socket_set_receive_buffer(sock, XDR_READ_BUFFER_SIZE);
	cf_socket_set_send_buffer(sock, XDR_WRITE_BUFFER_SIZE);
	cf_socket_set_window(sock, XDR_READ_BUFFER_SIZE);
	cf_socket_enable_nagle(sock);
}


//==========================================================
// Local helpers - reap idle and bad connections.
//

static void
start_reaper(void)
{
	g_n_slots = g_config.n_proto_fd_max;
	g_file_handles = cf_calloc(g_n_slots, sizeof(as_file_handle*));

	cf_queue_init(&g_free_slots, sizeof(uint32_t), g_n_slots, false);

	for (uint32_t i = 0; i < g_n_slots; i++) {
		cf_queue_push(&g_free_slots, &i);
	}

	cf_info(AS_SERVICE, "starting reaper thread");

	cf_thread_create_detached(run_reaper, NULL);
}

/**
 * 连接回收线程主循环
 *
 * 功能描述：
 * 这是Aerospike服务层的连接清理线程，负责定期扫描所有客户端连接，
 * 清理空闲连接、错误连接和过期连接，维护连接池的健康状态。
 *
 * 主要职责：
 * 1. 空闲连接检测：清理超过空闲时间限制的连接
 * 2. 错误连接清理：清理标记为reap_me的问题连接
 * 3. 安全令牌刷新：定期更新连接的安全认证状态
 * 4. 连接池管理：维护空闲slot队列，支持连接重用
 *
 * 扫描策略：
 * - 每秒执行一次完整扫描
 * - 跳过正在事务处理中的连接（保护活跃连接）
 * - 优先处理明确标记的错误连接
 * - 基于最后使用时间判断空闲连接
 *
 * 连接生命周期管理：
 * - 使用引用计数确保线程安全
 * - 支持跨线程的连接共享和释放
 * - 正确处理epoll事件的取消注册
 *
 * 性能优化：
 * - 计算活跃连接数，避免扫描空slot
 * - 批量处理连接清理，减少锁竞争
 * - 延迟删除策略，避免影响正在进行的事务
 *
 * @param udata 未使用的参数
 * @return NULL（线程退出时返回）
 *
 * 线程安全：
 * - 使用g_reaper_lock保护文件句柄数组
 * - 与accept线程和service线程协调工作
 * - 确保连接在多线程环境下的安全释放
 */
static void*
run_reaper(void* udata)
{
	(void)udata;

	while (true) {
		// 每秒执行一次连接清理
		sleep(1);

		//------------------------------------------------------
		// 准备清理参数
		//------------------------------------------------------

		// 检查是否需要刷新安全令牌
		bool security_refresh = as_security_should_refresh();

		// 计算连接空闲超时时间（纳秒）
		uint64_t kill_ns = (uint64_t)g_config.proto_fd_idle_ms * 1000000;
		uint64_t now_ns = cf_getns();

		//------------------------------------------------------
		// 扫描所有活跃连接
		//------------------------------------------------------

		cf_mutex_lock(&g_reaper_lock);

		// 计算当前活跃连接数（总slot数减去空闲slot数）
		uint32_t n_remaining = g_n_slots - cf_queue_sz(&g_free_slots);

		for (uint32_t i = 0; n_remaining != 0; i++) {
			as_file_handle* fd_h = g_file_handles[i];

			// 跳过空slot
			if (fd_h == NULL) {
				continue;
			}

			n_remaining--;  // 找到一个活跃连接

			//------------------------------------------------------
			// 安全令牌刷新
			//------------------------------------------------------
			if (security_refresh) {
				as_security_refresh(fd_h);
			}

			//------------------------------------------------------
			// 处理标记为删除的连接
			//------------------------------------------------------

			// reap_me标志优先于事务状态检查
			if (fd_h->reap_me) {
				g_file_handles[i] = NULL;                    // 清空slot
				cf_queue_push_head(&g_free_slots, &i);      // 回收slot到队列头部
				as_release_file_handle(fd_h);               // 释放连接资源
				continue;
			}

			//------------------------------------------------------
			// 跳过正在事务处理中的连接
			//------------------------------------------------------
			if (fd_h->in_transaction != 0) {
				continue;
			}

			//------------------------------------------------------
			// 检查连接空闲超时
			//------------------------------------------------------

			// 如果设置了空闲超时且连接已超时，则关闭连接
			if (kill_ns != 0 && fd_h->last_used + kill_ns < now_ns) {
				cf_socket_shutdown(&fd_h->sock);  // 关闭socket，触发epoll错误事件

				g_file_handles[i] = NULL;                    // 清空slot
				cf_queue_push_head(&g_free_slots, &i);      // 回收slot
				as_release_file_handle(fd_h);               // 释放连接资源

				g_stats.reaper_count++;  // 更新统计计数器
			}
		}

		cf_mutex_unlock(&g_reaper_lock);
	}

	return NULL;
}


//==========================================================
// Local helpers - transaction queue.
//

/**
 * 启动内部事务处理
 *
 * 功能描述：
 * 从service线程的内部事务队列中取出并处理一个事务。内部事务包括
 * 来自其他模块的请求，如proxy转发、batch子请求、XDR复制等。
 *
 * 处理流程：
 * 1. 加锁保护队列访问
 * 2. 从事务队列中弹出一个事务
 * 3. 检查事务有效性
 * 4. 分发到事务处理器
 *
 * 事务来源：
 * - Proxy模块：跨节点转发的事务
 * - Batch模块：批处理分解的子事务
 * - XDR模块：跨数据中心复制事务
 * - 内部操作：系统级别的数据操作
 *
 * 特殊处理：
 * - msgp为NULL的事务表示线程终止信号
 * - 线程安全：使用互斥锁保护队列操作
 *
 * @param ctx 服务线程上下文
 * @return true: 成功处理一个事务，继续运行
 *         false: 收到终止信号，应该停止线程
 */
static bool
start_internal_transaction(thread_ctx* ctx)
{
	as_transaction tr;

	//------------------------------------------------------
	// 从内部事务队列中获取事务
	//------------------------------------------------------

	cf_mutex_lock(ctx->lock);

	// 从本线程事务队列取出由 as_service_enqueue_internal 等压入的事务
	// 这些事务来源包括：proxy 转发、batch 子请求、XDR 复制等
	if (! cf_epoll_queue_pop(&ctx->trans_q, &tr)) {
		cf_crash(AS_SERVICE, "unable to pop from transaction queue");
	}

	cf_mutex_unlock(ctx->lock);

	//------------------------------------------------------
	// 检查是否为线程终止信号
	//------------------------------------------------------

	// msgp 为 NULL 表示这是线程终止信号
	if (tr.msgp == NULL) {
		return false;  // 返回false指示线程应该停止
	}

	//------------------------------------------------------
	// 处理正常的内部事务
	//------------------------------------------------------

	// 分发事务到事务处理器进行处理
	as_tsvc_process_transaction(&tr);

	return true;  // 返回true表示继续处理更多事务
}
