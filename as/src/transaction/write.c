/*
 * write.c
 *
 * Aerospike 写事务处理核心模块
 * 负责处理所有写操作：包括客户端写入、代理写入、批量子请求写入等
 *
 * =============================================================================
 * 模块功能说明：
 *
 * 本模块是 Aerospike 数据库写操作的核心处理引擎，负责处理所有类型的写事务。
 *
 * 主要职责：
 * 1. 写事务生命周期管理 - 从请求接收到响应发送的完整流程
 * 2. 副本解析与一致性保证 - 确保多副本间的数据一致性
 * 3. 主副本写入协调 - 管理主节点和副本节点的写入顺序
 * 4. Bin操作执行引擎 - 处理各种数据类型的写入操作
 * 5. 超时和错误处理 - 保证写操作的可靠性和及时性
 * 6. XDR集成 - 跨数据中心复制的写入事件处理
 * 7. 二级索引更新 - 维护索引的一致性
 * 8. 存储引擎集成 - 与底层存储系统的交互
 *
 * 关键数据流：
 * 客户端写请求 → as_write_start() → [副本解析] → write_master() →
 * [bin操作执行] → [副本同步] → [索引更新] → [XDR通知] → 响应发送
 *
 * 状态机转换图：
 * START → 权限验证 → {
 *   需要副本解析 → dup_res → write_master → MASTER_PROCESSING
 *   直接写入 → write_master → MASTER_PROCESSING
 * }
 *
 * MASTER_PROCESSING → bin操作执行 → {
 *   成功 → 副本同步 → REPL_WRITE
 *   失败 → 错误处理 → DONE_ERROR
 * }
 *
 * REPL_WRITE → 等待副本响应 → {
 *   所有副本成功 → 索引更新 → XDR通知 → DONE_SUCCESS
 *   副本失败/超时 → 错误处理 → DONE_ERROR
 *   部分成功 → 根据配置决定 → DONE_SUCCESS/DONE_ERROR
 * }
 *
 * 主要状态：
 * - TRANS_IN_PROGRESS: 事务正在异步处理中
 * - TRANS_WAITING: 等待其他资源或操作完成
 * - TRANS_DONE_SUCCESS: 写操作成功完成
 * - TRANS_DONE_ERROR: 写操作失败
 *
 * 核心函数：
 * 1. as_write_start() - 写事务入口点，负责初始化和流程控制
 * 2. write_dup_res_start_cb() - 副本解析启动回调
 * 3. write_master() - 主副本写入处理核心逻辑
 * 4. write_master_bin_ops() - bin操作执行引擎
 * 5. write_repl_write_cb() - 副本写入完成回调
 * 6. write_timeout_cb() - 超时处理回调
 * 7. write_done_cb() - 写操作完成清理回调
 * =============================================================================
 *
 * Copyright (C) 2016-2021 Aerospike, Inc.
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

#include "transaction/write.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/expop.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/truncate.h"
#include "base/xdr.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/replica_write.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// 类型定义和常量
// Typedefs & constants.
//

// 单个事务允许的最大操作数量 (32K)
#define MAX_N_OPS (32 * 1024)

// 编译时断言：确保记录最大 bin 数量加上最大操作数不超过 64K 限制
COMPILER_ASSERT(RECORD_MAX_BINS + MAX_N_OPS < 64 * 1024);

// 栈上分配的粒子缓冲区大小 (1MB) - 用于临时存储 bin 数据
#define STACK_PARTICLES_SIZE (1024 * 1024)


//==========================================================
// 前向声明 - 定义本文件中主要函数的接口
// Forward declarations.
//

// 重复解析相关回调函数
static void write_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r);
static bool write_dup_res_cb(rw_request* rw);

// 副本写入相关函数
static void start_write_repl_write(rw_request* rw, as_transaction* tr);
static void start_write_repl_write_forget(rw_request* rw, as_transaction* tr);
static void write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr);
static void write_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr);
static void write_repl_write_cb(rw_request* rw);

// 响应发送和超时处理
static void send_write_response(as_transaction* tr, cf_dyn_buf* db);
static void write_timeout_cb(rw_request* rw);

// 主副本写入核心逻辑
static transaction_status write_master(rw_request* rw, as_transaction* tr);
static void write_master_failed(as_transaction* tr, as_index_ref* r_ref, bool record_created, as_index_tree* tree, as_storage_rd* rd, int result_code);
static int write_master_preprocessing(as_transaction* tr);
static int write_master_policies(as_transaction* tr, bool* p_must_not_create, bool* p_is_replace);
static bool check_msg_set_name(as_transaction* tr, const char* set_name);
static int write_master_apply(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd, bool is_replace, rw_request* rw, bool* is_delete);

// Bin 操作处理函数
static int write_master_bin_ops(as_transaction* tr, as_storage_rd* rd, cf_ll_buf* particles_llb, cf_dyn_buf* db);
static int write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd, as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins, as_bin* result_bins, uint32_t* p_n_result_bins, cf_ll_buf* particles_llb);


//==========================================================
// 内联函数和宏定义 - 统计信息更新辅助函数
// Inlines & macros.
//

/**
 * 更新客户端写操作统计信息
 * @param ns 命名空间指针
 * @param result_code 操作结果码
 * @param is_xdr_op 是否为 XDR 操作
 */
//==========================================================
// 内联函数 - 统计信息更新
//

/**
 * 更新客户端写操作统计信息
 *
 * @param ns 命名空间对象
 * @param result_code 操作结果码
 * @param is_xdr_op 是否为XDR操作
 *
 * 功能说明：
 * 根据写操作的结果更新相应的统计计数器，用于监控和性能分析。
 * 区分普通写入和XDR写入的统计信息。
 *
 * 统计类别：
 * - 成功写入：n_client_write_success / n_xdr_write_success
 * - 一般错误：n_client_write_error / n_xdr_write_error
 * - 超时错误：n_client_write_timeout / n_xdr_write_timeout
 * - 记录不存在：仅普通写入统计
 * - 过滤排除：仅普通写入统计
 */
static inline void
client_write_update_stats(as_namespace* ns, uint8_t result_code, bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		// 写入成功统计
		as_incr_uint64(&ns->n_client_write_success);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_success);
		}
		break;
	default:
		// 写入错误统计
		as_incr_uint64(&ns->n_client_write_error);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_error);
		}
		break;
	case AS_ERR_TIMEOUT:
		// 写入超时统计
		as_incr_uint64(&ns->n_client_write_timeout);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_client_write_timeout);
		}
		break;
	case AS_ERR_FILTERED_OUT:
		// 被过滤器过滤掉的写入（不能是 XDR 写入）
		// Can't be an XDR write.
		as_incr_uint64(&ns->n_client_write_filtered_out);
		break;
	}
}

/**
 * 更新来自代理的写操作统计信息
 * @param ns 命名空间指针
 * @param result_code 操作结果码
 * @param is_xdr_op 是否为 XDR 操作
 */
/**
 * 更新代理节点写操作统计信息
 *
 * @param ns 命名空间对象
 * @param result_code 操作结果码
 * @param is_xdr_op 是否为XDR操作
 *
 * 功能说明：
 * 当写请求来自代理节点时，更新相应的统计信息。
 * 代理写入通常发生在分区迁移或负载均衡场景中。
 *
 * 统计类别：
 * - 成功写入：n_from_proxy_write_success / n_from_proxy_xdr_write_success
 * - 一般错误：n_from_proxy_write_error / n_from_proxy_xdr_write_error
 * - 超时错误：n_from_proxy_write_timeout / n_from_proxy_xdr_write_timeout
 * - 记录不存在：仅普通写入统计
 * - 过滤排除：仅普通写入统计
 */
static inline void
from_proxy_write_update_stats(as_namespace* ns, uint8_t result_code,
		bool is_xdr_op)
{
	switch (result_code) {
	case AS_OK:
		// 代理写入成功统计
		as_incr_uint64(&ns->n_from_proxy_write_success);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_success);
		}
		break;
	default:
		// 代理写入错误统计
		as_incr_uint64(&ns->n_from_proxy_write_error);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_error);
		}
		break;
	case AS_ERR_TIMEOUT:
		// 代理写入超时统计
		as_incr_uint64(&ns->n_from_proxy_write_timeout);
		if (is_xdr_op) {
			as_incr_uint64(&ns->n_xdr_from_proxy_write_timeout);
		}
		break;
	case AS_ERR_FILTERED_OUT:
		// 代理写入被过滤统计（不能是 XDR 写入）
		// Can't be an XDR write.
		as_incr_uint64(&ns->n_from_proxy_write_filtered_out);
		break;
	}
}

/**
 * 更新批量子写操作统计信息（不能是 XDR 写入）
 * @param ns 命名空间指针
 * @param result_code 操作结果码
 */
// Can't be an XDR write.
/**
 * 更新批处理子写操作统计信息
 *
 * @param ns 命名空间对象
 * @param result_code 操作结果码
 *
 * 功能说明：
 * 批处理操作中每个子写操作的统计信息更新。
 * 批处理写入可以显著提高吞吐量，但需要单独统计每个子操作的结果。
 *
 * 统计类别：
 * - 成功写入：n_batch_sub_write_success
 * - 一般错误：n_batch_sub_write_error
 * - 超时错误：n_batch_sub_write_timeout
 * - 记录不存在：n_batch_sub_write_not_found
 * - 过滤排除：n_batch_sub_write_filtered_out
 */
static inline void
batch_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		// 批量子写入成功统计
		as_incr_uint64(&ns->n_batch_sub_write_success);
		break;
	default:
		// 批量子写入错误统计
		as_incr_uint64(&ns->n_batch_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		// 批量子写入超时统计
		as_incr_uint64(&ns->n_batch_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		// 批量子写入被过滤统计
		as_incr_uint64(&ns->n_batch_sub_write_filtered_out);
		break;
	}
}

/**
 * 更新来自代理的批量子写操作统计信息（不能是 XDR 写入）
 * @param ns 命名空间指针
 * @param result_code 操作结果码
 */
// Can't be an XDR write.
/**
 * 更新来自代理的批处理子写操作统计信息
 *
 * @param ns 命名空间对象
 * @param result_code 操作结果码
 *
 * 功能说明：
 * 当批处理写请求来自代理节点时的统计信息更新。
 * 这种情况通常发生在跨节点的批处理操作中。
 *
 * 统计类别：
 * - 成功写入：n_from_proxy_batch_sub_write_success
 * - 一般错误：n_from_proxy_batch_sub_write_error
 * - 超时错误：n_from_proxy_batch_sub_write_timeout
 * - 记录不存在：n_from_proxy_batch_sub_write_not_found
 * - 过滤排除：n_from_proxy_batch_sub_write_filtered_out
 */
static inline void
from_proxy_batch_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		// 来自代理的批量子写入成功统计
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_success);
		break;
	default:
		// 来自代理的批量子写入错误统计
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		// 来自代理的批量子写入超时统计
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT:
		// 来自代理的批量子写入被过滤统计
		as_incr_uint64(&ns->n_from_proxy_batch_sub_write_filtered_out);
		break;
	}
}

/**
 * 更新操作子写入统计信息
 * @param ns 命名空间指针
 * @param result_code 操作结果码
 */
/**
 * 更新操作子写入统计信息
 *
 * @param ns 命名空间对象
 * @param result_code 操作结果码
 *
 * 功能说明：
 * 更新来自内部操作系统（如IOP - Internal Operations）的写入统计。
 * 这些操作通常是系统内部触发的写入，如数据迁移、修复等。
 *
 * 统计类别：
 * - 成功写入：n_ops_sub_write_success
 * - 一般错误：n_ops_sub_write_error
 * - 超时错误：n_ops_sub_write_timeout
 * - 记录不存在：n_ops_sub_write_not_found
 * - 过滤排除：n_ops_sub_write_filtered_out
 */
static inline void
ops_sub_write_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		// 操作子写入成功统计
		as_incr_uint64(&ns->n_ops_sub_write_success);
		break;
	default:
		// 操作子写入错误统计
		as_incr_uint64(&ns->n_ops_sub_write_error);
		break;
	case AS_ERR_TIMEOUT:
		// 操作子写入超时统计
		as_incr_uint64(&ns->n_ops_sub_write_timeout);
		break;
	case AS_ERR_FILTERED_OUT: // 不包括元数据过滤的情况
		// 操作子写入被过滤统计
		// doesn't include those filtered out by metadata
		as_incr_uint64(&ns->n_ops_sub_write_filtered_out);
		break;
	}
}


//==========================================================
// Public API.
//

//==========================================================
// 公共 API - 写事务处理主入口
// Public API.
//

/**
 * 写事务入口函数：所有写操作（客户端/代理/批量子请求）的统一入口
 *
 * 主要处理流程：
 * 1. XDR 过滤检查和存储过载检查
 * 2. 创建 rw_request 并插入到哈希表中用于重复检测
 * 3. 可选的重复解析处理（如果存在重复写入）
 * 4. 在主副本上执行写操作
 * 5. 可选的副本写入处理
 *
 * @param tr 事务对象指针，包含写入请求的所有信息
 * @return transaction_status 事务状态：
 *         - TRANS_DONE_SUCCESS: 写入成功完成
 *         - TRANS_DONE_ERROR: 写入失败
 *         - TRANS_IN_PROGRESS: 写入正在进行中（等待副本确认）
 *         - TRANS_WAITING: 事务等待中（重复解析或其他阻塞情况）
 *
 * 关键逻辑：
 * - 使用 (namespace_ix, key_digest) 作为哈希键确保同一记录的并发写入被正确排序
 * - 支持重复解析机制处理分区副本间的写入冲突
 * - 根据一致性策略决定是否等待副本写入确认
 */
/**
 * 启动写事务处理流程
 *
 * @param tr 事务对象指针
 * @return transaction_status 事务状态
 *   - TRANS_IN_PROGRESS: 事务正在异步处理中
 *   - TRANS_DONE_SUCCESS: 写操作成功完成
 *   - TRANS_DONE_ERROR: 写操作失败
 *   - TRANS_WAITING: 事务等待中（重新排队）
 *
 * 功能说明：
 * 这是写操作的主入口点，负责：
 * 1. 执行预处理检查（时钟偏斜、操作数量等）
 * 2. 应用写入策略和权限检查
 * 3. 判断是否需要进行副本解析
 * 4. 创建或找到 rw_request 并管理并发控制
 * 5. 根据需要启动副本解析或直接进入写入流程
 * 6. 处理各种错误情况和异常场景
 *
 * 状态机转换：
 * START → 预处理检查 → {
 *   检查失败 → send_write_response(错误) → DONE_ERROR
 *   检查通过 → 判断副本解析需求 → {
 *     无需解析 → write_master → MASTER_PROCESSING → {
 *       成功 → 副本写入 → REPL_WRITE → DONE_SUCCESS
 *       失败 → DONE_ERROR
 *     }
 *     需要解析 → dup_res_start → IN_PROGRESS → write_dup_res_cb
 *     哈希冲突 → WAITING（重新排队）
 *   }
 * }
 *
 * 关键控制点：
 * - rw_request哈希表插入：防止并发写入冲突
 * - 副本解析决策：基于一致性策略和副本状态
 * - 错误处理路径：确保资源正确释放和响应发送
 */
transaction_status
as_write_start(as_transaction* tr)
{
	// 性能基准测试点记录
	BENCHMARK_START(tr, write, FROM_CLIENT);
	BENCHMARK_START_FROM_BATCH(tr);
	BENCHMARK_START(tr, ops_sub, FROM_IOPS);

	// XDR 过滤器检查：某些写入可能被 XDR 策略禁止
	// Apply XDR filter.
	if (! xdr_allows_write(tr)) {
		tr->result_code = AS_ERR_FORBIDDEN;
		send_write_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// 存储过载检查：防止在存储系统过载时接受新的写入请求
	// Check that we aren't backed up.
	if (as_storage_overloaded(tr->rsv.ns, 0, "write")) {
		tr->result_code = AS_ERR_DEVICE_OVERLOAD;
		send_write_response(tr, NULL);
		return TRANS_DONE_ERROR;
	}

	// 按 (namespace_ix, key_digest) 创建 rw_request 并插入哈希，用于同一 key 的并发写合并与重复解析。
	// Create rw_request and add to hash.
	rw_request_hkey hkey = { tr->rsv.ns->ix, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);
	transaction_status status = rw_request_hash_insert(&hkey, rw, tr);

	// 如果 rw_request 未能插入哈希表，说明已有相同 key 的事务在处理，当前事务结束
	// If rw_request wasn't inserted in hash, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_write_response(tr, NULL);
		}

		return status;
	}
	// else - rw_request is now in hash, continue...

	// 如果命名空间禁用了重复解析，清零重复计数
	if (tr->rsv.ns->write_dup_res_disabled) {
		// Note - preventing duplicate resolution this way allows
		// rw_request_destroy() to handle dup_msg[] cleanup correctly.
		tr->rsv.n_dupl = 0;
	}

	// 若存在重复写（同一 partition 多副本收到写），先做重复解析；否则直接在 master 上执行写。
	// If there are duplicates to resolve, start doing so.
	if (tr->rsv.n_dupl != 0 && dup_res_start(rw, tr, write_dup_res_start_cb)) {
		return TRANS_IN_PROGRESS; // started duplicate resolution
	}
	// else - no duplicate resolution phase, apply operation to master.

	// 在 master 分区上执行写：索引查找/创建、bin 操作、落盘、副本发送。
	status = write_master(rw, tr);

	// 性能基准测试点记录
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, write, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, batch_sub, FROM_BATCH, write_master);
	BENCHMARK_NEXT_DATA_POINT_FROM(tr, ops_sub, FROM_IOPS, master);

	// 如果写入主副本失败，事务结束
	// If error, transaction is finished.
	if (status != TRANS_IN_PROGRESS) {
		rw_request_hash_delete(&hkey, rw);

		if (status != TRANS_WAITING) {
			send_write_response(tr, NULL);
		}

		return status;
	}

	// 如果不需要副本写入，事务完成
	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(tr);
		send_write_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE_SUCCESS;
	}

	// 如果不需要等待副本写入确认，采用"即发即忘"模式
	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(tr)) {
		start_write_repl_write_forget(rw, tr);
		send_write_response(tr, &rw->response_db);
		rw_request_hash_delete(&hkey, rw);
		return TRANS_DONE_SUCCESS;
	}

	// 启动副本写入，等待确认
	start_write_repl_write(rw, tr);

	// Started replica write.
	return TRANS_IN_PROGRESS;
}


//==========================================================
// Local helpers - transaction flow.
//

/**
 * 副本解析启动回调函数
 *
 * @param rw rw_request对象
 * @param tr 事务对象
 * @param r 记录对象（可能为NULL）
 *
 * 功能说明：
 * 当需要进行副本解析时被调用，负责：
 * 1. 完成rw_request的初始化设置
 * 2. 构造副本解析消息
 * 3. 发送消息到相关副本节点
 * 4. 设置回调函数等待响应
 *
 * 处理流程：
 * 1. 调用 dup_res_make_message() 构造解析消息
 * 2. 获取rw锁保护并发访问
 * 3. 设置副本解析参数和回调函数
 * 4. 发送消息到目标节点
 * 5. 释放锁，等待异步响应
 *
 * 状态机转换：副本解析启动 → 消息发送 → 等待响应 → write_dup_res_cb
 */
static void
write_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r)
{
	// Finish initializing rw, construct and send dup-res message.

	dup_res_make_message(rw, tr, r);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, write_dup_res_cb, write_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_write_repl_write(rw_request* rw, as_transaction* tr)
{
	// Finish initializing rw, construct and send repl-write message.

	repl_write_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_write_setup_rw(rw, tr, write_repl_write_cb, write_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

static void
start_write_repl_write_forget(rw_request* rw, as_transaction* tr)
{
	// Construct and send repl-write message. No need to finish rw setup.

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

/**
 * 副本解析完成回调函数
 *
 * @param rw rw_request对象
 * @return bool 是否完成事务处理（true=完成，false=继续异步处理）
 *
 * 功能说明：
 * 副本解析完成后被调用，负责：
 * 1. 检查解析结果和错误状态
 * 2. 决定下一步处理流程
 * 3. 根据副本策略选择写入或忘记模式
 * 4. 启动主副本写入操作
 *
 * 处理流程：
 * 1. 更新性能基准测试点
 * 2. 从rw_request重建事务对象
 * 3. 检查副本解析是否成功
 * 4. 根据副本写入策略选择处理方式：
 *    - 需要等待副本确认：启动副本写入并等待
 *    - 忘记模式：发送副本写入但不等待确认
 * 5. 执行主副本写入操作
 *
 * 状态机转换：
 * 副本解析完成 → 检查结果 → {
 *   失败 → 发送错误响应 → 结束(true)
 *   成功 → 选择副本策略 → {
 *     需要等待 → 启动副本写入 → 继续异步(false)
 *     忘记模式 → 启动忘记副本写入 → 继续异步(false)
 *     无副本写入 → 主副本写入 → 结束(true)
 *   }
 * }
 */
static bool
write_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, write, FROM_CLIENT, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, ops_sub, FROM_IOPS, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	if (tr.result_code != AS_OK) {
		send_write_response(&tr, NULL);
		return true;
	}

	transaction_status status = write_master(rw, &tr);

	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), write, FROM_CLIENT, master);
	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), batch_sub, FROM_BATCH, write_master);
	BENCHMARK_NEXT_DATA_POINT_FROM((&tr), ops_sub, FROM_IOPS, master);

	if (status == TRANS_WAITING) {
		// Note - new tr now owns msgp, make sure rw destructor doesn't free it.
		// Also, rw will release rsv - new tr will get a new one.
		rw->msgp = NULL;
		return true;
	}

	if (status == TRANS_DONE_ERROR) {
		send_write_response(&tr, NULL);
		return true;
	}

	// If we don't need replica writes, transaction is finished.
	if (rw->n_dest_nodes == 0) {
		finished_replicated(&tr);
		send_write_response(&tr, &rw->response_db);
		return true;
	}

	// If we don't need to wait for replica write acks, fire and forget.
	if (respond_on_master_complete(&tr)) {
		write_repl_write_forget_after_dup_res(rw, &tr);
		send_write_response(&tr, &rw->response_db);
		return true;
	}

	write_repl_write_after_dup_res(rw, &tr);

	// Started replica write - don't delete rw_request from hash.
	return false;
}

static void
write_repl_write_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Recycle rw_request that was just used for duplicate resolution to now do
	// replica writes. Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	repl_write_reset_rw(rw, tr, write_repl_write_cb);
	send_rw_messages(rw);
}

static void
write_repl_write_forget_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// Send replica writes. Not waiting for acks, so need to reset rw_request.
	// Note - we are under the rw_request lock here!

	repl_write_make_message(rw, tr);
	send_rw_messages_forget(rw);
}

/**
 * 副本写入完成回调函数
 *
 * @param rw rw_request对象
 *
 * 功能说明：
 * 当副本写入操作完成时被调用，负责：
 * 1. 检查所有副本的写入结果
 * 2. 决定整个写操作是否成功
 * 3. 根据一致性策略处理部分失败的情况
 * 4. 执行主副本写入操作
 * 5. 发送最终响应给客户端
 *
 * 处理流程：
 * 1. 更新性能基准测试点
 * 2. 从rw_request重建事务对象
 * 3. 检查副本写入状态：
 *    - 统计成功和失败的副本数量
 *    - 根据一致性策略判断是否满足要求
 * 4. 如果副本写入满足要求，执行主副本写入
 * 5. 处理错误情况，发送适当的错误响应
 *
 * 一致性策略处理：
 * - ALL: 所有副本都必须成功
 * - MAJORITY: 大多数副本成功即可
 * - ONE: 至少一个副本成功
 * - 根据失败的副本数量和策略决定写入成功与否
 *
 * 状态机转换：
 * 副本写入完成 → 检查副本结果 → {
 *   满足一致性要求 → 主副本写入 → 发送成功响应
 *   不满足要求 → 发送失败响应
 *   部分成功 → 根据策略决定 → 成功/失败响应
 * }
 */
static void
write_repl_write_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, write, FROM_CLIENT, repl_write);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, repl_write);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, ops_sub, FROM_IOPS, repl_write);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	finished_replicated(&tr);
	send_write_response(&tr, &rw->response_db);

	// Finished transaction - rw_request cleans up reservation and msgp!
}


//==========================================================
// Local helpers - transaction end.
//

static void
send_write_response(as_transaction* tr, cf_dyn_buf* db)
{
	// Paranoia - shouldn't get here on losing race with timeout.
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// Note - if tr was setup from rw, rw->from.any has been set null and
	// informs timeout it lost the race.

	clear_delete_response_metadata(tr);

	switch (tr->origin) {
	case FROM_CLIENT:
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, NULL, NULL, 0, tr->rsv.ns,
					as_transaction_trid(tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, write, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, write_hist);
		client_write_update_stats(tr->rsv.ns, tr->result_code,
				as_transaction_is_xdr(tr));
		break;
	case FROM_PROXY:
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, NULL, NULL,
					0, tr->rsv.ns, as_transaction_trid(tr));
		}
		if (as_transaction_is_batch_sub(tr)) {
			from_proxy_batch_sub_write_update_stats(tr->rsv.ns,
					tr->result_code);
		}
		else {
			from_proxy_write_update_stats(tr->rsv.ns, tr->result_code,
					as_transaction_is_xdr(tr));
		}
		break;
	case FROM_BATCH:
		if (db && db->used_sz != 0) {
			as_batch_add_made_result(tr->from.batch_shared,
					tr->from_data.batch_index, (cl_msg*)db->buf, db->used_sz);
		}
		else {
			as_batch_add_ack(tr);
		}
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, batch_sub_write_hist);
		batch_sub_write_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_IOPS:
		tr->from.iops_orig->done_cb(tr->from.iops_orig->udata, tr->result_code);
		BENCHMARK_NEXT_DATA_POINT(tr, ops_sub, response);
		ops_sub_write_update_stats(tr->rsv.ns, tr->result_code);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // pattern, not needed
}

/**
 * 写操作超时处理回调函数
 *
 * @param rw rw_request对象
 *
 * 功能说明：
 * 当写操作超时时被调用，负责：
 * 1. 检查是否与其他回调函数发生竞争
 * 2. 根据请求来源发送相应的超时错误响应
 * 3. 更新超时相关的统计信息
 * 4. 标记竞争状态以防止重复处理
 *
 * 处理逻辑：
 * 1. 检查 rw->from.any 是否为空（竞争检测）
 * 2. 根据请求来源发送超时响应：
 *    - FROM_CLIENT: 向客户端发送超时回复
 *    - FROM_PROXY: 更新代理相关统计（不发送响应）
 *    - FROM_BATCH: 向批处理添加超时错误
 * 3. 更新命名空间的超时统计计数器
 * 4. 设置 rw->from.any = NULL 通知其他回调竞争失败
 *
 * 竞争处理：
 * - 与副本解析回调的竞争
 * - 与副本写入回调的竞争
 * - 使用原子操作确保只有一个回调处理响应
 *
 * 注意事项：
 * - 超时的操作不包含在性能直方图统计中
 * - 代理请求的超时由代理节点负责处理响应
 * - 批处理请求需要特殊的错误处理格式
 */
static void
write_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // lost race against dup-res or repl-write callback
	}

	finished_not_replicated(rw);

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, rw_request_trid(rw));
		// Timeouts aren't included in histograms.
		client_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
				as_msg_is_xdr(&rw->msgp->msg));
		break;
	case FROM_PROXY:
		if (rw_request_is_batch_sub(rw)) {
			from_proxy_batch_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		else {
			from_proxy_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT,
					as_msg_is_xdr(&rw->msgp->msg));
		}
		break;
	case FROM_BATCH:
		as_batch_add_error(rw->from.batch_shared, rw->from_data.batch_index,
				AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		batch_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_IOPS:
		rw->from.iops_orig->done_cb(rw->from.iops_orig->udata, AS_ERR_TIMEOUT);
		// Timeouts aren't included in histograms.
		ops_sub_write_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // inform other callback it lost the race
}


//==========================================================
// Local helpers - write master.
//

/*
 * 在 master 分区上执行写：预处理 -> 策略与 set 校验 -> 索引查找或创建并加锁 ->
 * 打开/创建 as_storage_rd -> 执行 bin 操作并落盘(write_master_apply) -> 副本/XDR。
 */
/**
 * 执行主副本写入操作
 *
 * @param rw rw_request对象，包含写入请求的上下文信息
 * @param tr 事务对象，包含写入操作的详细信息
 * @return transaction_status 事务状态
 *   - TRANS_DONE_SUCCESS: 写操作成功完成
 *   - TRANS_DONE_ERROR: 写操作失败
 *   - TRANS_WAITING: 需要重新排队等待
 *
 * 功能说明：
 * 这是写操作的核心执行函数，负责在主副本上执行实际的写入操作：
 * 1. 执行预处理检查（不需要循环操作或创建/查找索引的检查）
 * 2. 应用写入策略和验证规则
 * 3. 查找或创建记录索引
 * 4. 执行bin操作（写入、更新、删除等）
 * 5. 处理存储引擎交互
 * 6. 管理记录生命周期（创建、更新、删除）
 * 7. 触发后续的副本同步和索引更新
 *
 * 处理流程：
 * 1. 预处理阶段 - write_master_preprocessing():
 *    - 检查时钟偏斜限制
 *    - 验证操作数量和消息格式
 *
 * 2. 策略应用阶段 - write_master_policies():
 *    - 解析写入策略（must_not_create, replace等）
 *    - 验证条件写入要求
 *
 * 3. 索引操作阶段：
 *    - 在主索引中查找或创建记录
 *    - 获取记录锁以确保原子性
 *    - 检查记录状态（过期、删除、副本状态等）
 *
 * 4. 存储操作阶段：
 *    - 打开存储记录描述符
 *    - 加载现有bin数据
 *    - 准备写入环境
 *
 * 5. 应用操作阶段 - write_master_apply():
 *    - 验证条件写入要求（generation、TTL等）
 *    - 执行所有bin操作
 *    - 更新记录元数据
 *
 * 6. 完成阶段：
 *    - 提交存储更改
 *    - 更新索引和统计信息
 *    - 触发XDR和二级索引更新
 *    - 启动副本同步或发送响应
 *
 * 错误处理：
 * - 任何阶段的错误都会调用 write_master_failed()
 * - 确保正确释放锁和资源
 * - 向客户端发送适当的错误响应
 *
 * 状态机转换：
 * MASTER_START → 预处理 → 策略检查 → 索引查找 → 存储操作 → bin操作执行 → {
 *   成功 → 副本同步启动 → 返回成功
 *   失败 → 错误清理 → 返回错误
 *   需要重排队 → 返回等待状态
 * }
 */
static transaction_status
write_master(rw_request* rw, as_transaction* tr)
{
	//------------------------------------------------------
	// Perform checks that don't need to loop over ops, or
	// create or find (and lock) the as_index.
	//

	if (! write_master_preprocessing(tr)) {
		// Failure cases all call write_master_failed().
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Loop over ops to set some essential policy flags.
	//

	bool must_not_create;
	bool is_replace;

	int result = write_master_policies(tr, &must_not_create, &is_replace);

	if (result != 0) {
		write_master_failed(tr, 0, false, 0, 0, result);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Find or create the as_index and get a reference -
	// this locks the record. Perform all checks that don't
	// need the as_storage_rd.
	//

	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_index_tree* tree = tr->rsv.tree;

	// Find or create as_index, populate as_index_ref, lock record.
	as_index_ref r_ref;
	as_record* r = NULL;
	bool record_created = false;

	if (must_not_create) {
		if (as_record_get(tree, &tr->keyd, &r_ref) != 0) {
			write_master_failed(tr, 0, record_created, tree, 0, AS_ERR_NOT_FOUND);
			return TRANS_DONE_ERROR;
		}

		r = r_ref.r;

		if (as_record_is_doomed(r, ns)) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_NOT_FOUND);
			return TRANS_DONE_ERROR;
		}

		if (repl_state_check(r, tr) < 0) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		if (! as_record_is_live(r)) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_NOT_FOUND);
			return TRANS_DONE_ERROR;
		}
	}
	else {
		int rv = as_record_get_create(tree, &tr->keyd, &r_ref, ns);

		if (rv < 0) {
			cf_detail(AS_RW, "{%s} write_master: fail as_record_get_create() %pD", ns->name, &tr->keyd);
			write_master_failed(tr, 0, record_created, tree, 0, AS_ERR_UNKNOWN);
			return TRANS_DONE_ERROR;
		}

		r = r_ref.r;
		record_created = rv == 1;

		bool is_doomed = as_record_is_doomed(r, ns);

		if (! record_created && ! is_doomed && repl_state_check(r, tr) < 0) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		// If it's an expired or truncated record, pretend it's a fresh create.
		if (! record_created && is_doomed) {
			as_set_index_delete_live(ns, tree, r, r_ref.r_h);
			as_record_rescue(&r_ref, ns);
			record_created = true;
		}
	}

	// Enforce record-level create-only existence policy.
	if ((m->info2 & AS_MSG_INFO2_CREATE_ONLY) != 0 &&
			! record_created && as_record_is_live(r)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_RECORD_EXISTS);
		return TRANS_DONE_ERROR;
	}

	// Check generation requirement, if any.
	if (! generation_check(r, m, ns)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_GENERATION);
		return TRANS_DONE_ERROR;
	}

	// If creating record, write set-ID into index.
	if (record_created) {
		int rv_set = as_transaction_has_set(tr) ?
				set_set_from_msg(r, ns, m) : 0;

		if (rv_set == -1) {
			cf_warning(AS_RW, "{%s} write_master: set can't be added %pD", ns->name, &tr->keyd);
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_PARAMETER);
			return TRANS_DONE_ERROR;
		}
		else if (rv_set == -2) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_FORBIDDEN);
			return TRANS_DONE_ERROR;
		}

		// Don't write record if it would be truncated.
		if (as_truncate_now_is_truncated(ns, as_index_get_set_id(r))) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_FORBIDDEN);
			return TRANS_DONE_ERROR;
		}
	}

	as_set* p_set = as_namespace_get_record_set(ns, r);

	// Enforce set size limit, if any.
	if (as_set_size_stop_writes(p_set)) {
		cf_ticker_warning(AS_RW, "{%s|%s} at stop-writes-size - can't write", ns->name, p_set->name);
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_FORBIDDEN);
		return TRANS_DONE_ERROR;
	}

	// Shortcut set name.
	const char* set_name = p_set == NULL ? NULL : p_set->name;

	// If record existed, check that as_msg set name matches.
	if (! record_created && tr->origin != FROM_IOPS &&
			! check_msg_set_name(tr, set_name)) {
		write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_PARAMETER);
		return TRANS_DONE_ERROR;
	}

	if (! record_created) {
		as_xdr_ship_status ship_status = as_xdr_ship_check(r, tr);

		if (ship_status == XDR_SHIP_NEAR) {
			as_record_done(&r_ref, ns);
			return TRANS_WAITING;
		}

		if (ship_status == XDR_SHIP_FAR) {
			write_master_failed(tr, &r_ref, record_created, tree, 0, AS_ERR_XDR_KEY_BUSY);
			return TRANS_DONE_ERROR;
		}
	}

	as_exp* filter_exp = NULL;

	// Handle metadata filter if present.
	if (! record_created && as_record_is_live(r) &&
			(result = handle_meta_filter(tr, r, &filter_exp)) != 0) {
		write_master_failed(tr, &r_ref, false, tree, 0, result);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Open or create the as_storage_rd, and handle record
	// metadata.
	//

	as_storage_rd rd;

	if (record_created) {
		as_storage_record_create(ns, r, &rd);
	}
	else {
		as_storage_record_open(ns, r, &rd);
	}

	// Apply record bins filter if present.
	if (filter_exp != NULL) {
		if ((result = read_and_filter_bins(&rd, filter_exp)) != 0) {
			destroy_filter_exp(tr, filter_exp);
			write_master_failed(tr, &r_ref, false, tree, &rd, result);
			return TRANS_DONE_ERROR;
		}

		destroy_filter_exp(tr, filter_exp);
	}

	// Check background si-queries for false positives.
	if (tr->origin == FROM_IOPS) {
		iops_origin* origin = tr->from.iops_orig;

		if (origin->check_cb != NULL &&
				! origin->check_cb(origin->udata, &rd)) {
			write_master_failed(tr, &r_ref, record_created, tree, &rd, AS_ERR_NOT_FOUND);
			return TRANS_DONE_ERROR;
		}
	}

	// Shortcut for set name storage.
	if (set_name) {
		rd.set_name = set_name;
		rd.set_name_len = strlen(set_name);
	}

	// Deal with key storage as needed.
	if ((result = handle_msg_key(tr, &rd)) != 0) {
		write_master_failed(tr, &r_ref, record_created, tree, &rd, result);
		return TRANS_DONE_ERROR;
	}

	// Convert message TTL special value if appropriate.
	if (m->record_ttl == TTL_DONT_UPDATE &&
			(record_created || ! as_record_is_live(r))) {
		m->record_ttl = TTL_USE_DEFAULT;
	}

	if (! is_valid_ttl(m->record_ttl)) {
		cf_warning(AS_RW, "write_master: invalid ttl %u", m->record_ttl);
		write_master_failed(tr, &r_ref, record_created, tree, &rd, AS_ERR_PARAMETER);
		return false;
	}

	if (is_ttl_disallowed(m->record_ttl, ns, p_set)) {
		cf_ticker_warning(AS_RW, "write_master: disallowed ttl with nsup-period 0");
		write_master_failed(tr, &r_ref, record_created, tree, &rd, AS_ERR_FORBIDDEN);
		return false;
	}

	// Set up the nodes to which we'll write replicas.
	if (! set_replica_destinations(tr, rw)) {
		write_master_failed(tr, &r_ref, record_created, tree, &rd, AS_ERR_UNAVAILABLE);
		return TRANS_DONE_ERROR;
	}

	// Fire and forget can overload the fabric send queues - check.
	if (respond_on_master_complete(tr) &&
			as_fabric_is_overloaded(rw->dest_nodes, rw->n_dest_nodes,
					AS_FABRIC_CHANNEL_RW, 0)) {
		tr->flags |= AS_TRANSACTION_FLAG_SWITCH_TO_COMMIT_ALL;
	}

	// Will we need a pickle?
	rd.keep_pickle = rw->n_dest_nodes != 0;

	// Save for XDR submit.
	uint64_t prev_lut = r->last_update_time;

	//------------------------------------------------------
	// 执行 bin 操作并提交到存储：加载旧 bins、应用消息中的 op、写入设备、更新 sindex。
	// Handle bin operations and commit master.
	//

	bool is_delete = false;

	result = write_master_apply(tr, &r_ref, &rd, is_replace, rw, &is_delete);

	if (result != 0) {
		write_master_failed(tr, &r_ref, record_created, tree, &rd, result);
		return TRANS_DONE_ERROR;
	}

	//------------------------------------------------------
	// Done - complete function's output, release the record
	// lock, and do XDR write if appropriate.
	//

	tr->generation = r->generation;
	tr->void_time = r->void_time;
	tr->last_update_time = r->last_update_time;

	// Handle deletion if appropriate.
	if (is_delete) {
		write_delete_record(r_ref.r, tree);
		as_incr_uint64(&ns->n_deleted_last_bin);
		tr->flags |= AS_TRANSACTION_FLAG_IS_DELETE;
	}
	// Or (normally) adjust max void-time.
	else if (r->void_time != 0) {
		as_setmax_uint32(&tr->rsv.p->max_void_time, r->void_time);
	}

	will_replicate(r, ns);

	// Save for XDR submit outside record lock.
	as_xdr_submit_info submit_info;

	as_xdr_get_submit_info(r, prev_lut, &submit_info);

	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	if (! write_is_full_drop(tr)) {
		as_xdr_submit(ns, &submit_info);
	}

	return TRANS_IN_PROGRESS;
}

static void
write_master_failed(as_transaction* tr, as_index_ref* r_ref,
		bool record_created, as_index_tree* tree, as_storage_rd* rd,
		int result_code)
{
	as_namespace* ns = tr->rsv.ns;

	if (r_ref) {
		if (rd) {
			as_storage_record_close(rd);
		}

		if (record_created) {
			as_index_delete(tree, &tr->keyd);
		}

		as_record_done(r_ref, ns);
	}

	switch (result_code) {
	case AS_ERR_GENERATION:
		as_incr_uint64(&ns->n_fail_generation);
		break;
	case AS_ERR_RECORD_TOO_BIG:
		cf_detail(AS_RW, "{%s} write_master: record too big %pD", ns->name, &tr->keyd);
		as_incr_uint64(&ns->n_fail_record_too_big);
		break;
	default:
		// These either log warnings or aren't interesting enough to count.
		break;
	}

	tr->result_code = (uint8_t)result_code;
}

static int
write_master_preprocessing(as_transaction* tr)
{
	as_namespace* ns = tr->rsv.ns;
	as_msg* m = &tr->msgp->msg;

	if (ns->clock_skew_stop_writes) {
		// TODO - new error code?
		write_master_failed(tr, 0, false, 0, 0, AS_ERR_FORBIDDEN);
		return false;
	}

	// ns->stop_writes is set by nsup if configured threshold is breached.
	if (ns->stop_writes) {
		write_master_failed(tr, 0, false, 0, 0, AS_ERR_OUT_OF_SPACE);
		return false;
	}

	// Fail if disallow_null_setname is true and set name is absent or empty.
	if (ns->disallow_null_setname) {
		as_msg_field* f = as_transaction_has_set(tr) ?
				as_msg_field_get(m, AS_MSG_FIELD_TYPE_SET) : NULL;

		if (! f || as_msg_field_get_value_sz(f) == 0) {
			cf_warning(AS_RW, "write_master: null/empty set name not allowed for namespace %s", ns->name);
			write_master_failed(tr, 0, false, 0, 0, AS_ERR_PARAMETER);
			return false;
		}
	}

	return true;
}

static int
write_master_policies(as_transaction* tr, bool* p_must_not_create,
		bool* p_is_replace)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	if (m->n_ops == 0) {
		cf_warning(AS_RW, "{%s} write_master: bin op(s) expected, none present %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (m->n_ops > MAX_N_OPS) {
		cf_warning(AS_RW, "{%s} write_master: can't exceed %u bin ops %pD", ns->name, MAX_N_OPS, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	bool info1_get_all = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	bool must_not_create =
			(m->info3 & AS_MSG_INFO3_UPDATE_ONLY) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	bool is_replace =
			(m->info3 & AS_MSG_INFO3_CREATE_OR_REPLACE) != 0 ||
			(m->info3 & AS_MSG_INFO3_REPLACE_ONLY) != 0;

	if (is_replace && forbid_replace(ns)) {
		cf_warning(AS_RW, "{%s} write_master: can't replace record %pD if conflict resolving", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	bool has_read_op = false;
	bool has_read_all_op = false;
	bool generates_response_bin = false;

	// Loop over ops to check and modify flags.
	as_msg_op* op = NULL;
	uint16_t i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (op->op == AS_MSG_OP_TOUCH) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: touch op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			must_not_create = true;
			continue;
		}

		if (! as_bin_name_check(op->name, op->name_sz)) {
			cf_warning(AS_RW, "{%s} write_master: bad bin name %.*s (%u) %pD", ns->name, op->name_sz, op->name, op->name_sz, &tr->keyd);
			return AS_ERR_BIN_NAME;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			if (op->particle_type == AS_PARTICLE_TYPE_NULL &&
					is_replace) {
				cf_warning(AS_RW, "{%s} write_master: bin delete can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (OP_IS_MODIFY(op->op)) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (op->op == AS_MSG_OP_DELETE_ALL) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: delete-all op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			// Could forbid multiple delete-alls and delete-all being first op
			// (should use replace), but these are nonsensical, not unworkable.
		}
		else if (op_is_read_all(op, m)) {
			if (respond_all_ops) {
				cf_warning(AS_RW, "{%s} write_master: read-all op can't have respond-all-ops flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			if (has_read_all_op) {
				cf_warning(AS_RW, "{%s} write_master: can't have more than one read-all op %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			has_read_op = true;
			has_read_all_op = true;
		}
		else if (op->op == AS_MSG_OP_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_BITS_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: bits modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}
		}
		else if (op->op == AS_MSG_OP_BITS_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_HLL_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: hll modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			generates_response_bin = true; // HLL modify may generate a response bin
		}
		else if (op->op == AS_MSG_OP_HLL_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			if (is_replace) {
				cf_warning(AS_RW, "{%s} write_master: cdt modify op can't have record-level replace flag %pD", ns->name, &tr->keyd);
				return AS_ERR_PARAMETER;
			}

			generates_response_bin = true; // CDT modify may generate a response bin
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
		else if (op->op == AS_MSG_OP_EXP_READ) {
			has_read_op = true;
			generates_response_bin = true;
		}
	}

	if (has_read_op && (m->info1 & AS_MSG_INFO1_READ) == 0) {
		cf_warning(AS_RW, "{%s} write_master: has read op but read flag not set %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (has_read_all_op && generates_response_bin) {
		cf_warning(AS_RW, "{%s} write_master: read-all op can't mix with ops that generate response bins %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	if (info1_get_all && ! has_read_all_op) {
		cf_warning(AS_RW, "{%s} write_master: get-all flag set with no read-all op %pD", ns->name, &tr->keyd);
		return AS_ERR_PARAMETER;
	}

	*p_must_not_create = must_not_create;
	*p_is_replace = is_replace;

	return 0;
}

static bool
check_msg_set_name(as_transaction* tr, const char* set_name)
{
	as_msg_field* f = as_transaction_has_set(tr) ?
			as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET) : NULL;

	if (! f || as_msg_field_get_value_sz(f) == 0) {
		if (set_name) {
			cf_warning(AS_RW, "overwriting record in set '%s' but msg has no set name %pD",
					set_name, &tr->keyd);
		}

		return true;
	}

	uint32_t msg_set_name_len = as_msg_field_get_value_sz(f);

	if (! set_name ||
			strncmp(set_name, (const char*)f->data, msg_set_name_len) != 0 ||
			set_name[msg_set_name_len] != 0) {
		cf_warning(AS_RW, "overwriting record in set '%s' but msg has different set name '%.*s' (%u) %pD",
				set_name ? set_name : "(null)", msg_set_name_len,
						(const char*)f->data, msg_set_name_len, &tr->keyd);
		return false;
	}

	return true;
}

/*
 * 将本次写操作应用到主副本：从设备加载 bins -> 准备元数据 -> 执行 bin ops -> 判删 ->
 * 调用 as_storage_record_write 落盘 -> 更新 sindex/索引。
 * 数据实际写入由 as_storage_record_write(rd) 根据 ns->storage_type 分发到 mem/ssd/pmem。
 */
static int
write_master_apply(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		bool is_replace, rw_request* rw, bool* is_delete)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;
	bool set_has_si = set_has_sindex(r, ns);
	bool si_needs_bins = set_has_si && r->in_sindex == 1;

	// For sindex, we must read existing record even if replacing.
	rd->ignore_record_on_device = is_replace && ! si_needs_bins;

	as_bin stack_bins[RECORD_MAX_BINS + m->n_ops];

	// 从存储引擎加载当前记录的 bins（mem 从 mwb 读、ssd 从块读等）。
	int result = as_storage_rd_load_bins(rd, stack_bins);

	if (result < 0) {
		cf_warning(AS_RW, "{%s} write_master: failed as_storage_rd_load_bins() %pD", ns->name, &tr->keyd);
		return -result;
	}

	uint32_t n_old_bins = (uint32_t)rd->n_bins;
	as_bin old_bins[n_old_bins];

	//------------------------------------------------------
	// Copy old bins (if any) - which are currently in new
	// bins array - to old bins array, for sindex purposes.
	//

	uint32_t n_old_bins_saved = 0;

	if (si_needs_bins && n_old_bins != 0) {
		memcpy(old_bins, rd->bins, n_old_bins * sizeof(as_bin));
		n_old_bins_saved = n_old_bins;

		// If it's a replace, clear the new bins array.
		if (is_replace) {
			rd->n_bins = 0;
		}
	}

	//------------------------------------------------------
	// Apply changes to metadata in as_index needed for
	// response, pickling, and writing.
	//

	prepare_bin_metadata(tr, rd);

	index_metadata old_metadata;

	stash_index_metadata(r, &old_metadata);
	advance_record_version(tr, r);
	set_xdr_write(tr, r);

	//------------------------------------------------------
	// Loop over bin ops to affect new bin space, creating
	// the new record bins to write.
	//

	cf_ll_buf_define(particles_llb, STACK_PARTICLES_SIZE);

	if ((result = write_master_bin_ops(tr, rd, &particles_llb,
			&rw->response_db)) != 0) {
		cf_ll_buf_free(&particles_llb);
		unwind_index_metadata(&old_metadata, r);
		return result;
	}

	//------------------------------------------------------
	// Created the new bins to write.
	//

	*is_delete = as_bin_empty_if_all_tombstones(rd,
			as_transaction_is_durable_delete(tr));

	if (*is_delete) {
		if (! as_transaction_is_xdr(tr) &&
				(n_old_bins == 0 || ! as_record_is_live(r))) {
			// Didn't exist or was bin cemetery (tombstone bit not yet updated).
			cf_ll_buf_free(&particles_llb);
			unwind_index_metadata(&old_metadata, r);
			return AS_ERR_NOT_FOUND;
		}

		if ((result = validate_delete_durability(tr)) != AS_OK) {
			cf_ll_buf_free(&particles_llb);
			unwind_index_metadata(&old_metadata, r);
			return result;
		}
	}

	transition_delete_metadata(tr, r, *is_delete,
			*is_delete && rd->n_bins != 0);

	//------------------------------------------------------
	// 将记录写入存储：根据 namespace 的 storage_type 调用 mem/ssd/pmem 的写实现；
	// 内部会把 rd 中的 bins 打包成 as_flat_record 写入写块缓冲区（或 SSD 写块）。
	// Write the record to storage.
	//

	if ((result = as_storage_record_write(rd)) < 0) {
		cf_detail(AS_RW, "{%s} write_master: failed as_storage_record_write() %pD", ns->name, &tr->keyd);
		cf_ll_buf_free(&particles_llb);
		unwind_index_metadata(&old_metadata, r);
		return -result;
	}

	as_record_transition_stats(r, ns, &old_metadata);
	as_record_transition_set_index(tr->rsv.tree, r_ref, ns, rd->n_bins,
			&old_metadata);
	pickle_all(rd, rw);

	//------------------------------------------------------
	// Success - adjust sindex, looking at old and new bins.
	//

	if (set_has_si) {
		update_sindex(ns, r_ref, old_bins, n_old_bins_saved, rd->bins,
				rd->n_bins);
	}
	else {
		// Sindex drop will leave in_sindex bit. Good opportunity to clear.
		as_index_clear_in_sindex(r);
	}

	//------------------------------------------------------
	// Final changes to record data in as_index.
	//

	// Accommodate a new stored key - wasn't needed for pickling and writing.
	if (r->key_stored == 0 && rd->key) {
		r->key_stored = 1;
	}

	cf_ll_buf_free(&particles_llb);

	return 0;
}

/**
 * 执行写入操作的bin级别操作
 *
 * @param tr 事务对象
 * @param rd 存储记录描述符
 * @param particles_llb 粒子链式缓冲区，用于临时存储
 * @param db 动态缓冲区，用于构造响应
 * @return int 操作结果码（0=成功，负值=错误码）
 *
 * 功能说明：
 * 这是bin操作的核心执行引擎，负责：
 * 1. 解析和验证消息中的所有操作
 * 2. 为每个操作分配响应和结果bin
 * 3. 调用操作循环处理每个bin操作
 * 4. 构造操作响应消息
 * 5. 处理批处理和非批处理请求的不同响应格式
 *
 * 支持的操作类型：
 * - AS_MSG_OP_WRITE: 普通写入/更新操作
 * - AS_MSG_OP_INCR: 原子增减操作
 * - AS_MSG_OP_APPEND/PREPEND: 字符串追加操作
 * - AS_MSG_OP_BITS_*: 位操作
 * - AS_MSG_OP_HLL_*: HyperLogLog操作
 * - AS_MSG_OP_CDT_*: 复杂数据类型操作（List/Map）
 * - AS_MSG_OP_EXP_*: 表达式操作
 * - AS_MSG_OP_DELETE: 删除操作
 * - AS_MSG_OP_TOUCH: 触摸操作（更新TTL）
 *
 * 处理流程：
 * 1. 验证操作数量和消息格式
 * 2. 分配操作、响应和结果bin数组
 * 3. 调用 write_master_bin_ops_loop() 执行所有操作
 * 4. 根据请求来源构造不同格式的响应：
 *    - 批处理请求：直接添加到批处理结果
 *    - 普通请求：构造完整的响应消息
 * 5. 清理临时分配的资源
 *
 * 内存管理：
 * - 使用栈分配的数组优化小规模操作
 * - 大量操作时动态分配内存
 * - 使用链式缓冲区管理临时粒子数据
 * - 确保所有分配的资源都能正确释放
 *
 * 错误处理：
 * - 操作失败时返回相应的错误码
 * - 确保部分成功的操作能够正确回滚
 * - 提供详细的错误信息用于调试
 */
static int
write_master_bin_ops(as_transaction* tr, as_storage_rd* rd,
		cf_ll_buf* particles_llb, cf_dyn_buf* db)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	as_record* r = rd->r;
	bool has_read_all_op = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0;

	as_msg_op* ops[m->n_ops];
	as_bin response_bins[has_read_all_op ? RECORD_MAX_BINS : m->n_ops];
	as_bin result_bins[m->n_ops];

	uint32_t n_response_bins = 0;
	uint32_t n_result_bins = 0;

	int result = write_master_bin_ops_loop(tr, rd, ops, response_bins,
			&n_response_bins, result_bins, &n_result_bins, particles_llb);

	if (result != 0) {
		as_bin_destroy_all(result_bins, n_result_bins);
		return result;
	}

	if (rd->n_bins > RECORD_MAX_BINS) {
		as_bin_destroy_all(result_bins, n_result_bins);
		return AS_ERR_PARAMETER;
	}

	if (n_response_bins == 0) {
		// If 'ordered-ops' flag was not set, and there were no read ops or CDT
		// ops with results, there's no response to build and send later.
		return 0;
	}

	as_bin* bins[n_response_bins];

	for (uint32_t i = 0; i < n_response_bins; i++) {
		as_bin* b = &response_bins[i];

		bins[i] = as_bin_is_used(b) ? b : NULL;
	}

	uint32_t generation = r->generation;
	uint32_t void_time = r->void_time;

	// Deletes don't return metadata.
	if (rd->n_bins == 0) {
		generation = 0;
		void_time = 0;
	}

	size_t msg_sz = 0;
	uint8_t* msgp = (uint8_t*)as_msg_make_response_msg(AS_OK, generation,
			void_time, has_read_all_op ? NULL : ops, bins,
			(uint16_t)n_response_bins, ns, NULL, &msg_sz,
			as_transaction_trid(tr));

	as_bin_destroy_all(result_bins, n_result_bins);

	// Stash the message, to be sent later.
	db->buf = msgp;
	db->is_stack = false;
	db->alloc_sz = msg_sz;
	db->used_sz = msg_sz;

	return 0;
}

static int
write_master_bin_ops_loop(as_transaction* tr, as_storage_rd* rd,
		as_msg_op** ops, as_bin* response_bins, uint32_t* p_n_response_bins,
		as_bin* result_bins, uint32_t* p_n_result_bins,
		cf_ll_buf* particles_llb)
{
	// Shortcut pointers.
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;
	bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

	uint64_t msg_lut = as_transaction_xdr_lut(tr);

	if (forbid_resolve(tr, rd, msg_lut)) {
		return AS_ERR_FORBIDDEN;
	}

	uint16_t n_won = m->n_ops;

	int result;

	as_msg_op* op = NULL;
	uint16_t i = 0;

	while ((op = as_msg_op_iterate(m, op, &i)) != NULL) {
		if (! resolve_bin(rd, op, msg_lut, m->n_ops, &n_won, &result)) {
			if (result != AS_OK) {
				return result;
			}

			continue;
		}

		if (op->op == AS_MSG_OP_TOUCH) {
			touch_bin_metadata(rd);
			continue;
		}

		if (op->op == AS_MSG_OP_WRITE) {
			// AS_PARTICLE_TYPE_NULL means delete the bin.
			if (op->particle_type == AS_PARTICLE_TYPE_NULL) {
				delete_bin(rd, op, msg_lut);
			}
			// It's a regular bin write.
			else {
				as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

				write_resolved_bin(rd, op, msg_lut, b);

				if ((result = as_bin_particle_from_client(b, particles_llb, op)) < 0) {
					cf_warning(AS_RW, "{%s} write_master: failed as_bin_particle_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		// Modify an existing bin value.
		else if (OP_IS_MODIFY(op->op)) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			if ((result = as_bin_particle_modify_from_client(b, particles_llb, op)) < 0) {
				cf_warning(AS_RW, "{%s} write_master: failed as_bin_particle_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_DELETE_ALL) {
			delete_all_bins(rd);

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op_is_read_all(op, m)) {
			for (uint16_t i = 0; i < rd->n_bins; i++) {
				as_bin* b = &rd->bins[i];

				if (! as_bin_is_tombstone(b)) {
					// ops array will not be not used in this case.
					response_bins[(*p_n_response_bins)++] = *b;
				}
			}
		}
		else if (op->op == AS_MSG_OP_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = *b;
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_BITS_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			if ((result = as_bin_bits_modify_from_client(b, particles_llb, op)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_bits_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_BITS_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_bits_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_bits_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_HLL_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_hll_modify_from_client(b, particles_llb, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_hll_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops || as_bin_is_used(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_HLL_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_hll_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_hll_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_CDT_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_cdt_modify_from_client(b, particles_llb, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_cdt_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops || as_bin_is_used(&result_bin)) {
				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}

			// The op will not empty a bin, but may leave a freshly created bin
			// empty. In this case it must be last in rd->bins - remove it.
			if (as_bin_is_unused(b)) {
				rd->n_bins--;
			}
		}
		else if (op->op == AS_MSG_OP_CDT_READ) {
			as_bin* b = as_bin_get_live_w_len(rd, op->name, op->name_sz);

			if (b) {
				as_bin result_bin;
				as_bin_set_empty(&result_bin);

				if ((result = as_bin_cdt_read_from_client(b, op, &result_bin)) < 0) {
					cf_detail(AS_RW, "{%s} write_master: failed as_bin_cdt_read_from_client() %pD", ns->name, &tr->keyd);
					return -result;
				}

				ops[*p_n_response_bins] = op;
				response_bins[(*p_n_response_bins)++] = result_bin;
				append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
			}
			else if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}
		}
		else if (op->op == AS_MSG_OP_EXP_MODIFY) {
			as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

			bool created_bin = as_bin_is_unused(b);
			const as_exp_ctx exp_ctx = { .ns = ns, .rd = rd, .r = rd->r };
			const iops_expop* expop = tr->origin == FROM_IOPS ?
					&tr->from.iops_orig->expops[i] : NULL;

			if ((result = as_bin_exp_modify_from_client(&exp_ctx, b, particles_llb, op, expop)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_exp_modify_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			if (respond_all_ops) {
				ops[*p_n_response_bins] = op;
				as_bin_set_empty(&response_bins[(*p_n_response_bins)++]);
			}

			if (as_bin_is_unused(b)) {
				if (created_bin) {
					rd->n_bins--;
				}
				else {
					delete_bin(rd, op, msg_lut);
				}
			}
		}
		else if (op->op == AS_MSG_OP_EXP_READ) {
			const as_exp_ctx exp_ctx = { .ns = ns, .rd = rd, .r = rd->r };

			as_bin result_bin;
			as_bin_set_empty(&result_bin);

			if ((result = as_bin_exp_read_from_client(&exp_ctx, op, &result_bin)) < 0) {
				cf_detail(AS_RW, "{%s} write_master: failed as_bin_exp_read_from_client() %pD", ns->name, &tr->keyd);
				return -result;
			}

			ops[*p_n_response_bins] = op;
			response_bins[(*p_n_response_bins)++] = result_bin;
			append_bin_to_destroy(&result_bin, result_bins, p_n_result_bins);
		}
		else {
			cf_warning(AS_RW, "{%s} write_master: unknown bin op %u %pD", ns->name, op->op, &tr->keyd);
			return AS_ERR_PARAMETER;
		}
	}

	return 0;
}
