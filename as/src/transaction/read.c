/*
 * read.c - Aerospike 读事务处理模块
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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
 *
 * =============================================================================
 * 模块功能说明：
 *
 * 本模块负责处理 Aerospike 数据库的读操作事务。主要功能包括：
 * 1. 协调不同一致性级别的读操作
 * 2. 处理副本解析（duplicate resolution）
 * 3. 管理副本ping操作
 * 4. 执行本地数据读取
 * 5. 处理各种读操作类型（普通读、位操作读、HLL读、CDT读、表达式读）
 *
 * 关键数据流：
 * 客户端请求 → as_read_start() → [副本解析/ping检查] → read_local() → 响应发送
 *
 * 状态机转换图：
 * START → 检查一致性要求 → {
 *   无需网络跳转 → read_local() → DONE
 *   需要副本解析 → dup_res → [ping检查] → read_local() → DONE
 *   需要ping → repl_ping → read_local() → DONE
 *   超时 → TIMEOUT
 * }
 *
 * 主要状态：
 * - TRANS_IN_PROGRESS: 事务正在进行中
 * - TRANS_WAITING: 等待其他操作完成
 * - TRANS_DONE_SUCCESS: 成功完成
 * - TRANS_DONE_ERROR: 发生错误
 * =============================================================================
 */

//==========================================================
// Includes.
//

#include "transaction/read.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/exp.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "fabric/partition.h"
#include "storage/storage.h"
#include "transaction/duplicate_resolve.h"
#include "transaction/proxy.h"
#include "transaction/read_touch.h"
#include "transaction/replica_ping.h"
#include "transaction/rw_request.h"
#include "transaction/rw_request_hash.h"
#include "transaction/rw_utils.h"


//==========================================================
// Forward declarations.
//

static void read_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r);
static void start_repl_ping(rw_request* rw, as_transaction* tr);
static bool read_dup_res_cb(rw_request* rw);
static void repl_ping_after_dup_res(rw_request* rw, as_transaction* tr);
static void repl_ping_cb(rw_request* rw);

static void send_read_response(as_transaction* tr, as_msg_op** ops, as_bin** response_bins, uint16_t n_bins, cf_dyn_buf* db);
static void read_timeout_cb(rw_request* rw);

static transaction_status read_local(as_transaction* tr);
static void read_local_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd, int result_code);


//==========================================================
// 内联函数和宏定义
//

/**
 * 检查读操作是否必须进行副本解析
 *
 * @param tr 事务对象
 * @return true 如果存在副本且一致性级别要求解析所有副本
 *
 * 说明：当一致性级别为 ALL 且存在重复副本时，需要进行副本解析
 * 以确保读取到最新的数据版本
 */
static inline bool
read_must_duplicate_resolve(const as_transaction* tr)
{
	return tr->rsv.n_dupl != 0 &&
			TR_READ_CONSISTENCY_LEVEL(tr) == AS_READ_CONSISTENCY_LEVEL_ALL;
}

/**
 * 检查读操作是否必须进行副本ping
 *
 * @param tr 事务对象
 * @return true 如果事务标志要求进行ping操作
 *
 * 说明：某些情况下（如强一致性读取）需要ping副本节点
 * 确保它们处于活跃状态
 */
static inline bool
read_must_ping(const as_transaction *tr)
{
	return (tr->flags & AS_TRANSACTION_FLAG_MUST_PING) != 0;
}

/**
 * 更新客户端读操作统计信息
 *
 * @param ns 命名空间
 * @param result_code 结果码
 *
 * 说明：根据操作结果更新相应的统计计数器
 * 用于监控和性能分析
 */
static inline void
client_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_client_read_success);
		break;
	default:
		as_incr_uint64(&ns->n_client_read_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_client_read_timeout);
		break;
	case AS_ERR_NOT_FOUND:
		as_incr_uint64(&ns->n_client_read_not_found);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_client_read_filtered_out);
		break;
	}
}

/**
 * 更新代理节点读操作统计信息
 *
 * @param ns 命名空间
 * @param result_code 结果码
 *
 * 说明：当读请求来自代理节点时，更新相应的统计信息
 */
static inline void
from_proxy_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_from_proxy_read_success);
		break;
	default:
		as_incr_uint64(&ns->n_from_proxy_read_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_from_proxy_read_timeout);
		break;
	case AS_ERR_NOT_FOUND:
		as_incr_uint64(&ns->n_from_proxy_read_not_found);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_from_proxy_read_filtered_out);
		break;
	}
}

/**
 * 更新批处理子读操作统计信息
 *
 * @param ns 命名空间
 * @param result_code 结果码
 *
 * 说明：批处理操作中每个子读操作的统计信息更新
 */
static inline void
batch_sub_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_batch_sub_read_success);
		break;
	default:
		as_incr_uint64(&ns->n_batch_sub_read_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_batch_sub_read_timeout);
		break;
	case AS_ERR_NOT_FOUND:
		as_incr_uint64(&ns->n_batch_sub_read_not_found);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_batch_sub_read_filtered_out);
		break;
	}
}

/**
 * 更新来自代理的批处理子读操作统计信息
 *
 * @param ns 命名空间
 * @param result_code 结果码
 *
 * 说明：当批处理读请求来自代理节点时的统计信息更新
 */
static inline void
from_proxy_batch_sub_read_update_stats(as_namespace* ns, uint8_t result_code)
{
	switch (result_code) {
	case AS_OK:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_read_success);
		break;
	default:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_read_error);
		break;
	case AS_ERR_TIMEOUT:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_read_timeout);
		break;
	case AS_ERR_NOT_FOUND:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_read_not_found);
		break;
	case AS_ERR_FILTERED_OUT:
		as_incr_uint64(&ns->n_from_proxy_batch_sub_read_filtered_out);
		break;
	}
}


//==========================================================
// 公共API接口
//

/**
 * 启动读事务处理流程
 *
 * @param tr 事务对象指针
 * @return transaction_status 事务状态
 *   - TRANS_IN_PROGRESS: 事务正在进行中（异步处理）
 *   - TRANS_DONE_SUCCESS: 事务成功完成
 *   - TRANS_DONE_ERROR: 事务处理出错
 *   - TRANS_WAITING: 事务等待中
 *
 * 功能说明：
 * 这是读操作的入口函数，负责：
 * 1. 检查副本ping要求
 * 2. 判断是否需要副本解析或ping操作
 * 3. 如果不需要网络操作，直接进行本地读取
 * 4. 否则创建rw_request并加入哈希表进行协调
 * 5. 根据需要启动副本解析或ping流程
 *
 * 状态机转换：
 * START → 检查ping要求 → {
 *   失败 → send_response → DONE_ERROR
 *   成功 → 检查副本解析/ping需求 → {
 *     无需求 → read_local → DONE_SUCCESS/DONE_ERROR
 *     需副本解析 → dup_res_start → IN_PROGRESS
 *     需ping → repl_ping → IN_PROGRESS
 *     重试 → read_local under hash → DONE_SUCCESS/DONE_ERROR
 *   }
 * }
 */
transaction_status
as_read_start(as_transaction* tr)
{
	// 启动性能基准测试计时
	BENCHMARK_START(tr, read, FROM_CLIENT);
	BENCHMARK_START_FROM_BATCH(tr);

	// 检查副本ping要求，如果检查失败则直接返回错误
	if (! repl_ping_check(tr)) {
		send_read_response(tr, NULL, NULL, 0, NULL);
		return TRANS_DONE_ERROR;
	}

	transaction_status status;
	// 判断是否需要进行副本解析（当存在重复副本且一致性级别为ALL时）
	bool must_duplicate_resolve = read_must_duplicate_resolve(tr);
	// 判断是否需要进行副本ping操作
	bool must_ping = read_must_ping(tr);

	// 如果不需要副本解析和ping，尝试直接进行本地读取
	if (! must_duplicate_resolve && ! must_ping) {
		// 无需网络跳转，尝试读取本地数据
		if ((status = read_local(tr)) != TRANS_IN_PROGRESS) {
			return status;
		}
		// else - 需要在哈希表保护下重试
	}
	// else - 存在重复副本需要解析，或者需要ping副本节点

	// ===== 状态机转换点：需要网络协调的复杂读操作 =====
	// 创建rw_request并添加到哈希表中进行并发控制
	rw_request_hkey hkey = { tr->rsv.ns->ix, tr->keyd };
	rw_request* rw = rw_request_create(&tr->keyd);

	// 如果rw_request无法插入哈希表，说明事务已完成或冲突
	if ((status = rw_request_hash_insert(&hkey, rw, tr)) != TRANS_IN_PROGRESS) {
		rw_request_release(rw);

		if (status != TRANS_WAITING) {
			send_read_response(tr, NULL, NULL, 0, NULL);
		}

		return status;
	}
	// else - rw_request现在已在哈希表中，继续处理...

	// ===== 状态机转换点：启动副本解析流程 =====
	// 如果存在需要解析的重复副本，开始副本解析流程
	if (must_duplicate_resolve &&
			dup_res_start(rw, tr, read_dup_res_start_cb)) {
		return TRANS_IN_PROGRESS; // 已启动副本解析，异步处理
	}

	// ===== 状态机转换点：启动副本ping流程 =====
	if (must_ping) {
		// 设置需要ping的副本节点
		if (! set_replica_destinations(tr, rw)) {
			rw_request_hash_delete(&hkey, rw);
			tr->result_code = AS_ERR_UNAVAILABLE;
			send_read_response(tr, NULL, NULL, 0, NULL);
			return TRANS_DONE_ERROR;
		}

		start_repl_ping(rw, tr);

		// 已启动副本ping，异步处理
		return TRANS_IN_PROGRESS;
	}

	// ===== 状态机转换点：在哈希保护下重试本地读取 =====
	// 在哈希表保护下重试本地读取
	status = read_local(tr);
	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");
	rw_request_hash_delete(&hkey, rw);

	return status;
}


//==========================================================
// 本地辅助函数 - 事务流程控制
//

/**
 * 副本解析启动回调函数
 *
 * @param rw rw_request对象
 * @param tr 事务对象
 * @param r 记录对象
 *
 * 功能说明：
 * 当副本解析流程启动时被调用，负责：
 * 1. 完成rw_request的初始化
 * 2. 构造并发送副本解析消息
 * 3. 设置回调函数和超时处理
 *
 * 状态机转换：副本解析启动 → 发送消息 → 等待响应
 */
static void
read_dup_res_start_cb(rw_request* rw, as_transaction* tr, as_record* r)
{
	// 完成rw_request初始化，构造并发送副本解析消息

	dup_res_make_message(rw, tr, r);

	cf_mutex_lock(&rw->lock);

	dup_res_setup_rw(rw, tr, read_dup_res_cb, read_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

/**
 * 启动副本ping操作
 *
 * @param rw rw_request对象
 * @param tr 事务对象
 *
 * 功能说明：
 * 启动副本ping流程，用于检查副本节点的活跃性：
 * 1. 完成rw_request初始化
 * 2. 构造并发送副本ping消息
 * 3. 设置响应回调和超时处理
 *
 * 状态机转换：ping启动 → 发送消息 → 等待响应
 */
static void
start_repl_ping(rw_request* rw, as_transaction* tr)
{
	// 完成rw初始化，构造并发送副本ping消息

	repl_ping_make_message(rw, tr);

	cf_mutex_lock(&rw->lock);

	repl_ping_setup_rw(rw, tr, repl_ping_cb, read_timeout_cb);
	send_rw_messages(rw);

	cf_mutex_unlock(&rw->lock);
}

/**
 * 副本解析完成回调函数
 *
 * @param rw rw_request对象
 * @return bool 是否完成事务处理（true=完成，false=继续异步处理）
 *
 * 功能说明：
 * 副本解析完成后被调用，负责：
 * 1. 检查解析结果
 * 2. 如果需要ping，继续ping流程
 * 3. 否则进行本地读取并响应
 *
 * 状态机转换：
 * 副本解析完成 → {
 *   失败 → 发送错误响应 → 结束
 *   成功且需ping → 启动ping → 继续异步
 *   成功无需ping → 本地读取 → 结束
 * }
 */
static bool
read_dup_res_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, read, FROM_CLIENT, dup_res);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, dup_res);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	// 检查副本解析结果
	if (tr.result_code != AS_OK) {
		send_read_response(&tr, NULL, NULL, 0, NULL);
		return true; // 事务完成
	}

	// ===== 状态机转换点：副本解析成功后的ping检查 =====
	if (read_must_ping(&tr)) {
		// 设置需要ping的副本节点
		if (! set_replica_destinations(&tr, rw)) {
			tr.result_code = AS_ERR_UNAVAILABLE;
			send_read_response(&tr, NULL, NULL, 0, NULL);
			return true; // 事务完成但失败
		}

		repl_ping_after_dup_res(rw, &tr);

		return false; // 继续异步处理
	}

	// ===== 状态机转换点：进行本地读取 =====
	// 读取本地副本并响应客户端
	transaction_status status = read_local(&tr);

	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");

	if (status == TRANS_WAITING) {
		// 注意 - 新的tr现在拥有msgp，确保rw析构函数不会释放它
		// 同时，rw将释放rsv - 新的tr将获得一个新的rsv
		rw->msgp = NULL;
	}

	// 事务完成 - rw_request负责清理reservation和msgp！
	return true;
}

/**
 * 副本解析后的ping操作
 *
 * @param rw rw_request对象
 * @param tr 事务对象
 *
 * 功能说明：
 * 在副本解析完成后继续进行ping操作：
 * 1. 回收刚用于副本解析的rw_request
 * 2. 重新配置用于副本ping
 * 3. 发送ping消息
 *
 * 注意：此函数在rw_request锁保护下调用
 */
static void
repl_ping_after_dup_res(rw_request* rw, as_transaction* tr)
{
	// 回收刚用于副本解析的rw_request，现在用于副本ping
	// 注意 - 我们现在在rw_request锁保护下！

	repl_ping_make_message(rw, tr);
	repl_ping_reset_rw(rw, tr, repl_ping_cb);
	send_rw_messages(rw);
}

/**
 * 副本ping完成回调函数
 *
 * @param rw rw_request对象
 *
 * 功能说明：
 * 副本ping完成后被调用，进行最终的本地读取：
 * 1. 从rw_request重建事务对象
 * 2. 进行本地数据读取
 * 3. 响应客户端
 *
 * 状态机转换：ping完成 → 本地读取 → 响应客户端 → 事务结束
 */
static void
repl_ping_cb(rw_request* rw)
{
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, read, FROM_CLIENT, repl_ping);
	BENCHMARK_NEXT_DATA_POINT_FROM(rw, batch_sub, FROM_BATCH, repl_ping);

	as_transaction tr;
	as_transaction_init_from_rw(&tr, rw);

	// ===== 状态机转换点：ping完成后的本地读取 =====
	// 读取本地副本并响应客户端
	transaction_status status = read_local(&tr);

	cf_assert(status != TRANS_IN_PROGRESS, AS_RW, "read in-progress");

	if (status == TRANS_WAITING) {
		// 注意 - 新的tr现在拥有msgp，确保rw析构函数不会释放它
		// 同时，rw将释放rsv - 新的tr将获得一个新的rsv
		rw->msgp = NULL;
	}
}


//==========================================================
// 本地辅助函数 - 事务结束处理
//

/**
 * 发送读操作响应
 *
 * @param tr 事务对象
 * @param ops 操作数组指针
 * @param response_bins 响应bin数组
 * @param n_bins bin数量
 * @param db 动态缓冲区（用于ops响应）
 *
 * 功能说明：
 * 根据事务来源（客户端、代理、批处理）发送相应的响应：
 * 1. 检查事务来源的有效性
 * 2. 根据不同来源调用相应的响应发送函数
 * 3. 更新统计信息和性能指标
 * 4. 处理响应压缩
 *
 * 支持的来源类型：
 * - FROM_CLIENT: 直接客户端请求
 * - FROM_PROXY: 代理节点请求
 * - FROM_BATCH: 批处理请求
 */
static void
send_read_response(as_transaction* tr, as_msg_op** ops, as_bin** response_bins,
		uint16_t n_bins, cf_dyn_buf* db)
{
	// 安全检查 - 在与超时竞争时不应该到达这里
	if (! tr->from.any) {
		cf_warning(AS_RW, "transaction origin %u has null 'from'", tr->origin);
		return;
	}

	// 注意 - 如果tr是从rw设置的，rw->from.any已被设置为null，
	// 这通知超时处理它失去了竞争

	switch (tr->origin) {
	case FROM_CLIENT:
		BENCHMARK_NEXT_DATA_POINT(tr, read, local);
		// 处理ops响应（包含操作结果的复杂响应）
		if (db && db->used_sz != 0) {
			as_msg_send_ops_reply(tr->from.proto_fd_h, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		// 处理简单响应（基本记录信息）
		else {
			as_msg_send_reply(tr->from.proto_fd_h, tr->result_code,
					tr->generation, tr->void_time, ops, response_bins, n_bins,
					tr->rsv.ns, as_transaction_trid(tr));
		}
		BENCHMARK_NEXT_DATA_POINT(tr, read, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, read_hist);
		client_read_update_stats(tr->rsv.ns, tr->result_code);
		break;
	case FROM_PROXY:
		// 处理来自代理的请求响应
		if (db && db->used_sz != 0) {
			as_proxy_send_ops_response(tr->from.proxy_node,
					tr->from_data.proxy_tid, db,
					as_transaction_compress_response(tr),
					&tr->rsv.ns->record_comp_stat);
		}
		else {
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid,
					tr->result_code, tr->generation, tr->void_time, ops,
					response_bins, n_bins, tr->rsv.ns, as_transaction_trid(tr));
		}
		// 区分批处理和普通代理请求的统计
		if (as_transaction_is_batch_sub(tr)) {
			from_proxy_batch_sub_read_update_stats(tr->rsv.ns, tr->result_code);
		}
		else {
			from_proxy_read_update_stats(tr->rsv.ns, tr->result_code);
		}
		break;
	case FROM_BATCH:
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, read_local);
		// 批处理请求直接添加到批处理结果中
		as_batch_add_result(tr, n_bins, response_bins, ops);
		BENCHMARK_NEXT_DATA_POINT(tr, batch_sub, response);
		HIST_ACTIVATE_INSERT_DATA_POINT(tr, batch_sub_read_hist);
		batch_sub_read_update_stats(tr->rsv.ns, tr->result_code);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", tr->origin);
		break;
	}

	tr->from.any = NULL; // 标准模式，不是必需的
}

/**
 * 读操作超时回调函数
 *
 * @param rw rw_request对象
 *
 * 功能说明：
 * 当读操作超时时被调用，负责：
 * 1. 检查是否与其他回调竞争
 * 2. 根据请求来源发送超时错误响应
 * 3. 更新超时统计信息
 * 4. 标记回调竞争状态
 *
 * 注意：超时不包含在性能直方图中
 */
static void
read_timeout_cb(rw_request* rw)
{
	if (! rw->from.any) {
		return; // 在与副本解析回调的竞争中失败
	}

	switch (rw->origin) {
	case FROM_CLIENT:
		as_msg_send_reply(rw->from.proto_fd_h, AS_ERR_TIMEOUT, 0, 0, NULL, NULL,
				0, rw->rsv.ns, rw_request_trid(rw));
		// 超时不包含在直方图中
		client_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	case FROM_PROXY:
		// 区分批处理和普通代理超时
		if (rw_request_is_batch_sub(rw)) {
			from_proxy_batch_sub_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		else {
			from_proxy_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		}
		break;
	case FROM_BATCH:
		as_batch_add_error(rw->from.batch_shared, rw->from_data.batch_index,
				AS_ERR_TIMEOUT);
		// 超时不包含在直方图中
		batch_sub_read_update_stats(rw->rsv.ns, AS_ERR_TIMEOUT);
		break;
	default:
		cf_crash(AS_RW, "unexpected transaction origin %u", rw->origin);
		break;
	}

	rw->from.any = NULL; // 通知其他回调它失去了竞争
}


//==========================================================
// 本地辅助函数 - 本地读取处理
//

/**
 * 执行本地数据读取操作
 *
 * @param tr 事务对象
 * @return transaction_status 事务状态
 *   - TRANS_DONE_SUCCESS: 读取成功
 *   - TRANS_DONE_ERROR: 读取失败
 *   - TRANS_WAITING: 需要等待（重新排队）
 *
 * 功能说明：
 * 这是核心的本地读取函数，负责：
 * 1. 从索引中获取记录引用
 * 2. 执行各种验证检查（set名称、过期、截断、副本状态等）
 * 3. 处理元数据过滤和表达式过滤
 * 4. 根据请求类型读取相应的数据
 * 5. 支持多种操作类型（普通读、位操作、HLL、CDT、表达式）
 * 6. 构造并发送响应
 *
 * 状态机转换：
 * 开始 → 获取记录 → 验证检查 → {
 *   验证失败 → read_local_done(错误) → DONE_ERROR
 *   记录不存在/过期 → read_local_done(NOT_FOUND) → DONE_ERROR
 *   需要touch → 重新排队 → WAITING
 *   验证成功 → 读取数据 → 构造响应 → DONE_SUCCESS
 * }
 */
static transaction_status
read_local(as_transaction* tr)
{
	as_msg* m = &tr->msgp->msg;
	as_namespace* ns = tr->rsv.ns;

	as_index_ref r_ref;

	// ===== 状态机转换点：记录查找 =====
	// 从主索引中获取记录
	if (as_record_get(tr->rsv.tree, &tr->keyd, &r_ref) != 0) {
		read_local_done(tr, NULL, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	as_record* r = r_ref.r;

	// ===== 验证检查阶段 =====
	// 确保消息中的set名称（如果存在）是正确的
	if (! set_name_check(tr, r)) {
		read_local_done(tr, &r_ref, NULL, AS_ERR_PARAMETER);
		return TRANS_DONE_ERROR;
	}

	// 检查记录是否已过期或被截断
	if (as_record_is_doomed(r, ns)) {
		read_local_done(tr, &r_ref, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	// ===== 状态机转换点：副本状态检查 =====
	int result = repl_state_check(r, tr);

	if (result != 0) {
		if (result == -3) {
			read_local_done(tr, &r_ref, NULL, AS_ERR_UNAVAILABLE);
			return TRANS_DONE_ERROR;
		}

		// 不向客户端发送响应，进行重新排队或异步处理
		as_record_done(&r_ref, ns);
		return result == 1 ? TRANS_IN_PROGRESS : TRANS_WAITING;
	}

	// 检查是否为墓碑记录
	if (! as_record_is_live(r)) {
		read_local_done(tr, &r_ref, NULL, AS_ERR_NOT_FOUND);
		return TRANS_DONE_ERROR;
	}

	// ===== 状态机转换点：Touch检查 =====
	if ((result = as_read_touch_check(r, tr)) != 0) {
		if (result < 0) {
			read_local_done(tr, &r_ref, NULL, AS_ERR_PARAMETER);
			return TRANS_DONE_ERROR;
		}

		// 重新排队，期望代理到主节点
		as_record_done(&r_ref, ns);
		return TRANS_WAITING;
	}

	as_exp* filter_exp = NULL;

	// ===== 元数据过滤处理 =====
	// 处理存在的元数据过滤器
	if ((result = handle_meta_filter(tr, r, &filter_exp)) != 0) {
		read_local_done(tr, &r_ref, NULL, result);
		return TRANS_DONE_ERROR;
	}

	as_storage_rd rd;

	as_storage_record_open(ns, r, &rd);

	// 如果配置允许，允许读取使用页面缓存
	rd.read_page_cache = ns->storage_read_page_cache;

	// ===== bin过滤器应用 =====
	// 应用存在的记录bin过滤器
	if (filter_exp != NULL) {
		if ((result = read_and_filter_bins(&rd, filter_exp)) != 0) {
			destroy_filter_exp(tr, filter_exp);
			read_local_done(tr, &r_ref, &rd, result);
			return TRANS_DONE_ERROR;
		}

		destroy_filter_exp(tr, filter_exp);
	}

	// ===== 密钥验证 =====
	// 如果需要检查密钥
	// 注意 - 对于数据不在内存的"exists"操作，密钥检查是昂贵的！
	if (as_transaction_has_key(tr) &&
			as_storage_rd_load_key(&rd) && ! check_msg_key(m, &rd)) {
		read_local_done(tr, &r_ref, &rd, AS_ERR_KEY_MISMATCH);
		return TRANS_DONE_ERROR;
	}

	// ===== 只获取元数据的处理 =====
	if ((m->info1 & AS_MSG_INFO1_GET_NO_BINS) != 0) {
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		read_local_done(tr, &r_ref, &rd, AS_OK);
		return TRANS_DONE_SUCCESS;
	}

	// ===== 数据读取和操作处理 =====
	as_bin stack_bins[RECORD_MAX_BINS];

	if ((result = as_storage_rd_load_bins(&rd, stack_bins)) < 0) {
		cf_warning(AS_RW, "{%s} read_local: failed as_storage_rd_load_bins() %pD", ns->name, &tr->keyd);
		read_local_done(tr, &r_ref, &rd, -result);
		return TRANS_DONE_ERROR;
	}

	uint32_t bin_count = (m->info1 & AS_MSG_INFO1_GET_ALL) != 0 ?
			rd.n_bins : m->n_ops;

	as_msg_op* ops[bin_count];
	as_msg_op** p_ops = ops;
	as_bin* response_bins[bin_count];
	uint16_t n_bins = 0;

	as_bin result_bins[bin_count];
	uint32_t n_result_bins = 0;

	// ===== 状态机转换点：处理GET_ALL或具体操作 =====
	if ((m->info1 & AS_MSG_INFO1_GET_ALL) != 0) {
		// 获取所有bin的处理
		p_ops = NULL;

		for (uint16_t i = 0; i < rd.n_bins; i++) {
			as_bin* b = &rd.bins[i];

			if (! as_bin_is_tombstone(b)) {
				response_bins[n_bins++] = b;
			}
		}
	}
	else {
		// 处理具体的操作请求
		if (m->n_ops == 0) {
			cf_warning(AS_RW, "{%s} read_local: bin op(s) expected, none present %pD", ns->name, &tr->keyd);
			read_local_done(tr, &r_ref, &rd, AS_ERR_PARAMETER);
			return TRANS_DONE_ERROR;
		}

		bool respond_all_ops = (m->info2 & AS_MSG_INFO2_RESPOND_ALL_OPS) != 0;

		as_msg_op* op = 0;
		uint16_t n = 0;

		// ===== 迭代处理每个操作 =====
		while ((op = as_msg_op_iterate(m, op, &n)) != NULL) {
			// 验证bin名称
			if (! as_bin_name_check(op->name, op->name_sz)) {
				cf_warning(AS_RW, "{%s} read_local: bad bin name %.*s (%u) %pD", ns->name, op->name_sz, op->name, op->name_sz, &tr->keyd);
				as_bin_destroy_all(result_bins, n_result_bins);
				read_local_done(tr, &r_ref, &rd, AS_ERR_BIN_NAME);
				return TRANS_DONE_ERROR;
			}

			// ===== 根据操作类型进行处理 =====
			if (op->op == AS_MSG_OP_READ) {
				// 普通读操作
				as_bin* b = as_bin_get_live_w_len(&rd, op->name, op->name_sz);

				if (b || respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = b;
				}
			}
			else if (op->op == AS_MSG_OP_BITS_READ) {
				// 位操作读取
				as_bin* b = as_bin_get_live_w_len(&rd, op->name, op->name_sz);

				if (b) {
					as_bin* rb = &result_bins[n_result_bins];
					as_bin_set_empty(rb);

					if ((result = as_bin_bits_read_from_client(b, op, rb)) < 0) {
						cf_detail(AS_RW, "{%s} read_local: failed as_bin_bits_read_from_client() %pD", ns->name, &tr->keyd);
						as_bin_destroy_all(result_bins, n_result_bins);
						read_local_done(tr, &r_ref, &rd, -result);
						return TRANS_DONE_ERROR;
					}

					if (as_bin_is_used(rb)) {
						n_result_bins++;
						ops[n_bins] = op;
						response_bins[n_bins++] = rb;
					}
					else if (respond_all_ops) {
						ops[n_bins] = op;
						response_bins[n_bins++] = NULL;
					}
				}
				else if (respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = NULL;
				}
			}
			else if (op->op == AS_MSG_OP_HLL_READ) {
				// HyperLogLog读操作
				as_bin* b = as_bin_get_live_w_len(&rd, op->name, op->name_sz);

				if (b) {
					as_bin* rb = &result_bins[n_result_bins];
					as_bin_set_empty(rb);

					if ((result = as_bin_hll_read_from_client(b, op, rb)) < 0) {
						cf_detail(AS_RW, "{%s} read_local: failed as_bin_hll_read_from_client() %pD", ns->name, &tr->keyd);
						as_bin_destroy_all(result_bins, n_result_bins);
						read_local_done(tr, &r_ref, &rd, -result);
						return TRANS_DONE_ERROR;
					}

					if (as_bin_is_used(rb)) {
						n_result_bins++;
						ops[n_bins] = op;
						response_bins[n_bins++] = rb;
					}
					else if (respond_all_ops) {
						ops[n_bins] = op;
						response_bins[n_bins++] = NULL;
					}
				}
				else if (respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = NULL;
				}
			}
			else if (op->op == AS_MSG_OP_CDT_READ) {
				// 复杂数据类型(CDT)读操作
				as_bin* b = as_bin_get_live_w_len(&rd, op->name, op->name_sz);

				if (b) {
					as_bin* rb = &result_bins[n_result_bins];
					as_bin_set_empty(rb);

					if ((result = as_bin_cdt_read_from_client(b, op, rb)) < 0) {
						cf_detail(AS_RW, "{%s} read_local: failed as_bin_cdt_read_from_client() %pD", ns->name, &tr->keyd);
						as_bin_destroy_all(result_bins, n_result_bins);
						read_local_done(tr, &r_ref, &rd, -result);
						return TRANS_DONE_ERROR;
					}

					if (as_bin_is_used(rb)) {
						n_result_bins++;
						ops[n_bins] = op;
						response_bins[n_bins++] = rb;
					}
					else if (respond_all_ops) {
						ops[n_bins] = op;
						response_bins[n_bins++] = NULL;
					}
				}
				else if (respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = NULL;
				}
			}
			else if (op->op == AS_MSG_OP_EXP_READ) {
				// 表达式读操作
				const as_exp_ctx exp_ctx = { .ns = ns, .rd = &rd, .r = rd.r };

				as_bin* rb = &result_bins[n_result_bins];
				as_bin_set_empty(rb);

				if ((result = as_bin_exp_read_from_client(&exp_ctx, op, rb)) < 0) {
					cf_detail(AS_RW, "{%s} read_local: failed as_bin_exp_read_from_client() %pD", ns->name, &tr->keyd);
					as_bin_destroy_all(result_bins, n_result_bins);
					read_local_done(tr, &r_ref, &rd, -result);
					return TRANS_DONE_ERROR;
				}

				if (as_bin_is_used(rb)) {
					n_result_bins++;
					ops[n_bins] = op;
					response_bins[n_bins++] = rb;
				}
				else if (respond_all_ops) {
					ops[n_bins] = op;
					response_bins[n_bins++] = NULL;
				}
			}
			else {
				// 未知操作类型
				cf_warning(AS_RW, "{%s} read_local: unexpected bin op %u %pD", ns->name, op->op, &tr->keyd);
				as_bin_destroy_all(result_bins, n_result_bins);
				read_local_done(tr, &r_ref, &rd, AS_ERR_PARAMETER);
				return TRANS_DONE_ERROR;
			}
		}
	}

	// ===== 响应构造和发送 =====
	cf_dyn_buf_define_size(db, 16 * 1024);

	if (tr->origin != FROM_BATCH) {
		// 非批处理请求：构造完整的响应消息
		db.used_sz = db.alloc_sz;
		db.buf = (uint8_t*)as_msg_make_response_msg(tr->result_code,
				r->generation, r->void_time, p_ops, response_bins, n_bins, ns,
				(cl_msg*)dyn_bufdb, &db.used_sz, as_transaction_trid(tr));

		db.is_stack = db.buf == dyn_bufdb;
		// 注意 - 如果buf被分配，则不费心更正alloc_sz
	}
	else {
		// 批处理请求：直接设置元数据，响应将由批处理处理器构造
		tr->generation = r->generation;
		tr->void_time = r->void_time;
		tr->last_update_time = r->last_update_time;

		// 由于as_batch_add_result()直接在共享缓冲区中构造响应
		// 以避免额外的复制，不能使用db
		send_read_response(tr, p_ops, response_bins, n_bins, NULL);
	}

	// ===== 清理和最终响应发送 =====
	as_bin_destroy_all(result_bins, n_result_bins);
	as_storage_record_close(&rd);
	as_record_done(&r_ref, ns);

	// 现在我们不在记录锁保护下，发送刚构造的消息
	if (db.used_sz != 0) {
		send_read_response(tr, NULL, NULL, 0, &db);

		cf_dyn_buf_free(&db);
		tr->from.proto_fd_h = NULL;
	}

	return TRANS_DONE_SUCCESS;
}

/**
 * 完成本地读取操作的清理工作
 *
 * @param tr 事务对象
 * @param r_ref 记录引用（可为NULL）
 * @param rd 存储记录描述符（可为NULL）
 * @param result_code 结果码
 *
 * 功能说明：
 * 统一的本地读取清理函数，负责：
 * 1. 关闭存储记录描述符
 * 2. 释放记录引用
 * 3. 设置事务结果码
 * 4. 发送错误响应
 *
 * 注意：此函数处理各种错误情况的清理工作
 */
static void
read_local_done(as_transaction* tr, as_index_ref* r_ref, as_storage_rd* rd,
		int result_code)
{
	// 清理存储相关资源
	if (r_ref) {
		if (rd) {
			as_storage_record_close(rd);
		}

		as_record_done(r_ref, tr->rsv.ns);
	}

	// 设置结果并发送响应
	tr->result_code = (uint8_t)result_code;

	send_read_response(tr, NULL, NULL, 0, NULL);
}
