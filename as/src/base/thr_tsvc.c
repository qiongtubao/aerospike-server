/*
 * thr_tsvc.c - 事务服务线程模块
 *
 * 模块职责：
 * 本模块是 Aerospike 单条记录事务的统一分发入口。
 * 客户端、批量子请求、代理请求等均经由 as_tsvc_process_transaction() 路由到具体处理路径：
 *   - 查询请求（multi-record）   → as_query()
 *   - 写请求（含 delete/UDF）   → as_write_start() / as_delete_start() / as_udf_start()
 *   - 读请求                    → as_read_start()
 *   - read-touch / re-replicate → as_read_touch_start() / as_re_replicate_start()
 *   - 无法在本节点处理（partition 不在本节点）→ 代理转发 as_proxy_divert()
 *
 * 关键流程：
 *   1. 安全认证检查
 *   2. 解析 namespace 字段，获取 as_namespace*
 *   3. 等待初始 partition balance 完成
 *   4. 计算并检查事务超时
 *   5. 预留 partition（写用 reserve_write，读用 reserve_read_tr）
 *   6. 根据操作类型分发到对应处理函数，返回 transaction_status
 *   7. 根据 status 决定是否释放 partition 预留及 msgp
 *
 * Copyright (C) 2008-2021 Aerospike, Inc.
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

#include "base/thr_tsvc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "node.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/stats.h"
#include "base/transaction.h"
#include "base/transaction_policy.h"
#include "base/xdr.h"
#include "fabric/partition.h"
#include "fabric/partition_balance.h"
#include "query/query.h"
#include "storage/storage.h"
#include "transaction/delete.h"
#include "transaction/proxy.h"
#include "transaction/re_replicate.h"
#include "transaction/read.h"
#include "transaction/read_touch.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"
#include "transaction/write.h"


//==========================================================
// Inlines & macros.
//

/*
 * 判断该事务是否需要执行数据操作权限检查。
 * 只有来自客户端（FROM_CLIENT）或批量请求（FROM_BATCH）的事务才需要安全检查；
 * 内部事务（代理、IUDF、IOPS 等）跳过此检查。
 */
static inline bool
should_security_check_data_op(const as_transaction *tr)
{
	return tr->origin == FROM_CLIENT || tr->origin == FROM_BATCH;
}

/*
 * 根据查询事务类型返回所需安全权限。
 * - UDF 查询：PERM_UDF_QUERY
 * - 写操作查询（带 INFO2_WRITE 标志）：PERM_OPS_QUERY
 * - 普通只读查询：PERM_QUERY
 */
static inline as_sec_perm
query_perm(const as_transaction *tr)
{
	if (as_transaction_is_udf(tr)) {
		return PERM_UDF_QUERY;
	}

	return (tr->msgp->msg.info2 & AS_MSG_INFO2_WRITE) != 0 ?
			PERM_OPS_QUERY : PERM_QUERY;
}

/*
 * 返回写事务的操作类型标签字符串，用于日志输出。
 * - delete 事务 → "delete"
 * - UDF 事务   → "udf"
 * - 普通写     → "write"
 */
static inline const char*
write_type_tag(const as_transaction *tr)
{
	return as_transaction_is_delete(tr) ? "delete" :
			(as_transaction_is_udf(tr) ? "udf" : "write");
}

/*
 * 记录唯一事务的详细日志（仅首次，不记录重试）。
 * 对来自客户端或批量请求的事务，输出 namespace、digest、客户端地址及操作类型。
 * 用于调试跟踪单条记录操作路径。
 */
static inline void
detail_unique(const as_transaction *tr, bool is_write)
{
	if (as_transaction_is_restart(tr)) {
		return;
	}

	if (tr->origin == FROM_CLIENT) {
		cf_detail(AS_RW_CLIENT, "{%s} digest %pD client %s %s",
				tr->rsv.ns->name, &tr->keyd, tr->from.proto_fd_h->client,
				is_write ? write_type_tag(tr) : "read");
	}
	else if (tr->origin == FROM_BATCH) {
		cf_detail(AS_BATCH_SUB, "{%s} digest %pD client %s %s",
				tr->rsv.ns->name, &tr->keyd,
				as_batch_get_fd_h(tr->from.batch_shared)->client,
				is_write ? write_type_tag(tr) : "read");
	}
}


//==========================================================
// Public API.
//

// Handle the transaction, including proxy to another node if necessary.
// 单条数据事务的统一入口：XDR/Info 等特殊类型先处理；否则校验 namespace、权限、partition reserve，
// 成功后根据 is_write/delete/udf/read_touch/re_repl 分发到 as_write_start、as_delete_start、as_read_start 等。
//
// 参数：
//   tr - 待处理的事务结构体指针，包含消息体、来源、key digest 等信息
/**
 * 事务服务核心处理函数 - 单条记录事务的统一入口
 *
 * 功能描述：
 * 这是 Aerospike 中所有单条记录事务的中央分发点，包括：
 * - 客户端直接请求（FROM_CLIENT）
 * - 批处理子请求（FROM_BATCH）
 * - 代理转发请求（FROM_PROXY）
 * - 内部操作请求（FROM_IUDF、FROM_IOPS 等）
 *
 * 主要处理流程：
 * 1. 特殊事务类型：XDR 内部事务直接处理
 * 2. 消息体初始化：设置事务的消息体信息
 * 3. 安全认证检查：验证客户端连接权限
 * 4. 命名空间解析：提取并验证目标命名空间
 * 5. 集群状态检查：确保分区平衡已完成初始化
 * 6. 事务类型分发：
 *    - 查询事务（多记录）→ as_query()
 *    - 单记录事务 → 进一步分类处理
 * 7. 单记录事务处理：
 *    - 超时检查：验证队列等待时间
 *    - Key digest 提取：获取记录唯一标识
 *    - 权限验证：检查操作权限
 *    - 分区预留：预留读/写分区资源
 *    - 操作分发：调用相应的处理函数
 *
 * 事务分发路径：
 * - 写路径：delete → as_write_start/as_delete_start
 * - UDF 路径：as_udf_start
 * - 特殊路径：read_touch → as_read_touch_start
 * - 复制路径：re_repl → as_re_replicate_start
 * - 默认写路径：as_write_start
 * - 读路径：as_read_start
 *
 * 分区管理：
 * - 成功预留：事务继续处理，资源由后续模块管理
 * - 预留失败：根据来源类型进行代理转发或错误处理
 *
 * 参数说明：
 * @tr: 事务对象指针，包含消息、来源、状态等信息
 *
 * 资源管理：
 * - 消息内存：根据事务状态决定是否释放
 * - 分区预留：根据事务状态决定是否保持
 * - 共享消息：批处理的共享消息不在此释放
 */
//
// 函数执行后，tr 所有权由各子路径接管或在此函数末尾清理。
void
as_tsvc_process_transaction(as_transaction *tr)
{
	// 内部 XDR 类型消息（跨数据中心复制），走专用 xdr_read 路径，不经过常规事务流程。
	if (tr->msgp->proto.type == PROTO_TYPE_INTERNAL_XDR) {
		as_xdr_read(tr);
		return;
	}

	int rv;
	bool free_msgp = true;
	cl_msg *msgp = tr->msgp;
	as_msg *m = &msgp->msg;

	// 初始化事务体（msg_fields 标志位、result_code 等），与 head 分离初始化。
	as_transaction_init_body(tr);

	// Check that the socket is authenticated.
	// 安全认证检查：仅客户端连接需要验证 socket 是否已通过认证。
	if (tr->origin == FROM_CLIENT) {
		uint8_t result = as_security_check_auth(tr->from.proto_fd_h);

		if (result != AS_OK) {
			as_security_log(tr->from.proto_fd_h, result, PERM_NONE, NULL, NULL);
			as_transaction_error(tr, NULL, (uint32_t)result);
			goto Cleanup;
		}
	}

	// All transactions must have a namespace.
	// 所有事务必须携带 namespace 字段，未携带则返回 AS_ERR_NAMESPACE。
	as_msg_field *nf = as_msg_field_get(m, AS_MSG_FIELD_TYPE_NAMESPACE);

	if (! nf) {
		cf_warning(AS_TSVC, "no namespace in protocol request");
		as_transaction_error(tr, NULL, AS_ERR_NAMESPACE);
		goto Cleanup;
	}

	as_namespace *ns = as_namespace_get_bymsgfield(nf);

	if (! ns) {
		uint32_t ns_sz = as_msg_field_get_value_sz(nf);

		cf_warning(AS_TSVC, "unknown namespace %.*s (%u) in protocol request - check configuration file",
				ns_sz, nf->data, ns_sz);

		as_transaction_error(tr, NULL, AS_ERR_NAMESPACE);
		goto Cleanup;
	}

	// Have we finished the very first partition balance?
	// 检查初始 partition balance 是否已完成。
	// 若尚未完成，代理请求退回发送方，其他请求返回 AS_ERR_UNAVAILABLE。
	if (! as_partition_balance_is_init_resolved()) {
		if (tr->origin == FROM_PROXY) {
			as_proxy_return_to_sender(tr, ns);
			tr->from.proxy_node = 0; // pattern, not needed
		}
		else {
			cf_debug(AS_TSVC, "rejecting transaction - initial partition balance unresolved");
			as_transaction_error(tr, NULL, AS_ERR_UNAVAILABLE);
			// Note that we forfeited namespace info above so query doesn't get
			// counted as single-record error.
		}

		goto Cleanup;
	}

	//------------------------------------------------------
	// Query.
	//

	if (as_transaction_is_query(tr)) {
		if (! as_security_check_data_op(tr, ns, query_perm(tr))) {
			as_multi_rec_transaction_error(tr, tr->result_code);
			goto Cleanup;
		}

		rv = as_query(tr, ns);

		if (rv != 0) {
			as_multi_rec_transaction_error(tr, rv);
		}

		goto Cleanup;
	}

	//------------------------------------------------------
	// Single-record transaction.
	//

	// Calculate end_time based on message transaction TTL. May be recalculating
	// for re-queued transactions, but nice if end_time not copied on/off queue.
	// 根据消息中的 transaction_ttl 计算事务截止时间（纳秒）。
	// 若 ttl 为 0，则使用全局默认超时 g_config.transaction_max_ns。
	if (m->transaction_ttl != 0) {
		tr->end_time = tr->start_time +
				((uint64_t)m->transaction_ttl * 1000000);
	}
	else {
		// Incorporate g_config.transaction_max_ns if appropriate.
		// TODO - should g_config.transaction_max_ns = 0 be special?
		tr->end_time = tr->start_time + g_config.transaction_max_ns;
	}

	// Did the transaction time out while on the queue?
	// 检查事务在队列中等待期间是否已超时，超时则返回 AS_ERR_TIMEOUT。
	if (cf_getns() > tr->end_time) {
		cf_debug(AS_TSVC, "transaction timed out in queue");
		as_transaction_error(tr, ns, AS_ERR_TIMEOUT);
		goto Cleanup;
	}

	// Copy digest if not already in tr.
	if (as_transaction_has_digest(tr)) {
		as_msg_field *df = as_msg_field_get(m, AS_MSG_FIELD_TYPE_DIGEST_RIPE);
		uint32_t digest_sz = as_msg_field_get_value_sz(df);

		if (digest_sz != sizeof(cf_digest)) {
			cf_warning(AS_TSVC, "digest msg field size %u", digest_sz);
			as_transaction_error(tr, ns, AS_ERR_PARAMETER);
			goto Cleanup;
		}

		tr->keyd = *(cf_digest *)df->data;
	}
	// else - batch sub-transactions & all internal transactions have no digest
	// in the message - digest is already in tr.

	// Process the transaction.
	// 判断事务读写属性：
	// - is_write=true 走写预留路径（同时可能带 READ 标志用于 read-modify-write）
	// - is_read=true  走读预留路径
	bool is_write = (m->info2 & AS_MSG_INFO2_WRITE) != 0;
	bool is_read = (m->info1 & AS_MSG_INFO1_READ) != 0;
	// Both can be set together, but is_write puts us on the 'write path' -
	// write reservation, replica writes, etc. Writes quickly get split into
	// write, delete, or UDF after the reservation.

	uint32_t pid = as_partition_getid(&tr->keyd);
	cf_node dest;

	if (is_write) {
		if (should_security_check_data_op(tr) &&
				! as_security_check_data_op(tr, ns,
						PERM_WRITE | (is_read ? PERM_READ : 0))) {
			as_transaction_error(tr, ns, tr->result_code);
			goto Cleanup;
		}

		rv = as_partition_reserve_write(ns, pid, &tr->rsv, &dest);
	}
	else if (is_read) {
		if (should_security_check_data_op(tr) &&
				! as_security_check_data_op(tr, ns, PERM_READ)) {
			as_transaction_error(tr, ns, tr->result_code);
			goto Cleanup;
		}

		rv = as_partition_reserve_read_tr(ns, pid, tr, &dest);
	}
	else {
		cf_warning(AS_TSVC, "transaction is neither read nor write - unexpected");
		as_transaction_error(tr, ns, AS_ERR_PARAMETER);
		goto Cleanup;
	}

	if (rv == -2) {
		// Partition is unavailable.
		// partition 处于不可用状态（迁移中或节点故障），直接返回错误。
		as_transaction_error(tr, ns, AS_ERR_UNAVAILABLE);
		goto Cleanup;
	}

	if (dest == 0) {
		cf_crash(AS_TSVC, "invalid destination while reserving partition");
	}

	if (rv == 0) {
		// <><><><><><>  Reservation Succeeded  <><><><><><>
		// partition 预留成功，本节点是该 key 的 master，执行实际操作。

		detail_unique(tr, is_write);

		transaction_status status;

		// 写路径分发：根据 delete/UDF/read_touch/re_repl 标志选择对应处理函数。
		// 写路径：delete -> write/delete_start；UDF -> udf_start；read_touch -> read_touch_start；re_repl -> re_replicate_start；否则 as_write_start(tr)。
		if (is_write) {
			if (as_transaction_is_delete(tr)) {
				status = convert_to_write(tr, &msgp) ?
						as_write_start(tr) : as_delete_start(tr);
			}
			else if (tr->origin == FROM_IUDF || as_transaction_is_udf(tr)) {
				status = as_udf_start(tr);
			}
			else if (tr->origin == FROM_READ_TOUCH) {
				status = as_read_touch_start(tr);
			}
			else if (tr->origin == FROM_RE_REPL) {
				status = as_re_replicate_start(tr);
			}
			else {
				status = as_write_start(tr);
			}
		}
		else {
			status = as_read_start(tr);
		}

		switch (status) {
		case TRANS_DONE_ERROR:
		case TRANS_DONE_SUCCESS:
			// Done, response already sent - free msg & release reservation.
			// 事务已同步完成（成功或失败），响应已发送，释放 partition 预留。
			as_partition_release(&tr->rsv);
			break;
		case TRANS_IN_PROGRESS:
			// Don't free msg or release reservation - both owned by rw_request.
			// 事务异步进行中（等待副本响应等），msgp 和 partition 预留由 rw_request 持有，此处不释放。
			free_msgp = false;
			break;
		case TRANS_WAITING:
			// Will be re-queued - don't free msg, but release reservation.
			// 事务因 key 冲突进入等待队列，msgp 由等待元素持有，但 partition 预留需释放。
			free_msgp = false;
			as_partition_release(&tr->rsv);
			break;
		default:
			cf_crash(AS_TSVC, "invalid transaction status %d", status);
			break;
		}
	}
	else {
		// <><><><><><>  Reservation Failed  <><><><><><>
		// partition 预留失败：本节点不是该 key 的 master，需要将请求转发到目标节点。
		// dest 是持有该 partition 的目标节点 ID。

		switch (tr->origin) {
		case FROM_CLIENT:
		case FROM_BATCH:
			// 客户端/批量请求：通过 fabric 代理转发到 dest 节点，msgp 由 fabric 接管。
			as_proxy_divert(dest, tr, ns);
			// CLIENT: fabric owns msgp, BATCH: it's shared, don't free it.
			free_msgp = false;
			break;
		case FROM_PROXY:
			// 已是代理请求但目标不对：退回原始发送方重新路由。
			as_proxy_return_to_sender(tr, ns);
			tr->from.proxy_node = 0; // pattern, not needed
			break;
		case FROM_IUDF:
			// 内部 UDF 子事务：统计错误并通知调用方完成回调。
			as_incr_uint64(&ns->n_udf_sub_tsvc_error);
			tr->from.iudf_orig->done_cb(tr->from.iudf_orig->udata,
					AS_ERR_UNKNOWN);
			tr->from.iudf_orig = NULL; // pattern, not needed
			break;
		case FROM_IOPS:
			// 内部 ops 子事务：统计错误并通知调用方完成回调。
			as_incr_uint64(&ns->n_ops_sub_tsvc_error);
			tr->from.iops_orig->done_cb(tr->from.iops_orig->udata,
					AS_ERR_UNKNOWN);
			tr->from.iops_orig = NULL; // pattern, not needed
			break;
		case FROM_READ_TOUCH:
			// read-touch 内部事务失败，统计错误计数。
			as_incr_uint64(&ns->n_read_touch_tsvc_error);
			tr->from.read_touch_active = NULL; // pattern, not needed
			break;
		case FROM_RE_REPL:
			// re-replicate 内部事务失败，调用原始回调通知上层。
			as_incr_uint64(&ns->n_re_repl_tsvc_error);
			tr->from.re_repl_orig_cb(tr);
			tr->from.re_repl_orig_cb = NULL; // pattern, not needed
			break;
		default:
			cf_crash(AS_TSVC, "unexpected transaction origin %u", tr->origin);
			break;
		}
	}

Cleanup:

	if (free_msgp && ! SHARED_MSGP(tr)) {
		cf_free(msgp);
	}
} // end process_transaction()
