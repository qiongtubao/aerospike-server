/*
 * transaction.c
 *
 * Copyright (C) 2008-2020 Aerospike, Inc.
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

/**
 * ===========================================================================
 * 事务管理模块 (Transaction Management Module)
 * ===========================================================================
 *
 * 模块职责:
 * - 事务对象的初始化、拷贝和销毁
 * - 事务消息的解析和字段验证
 * - 错误处理和响应发送
 * - 文件句柄的生命周期管理
 * - 支持多种事务来源(客户端、代理、批处理、UDF等)
 *
 * 核心数据流:
 * 1. 客户端请求 → 协议解析 → 事务创建
 * 2. 事务执行 → 结果生成 → 响应发送
 * 3. 错误处理 → 统计更新 → 连接清理
 *
 * 关键概念:
 * - as_transaction: 事务的核心数据结构
 * - 消息字段解析: 支持多种字段类型的动态解析
 * - 多来源支持: 客户端、代理节点、批处理、内部UDF等
 * - 错误统计: 按命名空间和操作类型分类的详细统计
 * ===========================================================================
 */

#include "base/transaction.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"

#include "log.h"
#include "socket.h"

#include "base/batch.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/service.h"
#include "base/stats.h"
#include "fabric/partition.h"
#include "transaction/proxy.h"
#include "transaction/rw_request.h"
#include "transaction/rw_utils.h"
#include "transaction/udf.h"
#include "transaction/write.h"


/**
 * 初始化事务头部信息
 *
 * 功能描述:
 * 初始化事务对象的头部字段，包括消息指针、摘要键、来源信息等基本属性。
 * 这些字段在事务的整个生命周期中保持不变。
 *
 * 参数说明:
 * @param tr    - 待初始化的事务对象
 * @param keyd  - 记录的摘要键(可为NULL，表示使用零摘要)
 * @param msgp  - 客户端消息指针
 *
 * 关键逻辑:
 * 1. 设置消息指针和字段标志位
 * 2. 清零来源信息和标志位
 * 3. 设置摘要键(如果提供)或使用零摘要
 * 4. 重置时间戳字段
 */
void
as_transaction_init_head(as_transaction *tr, const cf_digest *keyd,
		cl_msg *msgp)
{
	tr->msgp				= msgp;          // 设置客户端消息指针
	tr->msg_fields			= 0;             // 清零消息字段标志位

	tr->origin				= 0;             // 清零事务来源
	tr->from_flags			= 0;             // 清零来源标志位

	tr->from.any			= NULL;          // 清空来源联合体
	tr->from_data.any		= 0;             // 清空来源数据

	tr->keyd				= keyd ? *keyd : cf_digest_zero; // 设置摘要键

	tr->start_time			= 0;             // 重置开始时间
	tr->benchmark_time		= 0;             // 重置基准时间
}

/**
 * 初始化事务主体信息
 *
 * 功能描述:
 * 初始化事务对象的主体字段，包括分区保留、结果代码、时间戳等执行相关的属性。
 * 这些字段在事务执行过程中会被修改和更新。
 *
 * 参数说明:
 * @param tr - 待初始化的事务对象
 *
 * 关键逻辑:
 * 1. 初始化分区保留结构
 * 2. 设置默认的结果代码为成功
 * 3. 清零所有时间戳和标志位
 */
void
as_transaction_init_body(as_transaction *tr)
{
	AS_PARTITION_RESERVATION_INIT(tr->rsv); // 初始化分区保留

	tr->end_time			= 0;             // 清零结束时间
	tr->result_code			= AS_OK;         // 设置默认结果为成功
	tr->flags				= 0;             // 清零事务标志位
	tr->generation			= 0;             // 清零记录代数
	tr->void_time			= 0;             // 清零过期时间
	tr->last_update_time	= 0;             // 清零最后更新时间
	tr->epoch_ms			= 0;             // 清零纪元毫秒数
}

/**
 * 拷贝事务头部信息
 *
 * 功能描述:
 * 将源事务的头部信息完整拷贝到目标事务中，用于创建事务副本或传递事务上下文。
 * 只拷贝头部信息，不涉及执行状态和结果。
 *
 * 参数说明:
 * @param to   - 目标事务对象
 * @param from - 源事务对象
 *
 * 关键逻辑:
 * 1. 逐字段拷贝所有头部属性
 * 2. 保持消息指针和来源信息的引用关系
 * 3. 拷贝时间戳信息以保持执行上下文
 */
void
as_transaction_copy_head(as_transaction *to, const as_transaction *from)
{
	to->msgp				= from->msgp;        // 拷贝消息指针
	to->msg_fields			= from->msg_fields;  // 拷贝字段标志位

	to->origin				= from->origin;      // 拷贝事务来源
	to->from_flags			= from->from_flags;  // 拷贝来源标志位

	to->from.any			= from->from.any;    // 拷贝来源联合体
	to->from_data.any		= from->from_data.any; // 拷贝来源数据

	to->keyd				= from->keyd;        // 拷贝摘要键

	to->start_time			= from->start_time;  // 拷贝开始时间
	to->benchmark_time		= from->benchmark_time; // 拷贝基准时间
}

/**
 * 从读写请求初始化事务对象
 *
 * 功能描述:
 * 将读写请求对象的完整信息转换为事务对象，用于在不同的执行阶段传递上下文。
 * 包含头部信息、分区保留和执行状态的完整转移。
 *
 * 参数说明:
 * @param tr - 待初始化的事务对象
 * @param rw - 源读写请求对象
 *
 * 关键逻辑:
 * 1. 首先初始化头部信息
 * 2. 拷贝分区保留(注意：原对象仍保留分区保留)
 * 3. 转移执行状态和结果信息
 * 4. 清空消息指针的所有权转移标记
 */
void
as_transaction_init_from_rw(as_transaction *tr, rw_request *rw)
{
	as_transaction_init_head_from_rw(tr, rw); // 初始化头部信息
	// 注意：不清空rw->msgp，析构函数会释放它

	as_partition_reservation_copy(&tr->rsv, &rw->rsv); // 拷贝分区保留
	// 注意：析构函数仍会释放原保留

	tr->end_time = rw->end_time;                     // 转移结束时间
	tr->result_code = rw->result_code;               // 转移结果代码
	tr->flags = rw->flags;                           // 转移标志位
	tr->generation = rw->generation;                 // 转移记录代数
	tr->void_time = rw->void_time;                   // 转移过期时间
	tr->last_update_time = rw->last_update_time;     // 转移最后更新时间
	tr->epoch_ms = 0;                                // 清零纪元毫秒数
}

/**
 * 从读写请求初始化事务头部
 *
 * 功能描述:
 * 将读写请求的头部信息转移到事务对象中，并清理源对象的部分引用以避免重复释放。
 * 这是一个所有权转移操作。
 *
 * 参数说明:
 * @param tr - 目标事务对象
 * @param rw - 源读写请求对象
 *
 * 关键逻辑:
 * 1. 逐字段转移头部信息
 * 2. 清空源对象的from.any指针以避免重复释放
 * 3. 保留消息指针但不清空，由析构函数处理
 */
void
as_transaction_init_head_from_rw(as_transaction *tr, rw_request *rw)
{
	tr->msgp				= rw->msgp;          // 转移消息指针
	tr->msg_fields			= rw->msg_fields;    // 转移字段标志位
	tr->origin				= rw->origin;        // 转移事务来源
	tr->from_flags			= rw->from_flags;    // 转移来源标志位
	tr->from.any			= rw->from.any;      // 转移来源联合体
	tr->from_data.any		= rw->from_data.any; // 转移来源数据
	tr->keyd				= rw->keyd;          // 转移摘要键
	tr->start_time			= rw->start_time;    // 转移开始时间
	tr->benchmark_time		= rw->benchmark_time; // 转移基准时间

	rw->from.any = NULL; // 清空源对象指针以避免重复释放
	// 注意：不清空rw->msgp，析构函数会释放它
}

/**
 * 设置消息字段标志位
 *
 * 功能描述:
 * 根据消息字段类型设置事务对象中对应的标志位，用于快速检查消息中包含哪些字段，
 * 避免重复解析消息内容。支持所有已知的消息字段类型。
 *
 * 参数说明:
 * @param tr   - 事务对象
 * @param type - 消息字段类型
 *
 * 返回值:
 * @return true  - 字段类型已知且标志位已设置
 * @return false - 未知的字段类型
 *
 * 关键逻辑:
 * 1. 使用switch语句处理所有支持的字段类型
 * 2. 为每种字段类型设置对应的位标志
 * 3. 批处理字段通常由父事务处理
 * 4. 未知字段类型返回false但不影响事务执行
 */
bool
as_transaction_set_msg_field_flag(as_transaction *tr, uint8_t type)
{
	switch (type) {
	case AS_MSG_FIELD_TYPE_NAMESPACE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_NAMESPACE;    // 命名空间字段
		break;
	case AS_MSG_FIELD_TYPE_SET:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SET;          // 集合名字段
		break;
	case AS_MSG_FIELD_TYPE_KEY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_KEY;          // 用户键字段
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_RIPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_RIPE;  // 成熟摘要字段
		break;
	case AS_MSG_FIELD_TYPE_TRID:
		tr->msg_fields |= AS_MSG_FIELD_BIT_TRID;         // 事务ID字段
		break;
	case AS_MSG_FIELD_TYPE_SOCKET_TIMEOUT:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SOCKET_TIMEOUT; // 套接字超时字段
		break;
	case AS_MSG_FIELD_TYPE_RECS_PER_SEC:
		tr->msg_fields |= AS_MSG_FIELD_BIT_RECS_PER_SEC; // 每秒记录数字段
		break;
	case AS_MSG_FIELD_TYPE_PID_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_PID_ARRAY;    // 分区ID数组字段
		break;
	case AS_MSG_FIELD_TYPE_DIGEST_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_DIGEST_ARRAY; // 摘要数组字段
		break;
	case AS_MSG_FIELD_TYPE_SAMPLE_MAX:
		tr->msg_fields |= AS_MSG_FIELD_BIT_SAMPLE_MAX;   // 采样上限字段
		break;
	case AS_MSG_FIELD_TYPE_LUT:
		tr->msg_fields |= AS_MSG_FIELD_BIT_LUT;          // 最后更新时间字段
		break;
	case AS_MSG_FIELD_TYPE_BVAL_ARRAY:
		tr->msg_fields |= AS_MSG_FIELD_BIT_BVAL_ARRAY;   // 二进制值数组字段
		break;
	case AS_MSG_FIELD_TYPE_INDEX_RANGE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_RANGE;  // 索引范围字段
		break;
	case AS_MSG_FIELD_TYPE_INDEX_TYPE:
		tr->msg_fields |= AS_MSG_FIELD_BIT_INDEX_TYPE;   // 索引类型字段
		break;
	case AS_MSG_FIELD_TYPE_UDF_FILENAME:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FILENAME; // UDF文件名字段
		break;
	case AS_MSG_FIELD_TYPE_UDF_FUNCTION:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_FUNCTION; // UDF函数名字段
		break;
	case AS_MSG_FIELD_TYPE_UDF_ARGLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_ARGLIST;  // UDF参数列表字段
		break;
	case AS_MSG_FIELD_TYPE_UDF_OP:
		tr->msg_fields |= AS_MSG_FIELD_BIT_UDF_OP;       // UDF操作字段
		break;
	case AS_MSG_FIELD_TYPE_QUERY_BINLIST:
		tr->msg_fields |= AS_MSG_FIELD_BIT_QUERY_BINLIST; // 查询Bin列表字段
		break;
	case AS_MSG_FIELD_TYPE_BATCH: // 不应该到这里 - 批处理父事务处理此字段
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH;        // 批处理字段
		break;
	case AS_MSG_FIELD_TYPE_BATCH_WITH_SET: // 不应该到这里 - 批处理父事务处理此字段
		tr->msg_fields |= AS_MSG_FIELD_BIT_BATCH_WITH_SET; // 带集合的批处理字段
		break;
	case AS_MSG_FIELD_TYPE_PREDEXP:
		tr->msg_fields |= AS_MSG_FIELD_BIT_PREDEXP;      // 谓词表达式字段
		break;
	default:
		return false; // 未知字段类型
	}

	return true; // 成功设置标志位
}

/**
 * 准备和验证事务消息
 *
 * 功能描述:
 * 解析并验证客户端发送的消息格式，包括字段和操作的完整性检查。
 * 如果需要，执行字节序转换。这是消息处理的关键验证步骤。
 *
 * 参数说明:
 * @param tr   - 事务对象
 * @param swap - 是否需要进行字节序转换
 *
 * 返回值:
 * @return true  - 消息格式正确，准备成功
 * @return false - 消息格式错误或验证失败
 *
 * 关键逻辑:
 * 1. 验证协议大小是否足够容纳基本消息结构
 * 2. 解析和验证所有消息字段
 * 3. 解析和验证所有Bin操作
 * 4. 设置对应的字段标志位以优化后续访问
 * 5. 确保消息末尾没有多余数据
 */
bool
as_transaction_prepare(as_transaction *tr, bool swap)
{
	uint64_t size = tr->msgp->proto.sz;

	// 验证协议大小是否足够容纳基本as_msg结构
	if (size < sizeof(as_msg)) {
		cf_warning(AS_PROTO, "proto body size %lu smaller than as_msg", size);
		return false;
	}

	// 协议数据不小于as_msg - 可以安全地转换头部
	as_msg *m = &tr->msgp->msg;

	// 如果需要，进行字节序转换
	if (swap) {
		as_msg_swap_header(m);
	}

	uint8_t* p_end = (uint8_t*)m + size;   // 消息结束位置
	uint8_t* p_read = m->data;             // 当前读取位置

	// 首先解析和转换字段
	for (uint16_t n = 0; n < m->n_fields; n++) {
		// 检查是否有足够空间容纳字段头
		if (p_read + sizeof(as_msg_field) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg_field");
			return false;
		}

		as_msg_field* p_field = (as_msg_field*)p_read;

		// 如果需要，转换字段字节序
		if (swap) {
			as_msg_swap_field(p_field);
		}

		// 跳过当前字段，检查格式是否正确
		if (! (p_read = as_msg_field_skip(p_field))) {
			cf_warning(AS_PROTO, "bad as_msg_field");
			return false;
		}

		// 存储消息中存在的字段类型 - 避免大量重复解析
		if (! as_transaction_set_msg_field_flag(tr, p_field->type)) {
			cf_debug(AS_PROTO, "skipping as_msg_field type %u", p_field->type);
		}
	}

	// 解析和转换Bin操作(如果有的话)
	for (uint16_t n = 0; n < m->n_ops; n++) {
		// 检查是否有足够空间容纳操作头
		if (p_read + sizeof(as_msg_op) > p_end) {
			cf_warning(AS_PROTO, "incomplete as_msg");
			return false;
		}

		as_msg_op* op = (as_msg_op*)p_read;

		if (swap) {
			// 字节序转换可能涉及as_msg_op结构之外的元数据字节
			if (as_msg_op_get_value_p(op) > p_end) {
				cf_warning(AS_PROTO, "bad as_msg_op");
				return false;
			}

			as_msg_swap_op(op);
		}

		// 跳过当前操作，检查格式是否正确
		if (! (p_read = as_msg_op_skip(op))) {
			cf_warning(AS_PROTO, "bad as_msg_op");
			return false;
		}
	}

	// 确保消息末尾没有多余字节
	if (p_read != p_end) {
		cf_warning(AS_PROTO, "extra bytes follow fields and bin-ops");
		return false;
	}

	return true; // 消息准备成功
}

/**
 * 初始化内部UDF事务(用于UDF查询)
 *
 * 功能描述:
 * 为UDF查询创建内部事务对象。使用共享消息，包含命名空间但没有摘要键，
 * 暂时不设置集合名，因为这些事务不会进行安全检查，也不能创建记录。
 *
 * 参数说明:
 * @param tr        - 待初始化的事务对象
 * @param ns        - 目标命名空间
 * @param keyd      - 记录摘要键
 * @param iudf_orig - 内部UDF来源对象
 *
 * 关键逻辑:
 * 1. 在事务入队之前将摘要放在事务头部
 * 2. 设置命名空间字段标志以指示消息内容
 * 3. 标记事务来源为内部UDF
 * 4. 记录事务开始时间用于性能分析
 */
void
as_transaction_init_iudf(as_transaction *tr, as_namespace *ns, cf_digest *keyd,
		iudf_origin* iudf_orig)
{
	// 注意：摘要在事务入队之前就在事务头部了
	as_transaction_init_head(tr, keyd, iudf_orig->msgp);

	// 设置命名空间字段标志位
	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_NAMESPACE);

	tr->origin = FROM_IUDF;           // 设置来源为内部UDF
	tr->from.iudf_orig = iudf_orig;   // 设置UDF来源对象

	tr->start_time = cf_getns();      // 记录开始时间(纳秒)
}

/**
 * 初始化内部操作事务(用于操作查询)
 *
 * 功能描述:
 * 为操作查询创建内部事务对象。使用共享消息，包含命名空间但没有摘要键，
 * 暂时不设置集合名，因为这些事务不会进行安全检查，也不能创建记录。
 *
 * 参数说明:
 * @param tr        - 待初始化的事务对象
 * @param ns        - 目标命名空间
 * @param keyd      - 记录摘要键
 * @param iops_orig - 内部操作来源对象
 *
 * 关键逻辑:
 * 1. 在事务入队之前将摘要放在事务头部
 * 2. 设置命名空间字段标志以指示消息内容
 * 3. 标记事务来源为内部操作
 * 4. 记录事务开始时间用于性能分析
 */
void
as_transaction_init_iops(as_transaction *tr, as_namespace *ns, cf_digest *keyd,
		iops_origin* iops_orig)
{
	// 注意：摘要在事务入队之前就在事务头部了
	as_transaction_init_head(tr, keyd, iops_orig->msgp);

	// 设置命名空间字段标志位
	as_transaction_set_msg_field_flag(tr, AS_MSG_FIELD_TYPE_NAMESPACE);

	tr->origin = FROM_IOPS;           // 设置来源为内部操作
	tr->from.iops_orig = iops_orig;   // 设置操作来源对象

	tr->start_time = cf_getns();      // 记录开始时间(纳秒)
}

/**
 * 处理事务反序列化错误
 *
 * 功能描述:
 * 当消息反序列化失败时，向客户端发送错误响应并清理资源。
 * 这通常发生在消息格式不正确或协议版本不匹配时。
 *
 * 参数说明:
 * @param tr         - 事务对象
 * @param error_code - 错误代码
 *
 * 关键逻辑:
 * 1. 向客户端发送错误响应
 * 2. 清空文件句柄指针防止重复使用
 * 3. 释放消息内存
 * 4. 更新反序列化错误统计
 */
void
as_transaction_demarshal_error(as_transaction* tr, uint32_t error_code)
{
	// 向客户端发送错误响应
	as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, 0);
	tr->from.proto_fd_h = NULL; // 清空句柄指针

	// 释放消息内存
	cf_free(tr->msgp);
	tr->msgp = NULL;

	// 更新全局反序列化错误统计
	as_incr_uint64(&g_stats.n_demarshal_error);
}

/**
 * 更新错误统计宏
 *
 * 功能描述:
 * 根据命名空间是否存在，更新相应的错误统计计数器。
 * 区分超时错误和一般错误，支持命名空间级别和全局级别的统计。
 *
 * 参数说明:
 * @param name - 操作类型名称(如client、proxy、batch_sub等)
 *
 * 逻辑说明:
 * - 如果有命名空间：更新命名空间级别的统计
 * - 如果没有命名空间：更新全局级别的统计
 * - 超时错误和一般错误分别统计
 */
#define UPDATE_ERROR_STATS(name) \
	if (ns) { \
		if (error_code == AS_ERR_TIMEOUT) { \
			as_incr_uint64(&ns->n_##name##_tsvc_timeout); \
		} \
		else { \
			as_incr_uint64(&ns->n_##name##_tsvc_error); \
		} \
	} \
	else { \
		as_incr_uint64(&g_stats.n_tsvc_##name##_error); \
	}

/**
 * 处理事务执行错误
 *
 * 功能描述:
 * 根据事务来源类型，向相应的客户端发送错误响应并更新错误统计。
 * 支持所有事务来源类型的错误处理和统计更新。
 *
 * 参数说明:
 * @param tr         - 事务对象
 * @param ns         - 命名空间(可能为NULL)
 * @param error_code - 错误代码
 *
 * 关键逻辑:
 * 1. 确保错误代码不为0(转换为未知错误)
 * 2. 根据事务来源发送相应的错误响应
 * 3. 更新对应的错误统计计数器
 * 4. 清空相关指针防止重复使用
 */
void
as_transaction_error(as_transaction* tr, as_namespace* ns, uint32_t error_code)
{
	// 确保错误代码不为0
	if (error_code == 0) {
		cf_warning(AS_PROTO, "converting error code 0 to 1 (unknown)");
		error_code = AS_ERR_UNKNOWN;
	}

	// 下面的'from'检查是不必要的，只是为了安全起见
	switch (tr->origin) {
	case FROM_CLIENT: // 来自客户端的事务
		if (tr->from.proto_fd_h) {
			// 向客户端发送错误响应
			as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proto_fd_h = NULL; // 模式，实际上不需要
		}
		UPDATE_ERROR_STATS(client); // 更新客户端错误统计
		break;
	case FROM_PROXY: // 来自代理节点的事务
		if (tr->from.proxy_node != 0) {
			// 向代理节点发送错误响应
			as_proxy_send_response(tr->from.proxy_node, tr->from_data.proxy_tid, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proxy_node = 0; // 模式，实际上不需要
		}
		if (as_transaction_is_batch_sub(tr)) {
			UPDATE_ERROR_STATS(from_proxy_batch_sub); // 更新代理批处理子事务错误统计
		}
		else {
			UPDATE_ERROR_STATS(from_proxy); // 更新代理错误统计
		}
		break;
	case FROM_BATCH: // 来自批处理的事务
		if (tr->from.batch_shared) {
			// 向批处理添加错误
			as_batch_add_error(tr->from.batch_shared, tr->from_data.batch_index, error_code);
			tr->from.batch_shared = NULL; // 模式，实际上不需要
			tr->msgp = NULL; // 模式，实际上不需要
		}
		UPDATE_ERROR_STATS(batch_sub); // 更新批处理子事务错误统计
		break;
	case FROM_IUDF: // 来自内部UDF的事务
		if (tr->from.iudf_orig) {
			// 调用UDF完成回调
			tr->from.iudf_orig->done_cb(tr->from.iudf_orig->udata, error_code);
			tr->from.iudf_orig = NULL; // 模式，实际上不需要
		}
		UPDATE_ERROR_STATS(udf_sub); // 更新UDF子事务错误统计
		break;
	case FROM_IOPS: // 来自内部操作的事务
		if (tr->from.iops_orig) {
			// 调用操作完成回调
			tr->from.iops_orig->done_cb(tr->from.iops_orig->udata, error_code);
			tr->from.iops_orig = NULL; // 模式，实际上不需要
		}
		UPDATE_ERROR_STATS(ops_sub); // 更新操作子事务错误统计
		break;
	case FROM_READ_TOUCH: // 来自读取触摸的事务
		tr->from.read_touch_active = NULL; // 模式，实际上不需要
		UPDATE_ERROR_STATS(read_touch); // 更新读取触摸错误统计
		break;
	case FROM_RE_REPL: // 来自重复复制的事务
		if (tr->from.re_repl_orig_cb) {
			// 调用重复复制原始回调
			tr->from.re_repl_orig_cb(tr);
			tr->from.re_repl_orig_cb = NULL; // 模式，实际上不需要
		}
		UPDATE_ERROR_STATS(re_repl); // 更新重复复制错误统计
		break;
	default:
		cf_crash(AS_PROTO, "unexpected transaction origin %u", tr->origin);
		break;
	}
}

/**
 * 处理多记录事务错误(临时函数)
 *
 * 功能描述:
 * 临时函数，直到查询可以执行自己的同步失败响应。
 * 这里我们放弃命名空间信息并添加到全局范围错误中。
 *
 * 参数说明:
 * @param tr         - 事务对象
 * @param error_code - 错误代码
 *
 * 注意事项:
 * 此函数目前仅支持来自客户端的事务，其他来源会导致崩溃。
 * 错误统计只更新全局计数器，不按命名空间分类。
 */
// TODO - 临时函数，直到查询可以执行自己的同步失败响应
// (这里我们放弃命名空间信息并添加到全局范围错误中)
void
as_multi_rec_transaction_error(as_transaction* tr, uint32_t error_code)
{
	// 确保错误代码不为0
	if (error_code == 0) {
		cf_warning(AS_PROTO, "converting error code 0 to 1 (unknown)");
		error_code = AS_ERR_UNKNOWN;
	}

	switch (tr->origin) {
	case FROM_CLIENT: // 仅支持来自客户端的事务
		if (tr->from.proto_fd_h) {
			// 向客户端发送错误响应
			as_msg_send_reply(tr->from.proto_fd_h, error_code, 0, 0, NULL, NULL, 0, NULL, as_transaction_trid(tr));
			tr->from.proto_fd_h = NULL; // 模式，实际上不需要
		}
		// 更新全局客户端错误统计
		as_incr_uint64(&g_stats.n_tsvc_client_error);
		break;
	default:
		cf_crash(AS_PROTO, "unexpected transaction origin %u", tr->origin);
		break;
	}
}

/**
 * 释放事务文件句柄的帮助函数
 *
 * 功能描述:
 * 释放文件句柄相关的所有资源，包括套接字、协议缓冲区和安全过滤器。
 * 使用引用计数确保安全释放。
 *
 * 参数说明:
 * @param proto_fd_h - 文件句柄指针
 *
 * 关键逻辑:
 * 1. 减少引用计数，如果不为0则直接返回
 * 2. 关闭和终止套接字连接
 * 3. 释放协议缓冲区内存
 * 4. 销毁安全过滤器
 * 5. 释放句柄对象并更新统计
 */
// 释放事务文件句柄的帮助函数
void
as_release_file_handle(as_file_handle *proto_fd_h)
{
	// 减少引用计数，如果还有其他引用则返回
	if (cf_rc_release(proto_fd_h) != 0) {
		return;
	}

	// 关闭套接字连接
	cf_socket_close(&proto_fd_h->sock);
	cf_socket_term(&proto_fd_h->sock);

	// 释放协议缓冲区
	if (proto_fd_h->proto != NULL) {
		cf_free(proto_fd_h->proto);
	}

	// 销毁安全过滤器
	if (proto_fd_h->security_filter != NULL) {
		as_security_filter_destroy(proto_fd_h->security_filter);
	}

	// 释放文件句柄对象
	cf_rc_free(proto_fd_h);

	// 更新协议连接关闭统计
	as_incr_uint64_rls(&g_stats.proto_connections_closed);
}

/**
 * 结束事务处理
 *
 * 功能描述:
 * 完成事务处理，清理资源并准备文件句柄供下次使用。
 * 根据需要可以强制关闭连接。
 *
 * 参数说明:
 * @param proto_fd_h  - 文件句柄指针
 * @param force_close - 是否强制关闭连接
 *
 * 关键逻辑:
 * 1. 如果需要，强制关闭套接字连接
 * 2. 重新武装文件句柄以接收下一个请求
 * 3. 减少事务计数器，允许服务线程退出和关闭epoll实例
 */
void
as_end_of_transaction(as_file_handle *proto_fd_h, bool force_close)
{
	// 如果需要，强制关闭套接字
	if (force_close) {
		cf_socket_shutdown(&proto_fd_h->sock);
	}

	// 首先重新武装文件句柄
	as_service_rearm(proto_fd_h);

	// 现在允许服务线程退出和关闭epoll实例
	as_decr_uint32_rls(&proto_fd_h->in_transaction);
}

/**
 * 正常结束事务
 *
 * 功能描述:
 * 正常结束事务处理，不强制关闭连接，允许连接复用。
 *
 * 参数说明:
 * @param proto_fd_h - 文件句柄指针
 */
void
as_end_of_transaction_ok(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, false); // 不强制关闭连接
}

/**
 * 强制关闭连接并结束事务
 *
 * 功能描述:
 * 结束事务处理并强制关闭客户端连接，通常用于错误情况。
 *
 * 参数说明:
 * @param proto_fd_h - 文件句柄指针
 */
void
as_end_of_transaction_force_close(as_file_handle *proto_fd_h)
{
	as_end_of_transaction(proto_fd_h, true); // 强制关闭连接
}
