/*
 * rw_request.c - 读写请求管理模块
 *
 * 功能描述：
 * - 管理Aerospike数据库的读写请求对象的生命周期
 * - 实现读写请求的创建、销毁和状态管理
 * - 提供请求等待队列机制，处理并发访问控制
 * - 支持事务请求的复制和重试逻辑
 * - 管理请求的超时和回调处理
 * - 协调分区预留和资源清理
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
 */

//==========================================================
// Includes.
//

#include "transaction/rw_request.h"

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/proto.h"
#include "base/service.h"
#include "base/transaction.h"
#include "fabric/fabric.h"
#include "fabric/partition.h"


//==========================================================
// Typedefs & constants.
// 类型定义和常量
//

// 读写请求等待队列元素结构
// 用于存储等待处理的事务请求
typedef struct rw_wait_ele_s {
	uint8_t tr_head[AS_TRANSACTION_HEAD_SIZE];  // 事务头部数据，包含事务基本信息
	struct rw_wait_ele_s* next;                 // 指向下一个等待元素的指针，形成链表结构
} rw_wait_ele;


//==========================================================
// Globals.
// 全局变量
//

// 全局读写请求事务ID计数器
// 用于为每个读写请求分配唯一的事务ID
static uint32_t g_rw_tid = 0;


//==========================================================
// Public API.
// 公共API接口
//

/**
 * 创建读写请求对象
 *
 * 功能描述：
 * - 分配并初始化新的读写请求对象
 * - 设置默认的事务状态和参数
 * - 初始化等待队列和锁机制
 * - 分配唯一的事务ID
 * - 准备复制和重复解决相关字段
 *
 * 参数：
 * @param keyd - 记录的唯一标识摘要
 *
 * 返回值：
 * @return 新创建的读写请求对象指针，使用引用计数管理内存
 *
 * 关键逻辑步骤：
 * 1. 使用引用计数分配内存，确保线程安全
 * 2. 初始化as_transaction兼容的字段
 * 3. 设置分区预留信息
 * 4. 初始化互斥锁和等待队列
 * 5. 准备响应缓冲区和复制状态
 * 6. 分配全局唯一的事务ID
 */
rw_request*
rw_request_create(cf_digest* keyd)
{
	// 使用引用计数分配读写请求对象内存
	rw_request* rw = cf_rc_alloc(sizeof(rw_request));

	// 初始化as_transaction兼容字段，保持接口一致性
	rw->msgp				= NULL;		// 消息指针，初始为空
	rw->msg_fields			= 0;		// 消息字段标志位
	rw->origin				= 0;		// 请求来源类型
	rw->from_flags			= 0;		// 来源标志位
	rw->from.any			= NULL;		// 来源信息，可以是客户端或其他节点
	rw->from_data.any		= 0;		// 来源数据
	rw->keyd				= *keyd;	// 复制记录键的摘要值
	rw->start_time			= 0;		// 请求开始时间戳
	rw->benchmark_time		= 0;		// 基准测试时间

	// 初始化分区预留信息，确保操作的数据分区有效
	AS_PARTITION_RESERVATION_INIT(rw->rsv);

	rw->end_time			= 0;		// 请求结束时间戳
	rw->result_code			= AS_OK;	// 操作结果代码，初始为成功
	rw->flags				= 0;		// 请求标志位
	rw->generation			= 0;		// 记录版本号
	rw->void_time			= 0;		// 记录失效时间
	rw->last_update_time	= 0;		// 最后更新时间
	// as_transaction兼容字段结束

	// 初始化请求锁，用于并发访问控制
	cf_mutex_init(&rw->lock);

	// 初始化等待队列，用于管理等待此请求完成的其他事务
	rw->wait_queue_head = NULL;		// 等待队列头指针
	rw->wait_queue_tail = NULL;		// 等待队列尾指针
	rw->wait_queue_depth = 0;		// 等待队列深度

	// 请求设置状态标志
	rw->is_set_up = false;			// 标记请求是否已完全设置

	// 记录序列化相关字段
	rw->pickle = NULL;				// 序列化数据缓冲区
	rw->pickle_sz = 0;				// 序列化数据大小
	rw->set_name = NULL;			// 集合名称
	rw->set_name_len = 0;			// 集合名称长度
	rw->key = NULL;					// 用户键数据
	rw->key_size = 0;				// 用户键大小

	// 初始化响应动态缓冲区
	rw->response_db.buf = NULL;		// 响应缓冲区指针
	rw->response_db.is_stack = false;	// 是否使用栈内存
	rw->response_db.alloc_sz = 0;	// 分配的缓冲区大小
	rw->response_db.used_sz = 0;	// 已使用的缓冲区大小

	// 分配全局唯一的事务ID，使用原子操作确保线程安全
	rw->tid = as_faa_uint32(&g_rw_tid, 1);

	// 初始化复制和重复解决状态
	rw->dup_res_complete = false;	// 重复解决是否完成
	rw->repl_write_complete = false;	// 复制写入是否完成
	rw->repl_ping_complete = false;	// 复制ping是否完成

	// 初始化回调函数指针
	rw->dup_res_cb = NULL;			// 重复解决回调
	rw->repl_write_cb = NULL;		// 复制写入回调
	rw->repl_ping_cb = NULL;		// 复制ping回调
	rw->timeout_cb = NULL;			// 超时回调

	// 初始化目标消息和重试参数
	rw->dest_msg = NULL;			// 目标消息
	rw->xmit_ms = 0;				// 传输时间戳(毫秒)
	rw->retry_interval_ms = 0;		// 重试间隔(毫秒)

	// 目标节点数量
	rw->n_dest_nodes = 0;

	// 最佳重复消息相关字段，用于冲突解决
	rw->best_dup_msg = NULL;		// 最佳重复消息
	rw->best_dup_result_code = AS_OK;	// 最佳重复结果代码
	rw->best_dup_gen = 0;			// 最佳重复版本号
	rw->best_dup_lut = 0;			// 最佳重复最后更新时间

	// 平局复制标志
	rw->tie_was_replicated = false;

	// 复制开始时间戳(微秒)
	rw->repl_start_us = 0;

	return rw;
}

/**
 * 销毁读写请求对象
 *
 * 功能描述：
 * - 安全地清理和释放读写请求对象的所有资源
 * - 检查并释放所有分配的内存
 * - 清理等待队列中的所有事务
 * - 释放分区预留和消息资源
 * - 销毁互斥锁
 *
 * 参数：
 * @param rw - 待销毁的读写请求对象
 *
 * 关键逻辑步骤：
 * 1. 验证from指针为空(安全性检查)
 * 2. 释放消息指针(如果不是共享)
 * 3. 释放序列化数据和用户键
 * 4. 释放响应缓冲区
 * 5. 释放网络消息资源
 * 6. 释放分区预留和重复消息
 * 7. 销毁互斥锁
 * 8. 处理等待队列中的所有事务
 */
void
rw_request_destroy(rw_request* rw)
{
	// 安全性检查：确保from指针已被正确清理
	// 如果不为空，说明存在资源泄漏或状态错误
	if (rw->from.any) {
		cf_crash(AS_RW, "rw_request_destroy: origin %d has non-null 'from'",
				rw->origin);
	}

	// 释放消息指针内存(如果不是共享消息)
	if (rw->msgp != NULL && ! SHARED_MSGP(rw)) {
		cf_free(rw->msgp);
	}

	// 释放序列化数据缓冲区
	if (rw->pickle) {
		cf_free(rw->pickle);
	}

	// 释放用户键数据
	if (rw->key) {
		cf_free(rw->key);
	}

	// 释放响应动态缓冲区
	cf_dyn_buf_free(&rw->response_db);

	// 释放目标消息(fabric消息使用引用计数)
	if (rw->dest_msg) {
		as_fabric_msg_put(rw->dest_msg);
	}

	// 如果请求已完全设置，需要额外清理
	if (rw->is_set_up) {
		// 释放最佳重复消息
		if (rw->best_dup_msg) {
			as_fabric_msg_put(rw->best_dup_msg);
		}

		// 释放分区预留资源
		as_partition_release(&rw->rsv);
	}

	// 销毁互斥锁
	cf_mutex_destroy(&rw->lock);

	// 处理等待队列中的所有事务
	// 遍历等待队列，重新提交所有等待的事务
	rw_wait_ele* e = rw->wait_queue_head;

	while (e) {
		rw_wait_ele* next = e->next;				// 保存下一个元素
		as_transaction* tr = (as_transaction*)e->tr_head;	// 获取事务指针

		// 设置重启标志，表示事务需要重新处理
		tr->from_flags |= FROM_FLAG_RESTART;
		// 重新将事务加入服务队列
		as_service_enqueue_internal(tr);

		// 释放等待元素内存
		cf_free(e);
		e = next;
	}
}

/**
 * 将事务加入读写请求的等待队列尾部
 *
 * 功能描述：
 * - 创建等待队列元素，保存事务头部信息
 * - 将新元素添加到等待队列的尾部
 * - 清理原事务的资源指针，避免重复释放
 * - 更新等待队列深度统计
 *
 * 参数：
 * @param rw - 读写请求对象
 * @param tr - 需要等待的事务对象
 *
 * 关键逻辑步骤：
 * 1. 分配新的等待队列元素
 * 2. 复制事务头部信息到元素中
 * 3. 清理原事务指针避免冲突
 * 4. 将元素添加到队列尾部
 * 5. 更新队列统计信息
 */
void
rw_request_wait_q_push(rw_request* rw, as_transaction* tr)
{
	// 分配新的等待队列元素
	rw_wait_ele* e = cf_malloc(sizeof(rw_wait_ele));

	// 复制事务头部信息到等待元素中
	// 这样可以保存事务的关键信息而不需要保持整个事务对象
	as_transaction_copy_head((as_transaction*)e->tr_head, tr);

	// 清理原事务的资源指针，避免在销毁时重复释放
	tr->from.any = NULL;		// 清理来源指针
	tr->msgp = NULL;			// 清理消息指针

	// 初始化链表指针
	e->next = NULL;

	// 将元素添加到等待队列尾部
	if (rw->wait_queue_tail) {
		// 队列非空，添加到尾部
		rw->wait_queue_tail->next = e;
		rw->wait_queue_tail = e;
	}
	else {
		// 队列为空，设置为头尾元素
		rw->wait_queue_head = e;
		rw->wait_queue_tail = e;
	}

	// 增加等待队列深度计数
	rw->wait_queue_depth++;
}

/**
 * 将事务加入读写请求的等待队列头部
 *
 * 功能描述：
 * - 创建等待队列元素，保存事务头部信息
 * - 将新元素插入到等待队列的头部(高优先级处理)
 * - 清理原事务的资源指针，避免重复释放
 * - 更新等待队列深度统计
 * - 适用于需要优先处理的事务(如重试事务)
 *
 * 参数：
 * @param rw - 读写请求对象
 * @param tr - 需要等待的事务对象
 *
 * 关键逻辑步骤：
 * 1. 分配新的等待队列元素
 * 2. 复制事务头部信息到元素中
 * 3. 清理原事务指针避免冲突
 * 4. 将元素插入到队列头部
 * 5. 更新队列头尾指针和统计信息
 */
void
rw_request_wait_q_push_head(rw_request* rw, as_transaction* tr)
{
	// 分配新的等待队列元素
	rw_wait_ele* e = cf_malloc(sizeof(rw_wait_ele));
	// 验证内存分配成功
	cf_assert(e, AS_RW, "alloc rw_wait_ele");

	// 复制事务头部信息到等待元素中
	// 保存事务的关键状态信息，用于后续恢复
	as_transaction_copy_head((as_transaction*)e->tr_head, tr);

	// 清理原事务的资源指针，避免在销毁时重复释放
	tr->from.any = NULL;		// 清理来源指针
	tr->msgp = NULL;			// 清理消息指针

	// 将新元素插入到队列头部
	e->next = rw->wait_queue_head;		// 新元素指向原头部
	rw->wait_queue_head = e;			// 更新队列头指针

	// 如果队列原本为空，需要设置尾指针
	if (! rw->wait_queue_tail) {
		rw->wait_queue_tail = e;
	}

	// 增加等待队列深度计数
	rw->wait_queue_depth++;
}
