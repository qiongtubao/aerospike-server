/*
 * storage.c - Aerospike 存储引擎抽象层
 *
 * 模块职责：
 * - 提供统一的存储引擎抽象接口，屏蔽不同存储引擎（内存、PMEM、SSD）的实现差异
 * - 通过虚函数表（v-table）机制实现多态调用，支持运行时存储引擎切换
 * - 管理存储引擎的生命周期：初始化、加载、激活、关闭等阶段
 * - 协调记录的 CRUD 操作，确保数据一致性和完整性
 *
 * 存储抽象层设计：
 * - 采用策略模式，将存储操作抽象为统一接口
 * - 每个存储引擎实现自己的函数表，由抽象层根据namespace类型动态调用
 * - 支持混合存储：不同namespace可以使用不同的存储引擎
 *
 * IO 路径：
 * - 写入路径：应用层 -> 存储抽象层 -> 具体存储引擎 -> 物理设备/内存
 * - 读取路径：应用层 -> 存储抽象层 -> 具体存储引擎 -> 返回数据
 * - 缓存策略：支持多级缓存，提升读写性能
 *
 * Copyright (C) 2009-2023 Aerospike, Inc.
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

#include "storage/storage.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_queue.h"

#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "sindex/sindex.h"


//==========================================================
// 全局变量
//

// 全局唯一数据大小统计，用于社区版限制检查
uint64_t g_unique_data_size = 0;


//==========================================================
// 通用"基类"函数，通过存储引擎"虚函数表"进行调用
// 实现了多态机制，根据namespace的storage_type动态调用对应的存储引擎函数
//

//--------------------------------------
// as_storage_init - 存储引擎初始化
// 功能：初始化所有namespace的存储引擎，包括热重启时恢复索引
//

// 存储引擎初始化函数指针类型定义
typedef void (*as_storage_init_fn)(as_namespace* ns);

// 存储引擎初始化函数表：根据storage_type索引调用对应的初始化函数
// 0: 内存存储引擎, 1: PMEM存储引擎, 2: SSD存储引擎
static const as_storage_init_fn as_storage_init_table[] = {
	as_storage_init_mem,    // 内存存储引擎初始化
	as_storage_init_pmem,   // 持久内存存储引擎初始化
	as_storage_init_ssd     // SSD存储引擎初始化
};

/**
 * 初始化所有namespace的存储引擎
 * 功能描述：
 * 1. 遍历所有配置的namespace
 * 2. 根据每个namespace的storage_type调用对应的存储引擎初始化函数
 * 3. 检查社区版数据大小限制
 *
 * 关键逻辑：
 * - 支持热重启时的索引恢复
 * - 社区版限制检查，防止超过许可限制
 */
void
as_storage_init(void)
{
	// 包括热重启时恢复索引

	// 遍历所有namespace，初始化对应的存储引擎
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// 根据namespace的存储类型调用对应的初始化函数
		as_storage_init_table[ns->storage_type](ns);
	}

	// 检查社区版数据大小限制
	if (AS_NODE_STORAGE_SZ != 0 && g_unique_data_size > AS_NODE_STORAGE_SZ) {
		cf_crash_nostack(AS_STORAGE, "Community Edition limit exceeded");
	}
}

//--------------------------------------
// as_storage_load - 存储引擎数据加载
// 功能：加载存储设备上的数据，包括冷启动时的设备扫描
//

// 存储引擎加载函数指针类型定义
typedef void (*as_storage_load_fn)(as_namespace* ns, cf_queue* complete_q);
static const as_storage_load_fn as_storage_load_table[] = {
	as_storage_load_mem,    // 内存存储引擎加载
	as_storage_load_pmem,   // PMEM存储引擎加载
	as_storage_load_ssd     // SSD存储引擎加载
};

// 存储引擎加载进度ticker函数指针类型定义
typedef void (*as_storage_load_ticker_fn)(const as_namespace* ns);
static const as_storage_load_ticker_fn as_storage_load_ticker_table[] = {
	as_storage_load_ticker_mem,    // 内存存储引擎进度显示
	as_storage_load_ticker_pmem,   // PMEM存储引擎进度显示
	as_storage_load_ticker_ssd     // SSD存储引擎进度显示
};

#define TICKER_INTERVAL (5 * 1000) // 进度显示间隔：5秒

/**
 * 加载所有namespace的存储数据
 * 功能描述：
 * 1. 启动各namespace的数据加载线程
 * 2. 等待所有加载完成，期间显示加载进度
 * 3. 支持冷启动时的设备扫描和索引重建
 *
 * 参数说明：
 * 无参数
 *
 * 关键逻辑：
 * - 异步启动所有namespace的加载
 * - 使用完成队列等待所有加载任务完成
 * - 定期显示加载进度，特别是冷启动时可能耗时较长
 */
void
as_storage_load(void)
{
	// 包括冷启动时的设备扫描

	cf_queue complete_q;

	// 初始化完成队列，用于等待所有namespace加载完成
	cf_queue_init(&complete_q, sizeof(void*), g_config.n_namespaces, true);

	// 启动所有namespace的数据加载
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// 异步启动加载，完成后会向complete_q发送通知
		as_storage_load_table[ns->storage_type](ns, &complete_q);
	}

	// 等待完成 - 冷启动可能需要较长时间

	for (uint32_t n_done = 0; n_done < g_config.n_namespaces; n_done++) {
		void* _t;

		// 等待完成通知，超时则显示进度
		while (cf_queue_pop(&complete_q, &_t, TICKER_INTERVAL) != CF_QUEUE_OK) {
			// 遍历所有namespace，显示正在加载的进度
			for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
				as_namespace* ns = g_config.namespaces[ns_ix];

				// 如果namespace仍在加载记录，则显示进度
				if (ns->loading_records) {
					as_storage_load_ticker_table[ns->storage_type](ns);
				}
			}
		}
	}

	// 清理完成队列
	cf_queue_destroy(&complete_q);
}

//--------------------------------------
// as_storage_activate - 存储引擎激活
// 功能：激活所有存储引擎，等待碎片整理完成后开始正常服务
//

// 存储引擎激活函数指针类型定义
typedef void (*as_storage_activate_fn)(as_namespace* ns);
static const as_storage_activate_fn as_storage_activate_table[] = {
	as_storage_activate_mem,    // 内存存储引擎激活
	as_storage_activate_pmem,   // PMEM存储引擎激活
	as_storage_activate_ssd     // SSD存储引擎激活
};

// 碎片整理等待函数指针类型定义
typedef bool (*as_storage_wait_for_defrag_fn)(as_namespace* ns);
static const as_storage_wait_for_defrag_fn as_storage_wait_for_defrag_table[] = {
	as_storage_wait_for_defrag_mem,    // 内存存储引擎碎片整理等待
	as_storage_wait_for_defrag_pmem,   // PMEM存储引擎碎片整理等待
	as_storage_wait_for_defrag_ssd     // SSD存储引擎碎片整理等待
};

/**
 * 激活所有namespace的存储引擎
 * 功能描述：
 * 1. 调用各存储引擎的激活函数
 * 2. 等待所有namespace的碎片整理达到要求水平
 * 3. 确保存储引擎准备好接受正常读写请求
 *
 * 参数说明：
 * 无参数
 *
 * 关键逻辑：
 * - 先激活所有存储引擎
 * - 循环等待碎片整理完成，避免启动时性能问题
 * - 所有namespace都达到要求后才继续
 */
void
as_storage_activate(void)
{
	// 激活所有namespace的存储引擎
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// 调用对应存储引擎的激活函数
		as_storage_activate_table[ns->storage_type](ns);
	}

	// 等待碎片整理达到要求水平
	while (true) {
		bool any_defragging = false;

		// 检查所有namespace的碎片整理状态
		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			as_namespace* ns = g_config.namespaces[ns_ix];

			// 如果namespace仍需要碎片整理，标记为正在整理
			if (as_storage_wait_for_defrag_table[ns->storage_type](ns)) {
				any_defragging = true;
			}
		}

		// 如果所有namespace都完成了碎片整理，退出等待
		if (! any_defragging) {
			break;
		}

		// 等待3秒后再次检查
		sleep(3);
	}
}

//--------------------------------------
// as_storage_start_tomb_raider
//

typedef void (*as_storage_start_tomb_raider_fn)(as_namespace* ns);
static const as_storage_start_tomb_raider_fn as_storage_start_tomb_raider_table[] = {
	as_storage_start_tomb_raider_mem,
	as_storage_start_tomb_raider_pmem,
	as_storage_start_tomb_raider_ssd
};

void
as_storage_start_tomb_raider(void)
{
	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		as_storage_start_tomb_raider_table[ns->storage_type](ns);
	}
}

//--------------------------------------
// as_storage_shutdown
//

typedef void (*as_storage_shutdown_fn)(as_namespace* ns);
static const as_storage_shutdown_fn as_storage_shutdown_table[] = {
	as_storage_shutdown_mem,
	as_storage_shutdown_pmem,
	as_storage_shutdown_ssd
};

bool
as_storage_shutdown(uint32_t instance)
{
	bool all_ok = true;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		as_namespace* ns = g_config.namespaces[ns_ix];

		// Lock all record locks - ensure each operation's record lock scope is
		// either completed or never entered.
		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_tree_shutdown(ns, pid);
		}

		// Lock all partition locks - ensure partition info in device header is
		// not changing (migrations may still drop trees). Separate loop so
		// (future) async-IO partition locks don't need to be coroutine aware.
		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			as_partition_shutdown(ns, pid);
		}

		cf_info(AS_STORAGE, "{%s} partitions shut down", ns->name);

		as_sindex_shutdown(ns);

		// Now flush everything outstanding to storage devices.
		as_storage_shutdown_table[ns->storage_type](ns);

		cf_info(AS_STORAGE, "{%s} storage flushed", ns->name);

		if (! as_namespace_xmem_shutdown(ns, instance)) {
			all_ok = false; // but continue - next namespace may be ok
		}
	}

	return all_ok;
}

//--------------------------------------
// as_storage_destroy_record
//

typedef void (*as_storage_destroy_record_fn)(as_namespace* ns, as_record* r);
static const as_storage_destroy_record_fn as_storage_destroy_record_table[] = {
	as_storage_destroy_record_mem,
	as_storage_destroy_record_pmem,
	as_storage_destroy_record_ssd
};

void
as_storage_destroy_record(as_namespace* ns, as_record* r)
{
	as_storage_destroy_record_table[ns->storage_type](ns, r);
}

//--------------------------------------
// as_storage_record_create
//

//--------------------------------------
// as_storage_record_create - 创建存储记录描述符
// 功能：初始化用于新记录的存储描述符，不涉及具体存储引擎操作
//

/**
 * 创建新记录的存储描述符
 * 功能描述：
 * 1. 初始化as_storage_rd结构体
 * 2. 设置记录和namespace的关联
 * 3. 验证记录的初始状态
 *
 * 参数说明：
 * @ns: 目标namespace
 * @r: 要关联的记录索引
 * @rd: 输出的存储描述符
 *
 * 关键逻辑：
 * - 设置当前写块标识为SWB_MASTER
 * - 验证记录的rblock_id为0（未初始化状态）
 * - 不依赖具体存储引擎，通用初始化
 */
void
as_storage_record_create(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	// 初始化存储记录描述符
	*rd = (as_storage_rd){
			.r = r,                          // 关联的记录索引
			.ns = ns,                        // 所属namespace
			.which_current_swb = SWB_MASTER  // 使用主写块
	};

	// 古老的参数检查...确保记录未被初始化
	cf_assert(r->rblock_id == 0, AS_STORAGE, "uninitialized rblock-id");
}

//--------------------------------------
// as_storage_record_open - 打开存储记录
// 功能：打开已存储的记录，准备读取操作
//

// 存储引擎记录打开函数指针类型定义
typedef void (*as_storage_record_open_fn)(as_storage_rd* rd);
static const as_storage_record_open_fn as_storage_record_open_table[] = {
	as_storage_record_open_mem,    // 内存存储引擎记录打开
	as_storage_record_open_pmem,   // PMEM存储引擎记录打开
	as_storage_record_open_ssd     // SSD存储引擎记录打开
};

/**
 * 打开已存储的记录进行读取
 * 功能描述：
 * 1. 初始化存储记录描述符用于读取操作
 * 2. 调用存储引擎特定的打开函数设置设备指针
 * 3. 标记记录位于设备上
 *
 * 参数说明：
 * @ns: 记录所属的namespace
 * @r: 要打开的记录索引
 * @rd: 输出的存储描述符
 *
 * 关键逻辑：
 * - 设置record_on_device为true，表示数据在设备上
 * - 调用存储引擎特定函数设置设备联合指针
 * - 准备好后续的读取操作
 */
void
as_storage_record_open(as_namespace* ns, as_record* r, as_storage_rd* rd)
{
	// 初始化存储记录描述符用于读取
	*rd = (as_storage_rd){
			.r = r,                          // 关联的记录索引
			.ns = ns,                        // 所属namespace
			.record_on_device = true,        // 标记记录在设备上
			.which_current_swb = SWB_MASTER  // 使用主写块
	};

	// 调用存储引擎特定函数设置设备（联合）指针
	as_storage_record_open_table[ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_close - 关闭存储记录
// 功能：清理记录读取过程中分配的资源，主要是SSD存储引擎的读取缓冲区
//

/**
 * 关闭存储记录并清理资源
 * 功能描述：
 * 1. 释放读取操作中分配的缓冲区
 * 2. 主要用于SSD存储引擎的资源清理
 * 3. 内存和PMEM存储引擎通常无需特殊清理
 *
 * 参数说明：
 * @rd: 要关闭的存储记录描述符
 *
 * 关键逻辑：
 * - 仅对SSD存储引擎相关，释放read_buf
 * - 防止内存泄漏
 * - 将指针置为NULL防止重复释放
 */
void
as_storage_record_close(as_storage_rd* rd)
{
	// 仅对AS_STORAGE_ENGINE_SSD相关
	if (rd->read_buf != NULL) {
		cf_free(rd->read_buf);
		rd->read_buf = NULL; // TODO - 需要吗？（我们会重复调用这个函数吗？）
	}
}

//--------------------------------------
// as_storage_record_load_bins - 加载记录的bins数据
// 功能：从存储设备读取记录的bins数据并反序列化
//

// 存储引擎bins加载函数指针类型定义
typedef int (*as_storage_record_load_bins_fn)(as_storage_rd* rd);
static const as_storage_record_load_bins_fn as_storage_record_load_bins_table[] = {
	as_storage_record_load_bins_mem,    // 内存存储引擎bins加载
	as_storage_record_load_bins_pmem,   // PMEM存储引擎bins加载
	as_storage_record_load_bins_ssd     // SSD存储引擎bins加载
};

/**
 * 从存储加载记录的bins数据
 * 功能描述：
 * 1. 调用存储引擎特定的bins加载函数
 * 2. 将序列化的bins数据反序列化为可用格式
 * 3. 设置rd中的bins相关字段
 *
 * 参数说明：
 * @rd: 存储记录描述符
 *
 * 返回值：
 * @return: 0表示成功，负数表示错误码
 *
 * 关键逻辑：
 * - 根据存储类型选择合适的加载函数
 * - 处理序列化数据的解析
 * - 错误处理和资源管理
 */
int
as_storage_record_load_bins(as_storage_rd* rd)
{
	return as_storage_record_load_bins_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_key
//

typedef bool (*as_storage_record_load_key_fn)(as_storage_rd* rd);
static const as_storage_record_load_key_fn as_storage_record_load_key_table[] = {
	as_storage_record_load_key_mem,
	as_storage_record_load_key_pmem,
	as_storage_record_load_key_ssd
};

bool
as_storage_record_load_key(as_storage_rd* rd)
{
	return as_storage_record_load_key_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_pickle
//

typedef bool (*as_storage_record_load_pickle_fn)(as_storage_rd* rd);
static const as_storage_record_load_pickle_fn as_storage_record_load_pickle_table[] = {
	as_storage_record_load_pickle_mem,
	as_storage_record_load_pickle_pmem,
	as_storage_record_load_pickle_ssd
};

bool
as_storage_record_load_pickle(as_storage_rd* rd)
{
	return as_storage_record_load_pickle_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_record_load_raw
//

typedef bool (*as_storage_record_load_raw_fn)(as_storage_rd* rd, bool leave_encrypted);
static const as_storage_record_load_raw_fn as_storage_record_load_raw_table[] = {
	as_storage_record_load_raw_mem,
	as_storage_record_load_raw_pmem,
	as_storage_record_load_raw_ssd
};

bool
as_storage_record_load_raw(as_storage_rd* rd, bool leave_encrypted)
{
	return as_storage_record_load_raw_table[rd->ns->storage_type](rd, leave_encrypted);
}

//--------------------------------------
// as_storage_record_write - 记录写入
// 功能：根据namespace的storage_type将记录写入对应引擎：memory/pmem/ssd
// 写入内容来自rd（as_storage_rd），最终会打包成as_flat_record格式写入写块
//

// 存储引擎记录写入函数指针类型定义
typedef int (*as_storage_record_write_fn)(as_storage_rd* rd);
static const as_storage_record_write_fn as_storage_record_write_table[] = {
	as_storage_record_write_mem,    // 内存存储引擎记录写入
	as_storage_record_write_pmem,   // PMEM存储引擎记录写入
	as_storage_record_write_ssd     // SSD存储引擎记录写入
};

/**
 * 将记录写入存储
 * 功能描述：
 * 1. 根据namespace的存储类型调用对应的写入函数
 * 2. 将as_storage_rd中的数据序列化为as_flat_record格式
 * 3. 写入到对应的存储引擎（内存、PMEM或SSD）
 *
 * 参数说明：
 * @rd: 存储记录描述符，包含要写入的记录数据
 *
 * 返回值：
 * @return: 0表示成功，负数表示错误码
 *
 * 关键逻辑：
 * - 数据序列化：将结构化数据转换为平坦格式
 * - 存储分发：根据存储类型选择合适的写入路径
 * - 错误处理：返回详细的错误信息
 */
int
as_storage_record_write(as_storage_rd* rd)
{
	return as_storage_record_write_table[rd->ns->storage_type](rd);
}

//--------------------------------------
// as_storage_overloaded
//

// Used to have table functions, but no foreseeable need - so we removed them.
bool
as_storage_overloaded(const as_namespace* ns, uint32_t margin, const char* tag)
{
	uint32_t limit = ns->storage_max_write_q + margin;

	if (ns->n_wblocks_to_flush > limit) {
		cf_ticker_warning(AS_STORAGE, "{%s} %s fail: queue too deep: exceeds max %u",
				ns->name, tag, limit);
		return true;
	}

	return false;
}

//--------------------------------------
// as_storage_defrag_sweep
//

typedef void (*as_storage_defrag_sweep_fn)(as_namespace* ns);
static const as_storage_defrag_sweep_fn as_storage_defrag_sweep_table[] = {
	as_storage_defrag_sweep_mem,
	as_storage_defrag_sweep_pmem,
	as_storage_defrag_sweep_ssd
};

void
as_storage_defrag_sweep(as_namespace* ns)
{
	as_storage_defrag_sweep_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_regime
//

typedef void (*as_storage_load_regime_fn)(as_namespace* ns);
static const as_storage_load_regime_fn as_storage_load_regime_table[] = {
	as_storage_load_regime_mem,
	as_storage_load_regime_pmem,
	as_storage_load_regime_ssd
};

void
as_storage_load_regime(as_namespace* ns)
{
	as_storage_load_regime_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_save_regime
//

typedef void (*as_storage_save_regime_fn)(as_namespace* ns);
static const as_storage_save_regime_fn as_storage_save_regime_table[] = {
	as_storage_save_regime_mem,
	as_storage_save_regime_pmem,
	as_storage_save_regime_ssd
};

void
as_storage_save_regime(as_namespace* ns)
{
	as_storage_save_regime_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_roster_generation
//

typedef void (*as_storage_load_roster_generation_fn)(as_namespace* ns);
static const as_storage_load_roster_generation_fn as_storage_load_roster_generation_table[] = {
	as_storage_load_roster_generation_mem,
	as_storage_load_roster_generation_pmem,
	as_storage_load_roster_generation_ssd
};

void
as_storage_load_roster_generation(as_namespace* ns)
{
	as_storage_load_roster_generation_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_save_roster_generation
//

typedef void (*as_storage_save_roster_generation_fn)(as_namespace* ns);
static const as_storage_save_roster_generation_fn as_storage_save_roster_generation_table[] = {
	as_storage_save_roster_generation_mem,
	as_storage_save_roster_generation_pmem,
	as_storage_save_roster_generation_ssd
};

void
as_storage_save_roster_generation(as_namespace* ns)
{
	as_storage_save_roster_generation_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_load_pmeta
//

typedef void (*as_storage_load_pmeta_fn)(as_namespace* ns, as_partition* p);
static const as_storage_load_pmeta_fn as_storage_load_pmeta_table[] = {
	as_storage_load_pmeta_mem,
	as_storage_load_pmeta_pmem,
	as_storage_load_pmeta_ssd
};

void
as_storage_load_pmeta(as_namespace* ns, as_partition* p)
{
	as_storage_load_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_save_pmeta
//

typedef void (*as_storage_save_pmeta_fn)(as_namespace* ns, const as_partition* p);
static const as_storage_save_pmeta_fn as_storage_save_pmeta_table[] = {
	as_storage_save_pmeta_mem,
	as_storage_save_pmeta_pmem,
	as_storage_save_pmeta_ssd
};

void
as_storage_save_pmeta(as_namespace* ns, const as_partition* p)
{
	as_storage_save_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_cache_pmeta
//

typedef void (*as_storage_cache_pmeta_fn)(as_namespace* ns, const as_partition* p);
static const as_storage_cache_pmeta_fn as_storage_cache_pmeta_table[] = {
	as_storage_cache_pmeta_mem,
	as_storage_cache_pmeta_pmem,
	as_storage_cache_pmeta_ssd
};

void
as_storage_cache_pmeta(as_namespace* ns, const as_partition* p)
{
	as_storage_cache_pmeta_table[ns->storage_type](ns, p);
}

//--------------------------------------
// as_storage_flush_pmeta
//

typedef void (*as_storage_flush_pmeta_fn)(as_namespace* ns, uint32_t start_pid, uint32_t n_partitions);
static const as_storage_flush_pmeta_fn as_storage_flush_pmeta_table[] = {
	as_storage_flush_pmeta_mem,
	as_storage_flush_pmeta_pmem,
	as_storage_flush_pmeta_ssd
};

void
as_storage_flush_pmeta(as_namespace* ns, uint32_t start_pid, uint32_t n_partitions)
{
	as_storage_flush_pmeta_table[ns->storage_type](ns, start_pid, n_partitions);
}

//--------------------------------------
// as_storage_stats
//

typedef void (*as_storage_stats_fn)(as_namespace* ns, uint32_t* avail_pct, uint64_t* used_bytes);
static const as_storage_stats_fn as_storage_stats_table[] = {
	as_storage_stats_mem,
	as_storage_stats_pmem,
	as_storage_stats_ssd
};

void
as_storage_stats(as_namespace* ns, uint32_t* avail_pct, uint64_t* used_bytes)
{
	as_storage_stats_table[ns->storage_type](ns, avail_pct, used_bytes);
}

//--------------------------------------
// as_storage_device_stats
//

typedef void (*as_storage_device_stats_fn)(const as_namespace* ns, uint32_t device_ix, storage_device_stats* stats);
static const as_storage_device_stats_fn as_storage_device_stats_table[] = {
	as_storage_device_stats_mem,
	as_storage_device_stats_pmem,
	as_storage_device_stats_ssd
};

void
as_storage_device_stats(const as_namespace* ns, uint32_t device_ix, storage_device_stats* stats)
{
	as_storage_device_stats_table[ns->storage_type](ns, device_ix, stats);
}

//--------------------------------------
// as_storage_ticker_stats
//

typedef void (*as_storage_ticker_stats_fn)(as_namespace* ns);
static const as_storage_ticker_stats_fn as_storage_ticker_stats_table[] = {
	as_storage_ticker_stats_mem,
	as_storage_ticker_stats_pmem,
	as_storage_ticker_stats_ssd
};

void
as_storage_ticker_stats(as_namespace* ns)
{
	as_storage_ticker_stats_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_dump_wb_summary
//

typedef void (*as_storage_dump_wb_summary_fn)(const as_namespace* ns);
static const as_storage_dump_wb_summary_fn as_storage_dump_wb_summary_table[] = {
	as_storage_dump_wb_summary_mem,
	as_storage_dump_wb_summary_pmem,
	as_storage_dump_wb_summary_ssd
};

void
as_storage_dump_wb_summary(const as_namespace* ns)
{
	as_storage_dump_wb_summary_table[ns->storage_type](ns);
}

//--------------------------------------
// as_storage_histogram_clear_all
//

typedef void (*as_storage_histogram_clear_fn)(as_namespace* ns);
static const as_storage_histogram_clear_fn as_storage_histogram_clear_table[] = {
	as_storage_histogram_clear_mem,
	as_storage_histogram_clear_pmem,
	as_storage_histogram_clear_ssd
};

void
as_storage_histogram_clear_all(as_namespace* ns)
{
	as_storage_histogram_clear_table[ns->storage_type](ns);
}


//==========================================================
// Generic functions that don't use "v-tables".
//

void
as_storage_record_get_set_name(as_storage_rd* rd)
{
	rd->set_name = as_index_get_set_name(rd->r, rd->ns);

	if (rd->set_name != NULL) {
		rd->set_name_len = strlen(rd->set_name);
	}
}

bool
as_storage_rd_load_key(as_storage_rd* rd)
{
	if (rd->r->key_stored == 0) {
		return false;
	}

	if (rd->record_on_device && ! rd->ignore_record_on_device) {
		return as_storage_record_load_key(rd);
	}

	return false;
}
