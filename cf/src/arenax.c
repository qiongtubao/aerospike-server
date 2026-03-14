/*
 * arenax.c - 内存竞技场扩展管理模块
 *
 * 功能描述：
 * - 提供高效的内存分配和释放机制，支持多个阶段（stage）的内存管理
 * - 实现了类似内存池的功能，减少内存碎片化
 * - 支持持久化内存管理，使用 xmem 接口与底层存储交互
 * - 通过分块（chunk）管理提供线程安全的内存分配
 * - 维护空闲元素链表，提高内存重用效率
 *
 * Copyright (C) 2012-2023 Aerospike, Inc.
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

#include "arenax.h"
 
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "log.h"
#include "xmem.h"


//==========================================================
// Typedefs & constants.
//

// 错误信息字符串数组，必须与 cf_arenax_err 枚举保持同步
// 用于将错误码转换为可读的错误描述信息
const char* ARENAX_ERR_STRINGS[] = {
	"ok",
	"bad parameter",
	"error creating stage",
	"error attaching stage",
	"error detaching stage",
	"unknown error"
};


//==========================================================
// Public API.
//

// 将 cf_arenax_err 错误码转换为可读的错误描述字符串
// 参数：
//   err - 错误码枚举值
// 返回值：
//   对应错误码的描述字符串
const char*
cf_arenax_errstr(cf_arenax_err err)
{
	// 检查错误码是否在有效范围内，超出范围则返回未知错误
	if (err < 0 || err > CF_ARENAX_ERR_UNKNOWN) {
		err = CF_ARENAX_ERR_UNKNOWN;
	}

	return ARENAX_ERR_STRINGS[err];
}

// 初始化内存竞技场对象，在持久化内存中创建竞技场和第一个内存阶段
// 参数：
//   arena - 要初始化的竞技场对象指针
//   xmem_type - 扩展内存类型（如共享内存、文件映射等）
//   xmem_type_cfg - 扩展内存类型的配置参数
//   key_base - 用于生成内存段标识的基础键值
//   element_size - 每个元素的字节大小
//   chunk_count - 每次分配的块数量（1表示单元素分配，>1表示批量分配）
//   stage_size - 每个内存阶段的总字节大小
// 功能：
//   - 设置竞技场基本参数和内存分配策略
//   - 初始化空闲元素链表和分配追踪变量
//   - 创建线程同步锁确保并发安全
//   - 添加第一个内存阶段并清零空元素
void
cf_arenax_init(cf_arenax* arena, cf_xmem_type xmem_type,
		const void* xmem_type_cfg, key_t key_base, uint32_t element_size,
		uint32_t chunk_count, size_t stage_size)
{
	// 设置竞技场基本配置参数
	arena->xmem_type = xmem_type;
	arena->xmem_type_cfg = xmem_type_cfg;
	arena->key_base = key_base;
	arena->element_size = element_size;
	arena->chunk_count = chunk_count;
	arena->stage_capacity = (uint32_t)(stage_size / element_size);  // 每阶段可容纳的元素数量
	arena->unused_1 = 0;
	arena->unused_2 = 0;
	arena->stage_size = stage_size;

	// 初始化空闲元素链表头指针
	arena->free_h = 0;

	// 根据块分配数量设置池缓冲区
	// chunk_count = 1: 单元素分配模式，无需池缓冲区
	// chunk_count > 1: 批量分配模式，需要池缓冲区管理
	if (chunk_count == 1) {
		arena->pool_len = 0;
		arena->pool_buf = NULL;
	}
	else {
		arena->pool_len = arena->stage_capacity;
		// 为池分配内存，存储批量分配的块信息
		arena->pool_buf =
				cf_malloc(arena->pool_len * sizeof(cf_arenax_chunk));
	}

	arena->pool_i = 0;
	arena->alloc_sz = 0; // 仅用于闪存索引统计

	// 跳过 0:0 位置，确保空句柄永远不会被使用
	// 这样可以将句柄值 0 作为无效/空句柄的标识
	arena->at_stage_id = 0;
	arena->at_element_id = arena->chunk_count;

	// 初始化线程互斥锁，保证并发访问安全性
	cf_mutex_init(&arena->lock);

	// 初始化阶段计数和阶段数组
	arena->stage_count = 0;
	memset(arena->stages, 0, sizeof(arena->stages));

	// 添加第一个内存阶段，如果失败则程序崩溃
	if (cf_arenax_add_stage(arena) != CF_ARENAX_OK) {
		cf_crash(CF_ARENAX, "failed to add first stage");
	}

	// 清零空元素内存，防止读取垃圾数据
	// 虽然分配时会跳过空元素，但可能会被读取到
	memset(cf_arenax_resolve(arena, 0), 0, element_size * chunk_count);
}

// 在竞技场中分配一个元素，返回元素句柄
// 参数：
//   arena - 竞技场对象指针
//   puddle - 水坑对象指针，用于批量分配（NULL表示单元素分配）
// 返回值：
//   成功时返回有效的元素句柄，失败时返回 0
// 内存分配策略：
//   1. 优先从空闲链表中获取已释放的元素（提高内存重用率）
//   2. 如果空闲链表为空，则从当前阶段末尾分配新元素
//   3. 如果当前阶段空间不足，创建新阶段并从新阶段分配
// 线程安全性：使用互斥锁保护分配过程，确保并发安全
cf_arenax_handle
cf_arenax_alloc(cf_arenax* arena, cf_arenax_puddle* puddle)
{
	// 如果提供了水坑对象，使用批量分配模式
	if (puddle != NULL) {
		return cf_arenax_alloc_chunked(arena, puddle);
	}

	// 获取互斥锁，保证线程安全的单元素分配
	cf_mutex_lock(&arena->lock);

	cf_arenax_handle h;

	// 策略1：优先检查空闲链表，重用已释放的内存
	if (arena->free_h != 0) {
		h = arena->free_h;

		// 获取空闲元素指针，更新空闲链表头
		free_element* p_free_element = cf_arenax_resolve(arena, h);

		arena->free_h = p_free_element->next_h;
	}
	// 策略2：从当前阶段末尾分配新元素
	else {
		// 检查当前阶段是否还有可用空间
		if (arena->at_element_id >= arena->stage_capacity) {
			// 当前阶段已满，尝试添加新阶段
			if (cf_arenax_add_stage(arena) != CF_ARENAX_OK) {
				cf_mutex_unlock(&arena->lock);
				return 0;  // 添加阶段失败，返回无效句柄
			}

			// 切换到新阶段，重置元素索引
			arena->at_stage_id++;
			arena->at_element_id = 0;
		}

		// 生成新元素的句柄
		cf_arenax_set_handle(&h, arena->at_stage_id, arena->at_element_id);

		// 更新下一个可分配元素的位置
		arena->at_element_id++;
	}

	cf_mutex_unlock(&arena->lock);

	return h;
}

// 释放竞技场中的一个元素，将其加入空闲链表以供后续重用
// 参数：
//   arena - 竞技场对象指针
//   h - 要释放的元素句柄
//   puddle - 水坑对象指针，用于批量释放（NULL表示单元素释放）
// 内存回收策略：
//   - 将释放的元素标记为FREE_MAGIC，便于调试和验证
//   - 使用链表结构管理空闲元素，新释放的元素插入链表头部
//   - 不实际回收内存到系统，而是保留在竞技场中供后续分配重用
// 线程安全性：使用互斥锁保护释放过程，确保空闲链表操作的原子性
void
cf_arenax_free(cf_arenax* arena, cf_arenax_handle h, cf_arenax_puddle* puddle)
{
	// 如果提供了水坑对象，使用批量释放模式
	if (puddle != NULL) {
		cf_arenax_free_chunked(arena, h, puddle);
		return;
	}

	// 通过句柄解析出元素的内存地址，转换为空闲元素结构
	free_element* p_free_element = cf_arenax_resolve(arena, h);

	// 获取互斥锁，保证线程安全的空闲链表操作
	cf_mutex_lock(&arena->lock);

	// 标记元素为已释放状态，设置魔数用于调试验证
	p_free_element->magic = FREE_MAGIC;
	// 将当前元素插入空闲链表头部（LIFO策略）
	p_free_element->next_h = arena->free_h;
	arena->free_h = h;

	cf_mutex_unlock(&arena->lock);
}

// 检查给定的内存地址是否属于竞技场中的某个内存阶段
// 参数：
//   arena - 竞技场对象指针
//   address - 要检查的内存地址
// 返回值：
//   true - 地址属于竞技场的某个阶段
//   false - 地址不属于竞技场的任何阶段
// 功能：
//   - 遍历竞技场中所有已分配的内存阶段
//   - 检查给定地址是否与任一阶段的起始地址匹配
//   - 主要用于内存管理和调试验证
// 线程安全性：使用互斥锁保护阶段数组的遍历过程
bool
cf_arenax_is_stage_address(cf_arenax* arena, const void* address)
{
	bool found = false;

	// 获取互斥锁，保护阶段数组的并发访问
	cf_mutex_lock(&arena->lock);

	// 遍历所有已分配的内存阶段
	for (uint32_t i = 0; i < arena->stage_count; i++) {
		// 检查地址是否匹配某个阶段的起始地址
		if (arena->stages[i] == address) {
			found = true;
			break;
		}
	}

	cf_mutex_unlock(&arena->lock);

	return found;
}
