/*
 * dynbuf.c - 动态缓冲区管理模块
 *
 * 功能描述：
 * - 提供自动扩展的动态缓冲区，支持高效的数据追加操作
 * - 实现了多种数据类型的追加方法（字符串、数值、布尔值等）
 * - 支持栈内存和堆内存两种分配策略，优化小缓冲区性能
 * - 提供格式化输出功能，类似 sprintf 但更安全
 * - 包含链式缓冲区（cf_ll_buf）和构建器模式（cf_buf_builder）
 * - 专门针对信息输出场景优化，支持键值对格式化
 *
 * Copyright (C) 2008-2022 Aerospike, Inc.
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

#include "dynbuf.h"

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <citrusleaf/alloc.h>


#define MAX_BACKOFF (1024 * 256)  // 最大回退分配大小，防止过度分配内存
#define MAX_FORMAT 100             // 格式化字符串的最大长度限制

// 计算新的缓冲区大小，采用分阶段回退策略以平衡内存使用和性能
// 参数：
//   alloc - 当前已分配的缓冲区大小
//   used - 当前已使用的字节数
//   requested - 本次请求的额外字节数
// 返回值：
//   计算得出的新缓冲区大小
// 内存分配策略：
//   - 小于8KB：以1KB为单位对齐，适合小数据快速分配
//   - 8KB-32KB：以4KB为单位对齐，平衡内存碎片
//   - 32KB-128KB：以32KB为单位对齐，减少大数据的重分配次数
//   - 大于128KB：以256KB为单位对齐，优化大缓冲区性能
size_t
get_new_size(int alloc, int used, int requested)
{
	// 如果当前缓冲区剩余空间足够，直接返回当前大小
	if (alloc - used > requested) {
		return alloc;
	}

	// 计算基础新大小：当前大小 + 请求大小 + 结构体开销
	size_t new_sz = alloc + requested + sizeof(cf_buf_builder);
	int backoff;

	// 根据缓冲区大小选择不同的对齐策略
	if (new_sz < 1024 * 8) {
		backoff = 1024;        // 1KB对齐
	}
	else if (new_sz < 1024 * 32) {
		backoff = 1024 * 4;    // 4KB对齐
	}
	else if (new_sz < 1024 * 128) {
		backoff = 1024 * 32;   // 32KB对齐
	}
	else {
		backoff = MAX_BACKOFF; // 256KB对齐
	}

	// 向上对齐到指定边界，减少内存碎片化
	return new_sz + (backoff - (new_sz % backoff));
}

// 动态缓冲区内部扩容函数，处理栈内存到堆内存的转换
// 参数：
//   db - 动态缓冲区对象指针
//   sz - 需要额外分配的字节数
// 内存管理策略：
//   - 如果是栈缓冲区且需要扩容，先分配堆内存并复制数据，然后标记为堆缓冲区
//   - 如果已经是堆缓冲区，直接使用realloc扩容
//   - 扩容大小通过get_new_size函数计算，采用对齐策略减少重分配频率
void
cf_dyn_buf_reserve_internal(cf_dyn_buf *db, size_t sz)
{
	// 计算新的缓冲区大小
	size_t new_sz = get_new_size(db->alloc_sz, db->used_sz, sz);

	// 只有当新大小确实大于当前分配大小时才进行扩容
	if (new_sz > db->alloc_sz) {
		uint8_t	*_t;

		// 处理栈内存到堆内存的转换
		if (db->is_stack) {
			// 分配新的堆内存
			_t = cf_malloc(new_sz);
			// 复制现有数据到新内存
			memcpy(_t, db->buf, db->used_sz);
			// 标记为堆缓冲区
			db->is_stack = false;
		}
		else {
			// 已经是堆缓冲区，直接重新分配
			_t = cf_realloc(db->buf, new_sz);
		}

		// 更新缓冲区指针和大小
		db->buf = _t;
		db->alloc_sz = new_sz;
	}
}

// 缓冲区空间检查和预留宏，如果空间不足则自动扩容
// 参数 _n：需要的额外字节数
#define DB_RESERVE(_n) \
	if (db->alloc_sz - db->used_sz < _n) { \
		cf_dyn_buf_reserve_internal(db, _n); \
	}

// 初始化堆内存动态缓冲区
// 参数：
//   db - 要初始化的动态缓冲区对象
//   sz - 初始分配的缓冲区大小
// 功能：直接在堆上分配指定大小的内存，适合长期使用的缓冲区
void
cf_dyn_buf_init_heap(cf_dyn_buf *db, size_t sz)
{
	db->buf = cf_malloc(sz);        // 在堆上分配内存
	db->is_stack = false;           // 标记为堆缓冲区
	db->alloc_sz = sz;              // 设置分配大小
	db->used_sz = 0;                // 初始使用大小为0
}

// 预留指定大小的缓冲区空间，并返回写入位置指针
// 参数：
//   db - 动态缓冲区对象
//   sz - 要预留的字节数
//   from - 输出参数，返回可写入数据的起始位置指针（可为NULL）
// 功能：
//   - 确保缓冲区有足够空间容纳sz字节数据
//   - 更新已使用大小，相当于"占位"操作
//   - 调用者需要自行向返回的指针位置写入数据
void
cf_dyn_buf_reserve(cf_dyn_buf *db, size_t sz, uint8_t **from)
{
	DB_RESERVE(sz);

	// 如果需要返回写入位置，设置为当前缓冲区末尾
	if (from) {
		*from = &db->buf[db->used_sz];
	}

	// 更新已使用大小（预先占位）
	db->used_sz += sz;
}

// 向动态缓冲区追加二进制数据
// 参数：
//   db - 动态缓冲区对象
//   buf - 要追加的数据缓冲区
//   sz - 数据大小（字节数）
// 功能：将指定长度的二进制数据复制到缓冲区末尾
void
cf_dyn_buf_append_buf(cf_dyn_buf *db, const uint8_t *buf, size_t sz)
{
	DB_RESERVE(sz);
	memcpy(&db->buf[db->used_sz], buf, sz);
	db->used_sz += sz;
}

// 向动态缓冲区追加字符串
// 参数：
//   db - 动态缓冲区对象
//   s - 要追加的C字符串（以'\0'结尾）
// 功能：将字符串内容（不包括结尾的'\0'）追加到缓冲区
void
cf_dyn_buf_append_string(cf_dyn_buf *db, const char *s)
{
	size_t len = strlen(s);

	DB_RESERVE(len);
	memcpy(&db->buf[db->used_sz], s, len);
	db->used_sz += len;
}

// 向动态缓冲区追加单个字符
// 参数：
//   db - 动态缓冲区对象
//   c - 要追加的字符
void
cf_dyn_buf_append_char(cf_dyn_buf *db, char c)
{
	DB_RESERVE(1);
	db->buf[db->used_sz] = (uint8_t)c;
	db->used_sz++;
}

// 向动态缓冲区追加布尔值的字符串表示
// 参数：
//   db - 动态缓冲区对象
//   b - 布尔值
// 功能：追加"true"或"false"字符串
void
cf_dyn_buf_append_bool(cf_dyn_buf *db, bool b)
{
	if (b) {
		DB_RESERVE(4);
		memcpy(&db->buf[db->used_sz], "true", 4);
		db->used_sz += 4;
	}
	else {
		DB_RESERVE(5);
		memcpy(&db->buf[db->used_sz], "false", 5);
		db->used_sz += 5;
	}
}

void
cf_dyn_buf_append_int(cf_dyn_buf *db, int i)
{
	DB_RESERVE(12);
	db->used_sz += sprintf((char *)&db->buf[db->used_sz], "%d", i);
}

void
cf_dyn_buf_append_uint64_x(cf_dyn_buf *db, uint64_t i)
{
	DB_RESERVE(18);
	db->used_sz += sprintf((char *)&db->buf[db->used_sz], "%lX", i);
}

void
cf_dyn_buf_append_uint64(cf_dyn_buf *db, uint64_t i)
{
	DB_RESERVE(22);
	db->used_sz += sprintf((char *)&db->buf[db->used_sz], "%lu", i);
}

void
cf_dyn_buf_append_uint32(cf_dyn_buf *db, uint32_t i)
{
	DB_RESERVE(12);
	db->used_sz += sprintf((char *)&db->buf[db->used_sz], "%u", i);
}

void
cf_dyn_buf_append_format_va(cf_dyn_buf *db, const char *form, va_list va)
{
	DB_RESERVE(MAX_FORMAT + 1);
	int32_t len = vsnprintf((char *)&db->buf[db->used_sz], MAX_FORMAT + 1, form,
			va);

	if (len > MAX_FORMAT) {
		len = MAX_FORMAT;
	}

	db->used_sz += len;
}

void
cf_dyn_buf_append_format(cf_dyn_buf *db, const char *form, ...)
{
	va_list va;
	va_start(va, form);

	cf_dyn_buf_append_format_va(db, form, va);

	va_end(va);
}

void
cf_dyn_buf_chomp(cf_dyn_buf *db)
{
	if (db->used_sz > 0) {
		db->used_sz--;
	}
}

void
cf_dyn_buf_chomp_char(cf_dyn_buf *db, char c)
{
	if (db->used_sz > 0 && db->buf[db->used_sz - 1] == (uint8_t)c) {
		db->used_sz--;
	}
}

char *
cf_dyn_buf_strdup(cf_dyn_buf *db)
{
	if (db->used_sz == 0) {
		return NULL;
	}

	char *s = cf_malloc(db->used_sz + 1);

	memcpy(s, db->buf, db->used_sz);
	s[db->used_sz] = 0;

	return s;
}

void
cf_dyn_buf_free(cf_dyn_buf *db)
{
	if (! db->is_stack && db->buf) {
		cf_free(db->buf);
	}
}

// Helpers to append name value pairs to a cf_dyn_buf in pattern: name=value;

void
info_append_bool(cf_dyn_buf *db, const char *name, bool value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_bool(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_int(cf_dyn_buf *db, const char *name, int value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_int(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_string(cf_dyn_buf *db, const char *name, const char *value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_string(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_string_safe(cf_dyn_buf *db, const char *name, const char *value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_string(db, value ? value : "null");
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint32(cf_dyn_buf *db, const char *name, uint32_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint32(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint64(cf_dyn_buf *db, const char *name, uint64_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint64(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_uint64_x(cf_dyn_buf *db, const char *name, uint64_t value)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_uint64_x(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_format(cf_dyn_buf *db, const char *name, const char *form, ...)
{
	va_list va;
	va_start(va, form);

	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '=');
	cf_dyn_buf_append_format_va(db, form, va);
	cf_dyn_buf_append_char(db, ';');

	va_end(va);
}

static inline void
append_indexed_name(cf_dyn_buf *db, const char *name, uint32_t ix,
		const char *attr)
{
	cf_dyn_buf_append_string(db, name);
	cf_dyn_buf_append_char(db, '[');
	cf_dyn_buf_append_uint32(db, ix);
	cf_dyn_buf_append_char(db, ']');

	if (attr) {
		cf_dyn_buf_append_char(db, '.');
		cf_dyn_buf_append_string(db, attr);
	}

	cf_dyn_buf_append_char(db, '=');
}

void
info_append_indexed_string(cf_dyn_buf *db, const char *name, uint32_t ix,
		const char *attr, const char *value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_string(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_int(cf_dyn_buf *db, const char *name, uint32_t ix,
		const char *attr, int value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_int(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_uint32(cf_dyn_buf *db, const char *name, uint32_t ix,
		const char *attr, uint32_t value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_uint32(db, value);
	cf_dyn_buf_append_char(db, ';');
}

void
info_append_indexed_uint64(cf_dyn_buf *db, const char *name, uint32_t ix,
		const char *attr, uint64_t value)
{
	append_indexed_name(db, name, ix, attr);
	cf_dyn_buf_append_uint64(db, value);
	cf_dyn_buf_append_char(db, ';');
}



void
cf_buf_builder_reserve_internal(cf_buf_builder **bb_r, size_t sz)
{
	cf_buf_builder *bb = *bb_r;
	size_t new_sz = get_new_size(bb->alloc_sz, bb->used_sz, sz);

	if (new_sz > bb->alloc_sz) {
		if (bb->alloc_sz - bb->used_sz < MAX_BACKOFF) {
			bb = cf_realloc(bb, new_sz);
		}
		else {
			// Only possible if buffer was reset. Avoids potential expensive
			// copy within realloc.
			cf_buf_builder	*_t = cf_malloc(new_sz);

			memcpy(_t->buf, bb->buf, bb->used_sz);
			_t->used_sz = bb->used_sz;
			cf_free(bb);
			bb = _t;
		}

		bb->alloc_sz = new_sz - sizeof(cf_buf_builder);
		*bb_r = bb;
	}
}

#define BB_RESERVE(_n) \
	if ((*bb_r)->alloc_sz - (*bb_r)->used_sz < _n) { \
		cf_buf_builder_reserve_internal(bb_r, _n); \
	}

void
cf_buf_builder_reserve(cf_buf_builder **bb_r, int sz, uint8_t **buf)
{
	BB_RESERVE(sz);
	cf_buf_builder *bb = *bb_r;

	if (buf) {
		*buf = &bb->buf[bb->used_sz];
	}

	bb->used_sz += sz;
}

cf_buf_builder *
cf_buf_builder_create(size_t sz)
{
	size_t malloc_sz = (sz < 1024) ? 1024 : sz;
	cf_buf_builder *bb = cf_malloc(malloc_sz);

	bb->alloc_sz = malloc_sz - sizeof(cf_buf_builder);
	bb->used_sz = 0;

	return bb;
}

void
cf_buf_builder_free(cf_buf_builder *bb)
{
	cf_free(bb);
}

void
cf_buf_builder_reset(cf_buf_builder *bb)
{
	bb->used_sz = 0;
}



// TODO - We've only implemented a few cf_ll_buf methods for now. We'll add more
// functionality if and when it's needed.

void
cf_ll_buf_init_heap(cf_ll_buf *llb, size_t buf_sz)
{
	cf_ll_buf_stage *stage = cf_malloc(sizeof(cf_ll_buf_stage) + buf_sz);

	*stage = (cf_ll_buf_stage){ .buf_sz = buf_sz };
	*llb = (cf_ll_buf){ .head = stage, .tail = stage };
}

void
cf_ll_buf_grow(cf_ll_buf *llb, size_t sz)
{
	size_t buf_sz = sz > llb->head->buf_sz ? sz : llb->head->buf_sz;
	cf_ll_buf_stage *new_tail = cf_malloc(sizeof(cf_ll_buf_stage) + buf_sz);

	new_tail->next = NULL;
	new_tail->buf_sz = buf_sz;
	new_tail->used_sz = 0;

	llb->tail->next = new_tail;
	llb->tail = new_tail;
}

#define LLB_RESERVE(_n) \
		if (_n > llb->tail->buf_sz - llb->tail->used_sz) { \
			cf_ll_buf_grow(llb, _n); \
		}

void
cf_ll_buf_reserve(cf_ll_buf *llb, size_t sz, uint8_t **from)
{
	LLB_RESERVE(sz);

	if (from) {
		*from = llb->tail->buf + llb->tail->used_sz;
	}

	llb->tail->used_sz += sz;
}

void
cf_ll_buf_free(cf_ll_buf *llb)
{
	cf_ll_buf_stage *cur = llb->head_is_stack ? llb->head->next : llb->head;

	while (cur) {
		cf_ll_buf_stage *temp = cur;

		cur = cur->next;
		cf_free(temp);
	}
}
