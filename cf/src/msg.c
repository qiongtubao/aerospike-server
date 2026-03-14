/*
 * msg.c - 消息格式化与序列化模块
 *
 * 功能描述：
 * - 提供高效的消息序列化和反序列化机制，支持网络传输
 * - 实现基于模板的消息结构定义，确保类型安全和版本兼容性
 * - 支持多种数据类型：整数、字符串、二进制数据、数组等
 * - 提供零拷贝的消息解析能力，直接指向原始缓冲区数据
 * - 集成MessagePack格式支持，兼容标准序列化协议
 * - 支持引用计数的内存管理，防止内存泄漏
 * - 优化网络传输性能，支持scatter-gather I/O操作
 * - 提供消息字段的动态设置和获取接口
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


//==========================================================
// Includes.
//

#include "msg.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>

#include "aerospike/as_msgpack.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_byte_order.h"

#include "dynbuf.h"
#include "log.h"
#include "msgpack_in.h"
#include "vector.h"


//==========================================================
// Typedefs & constants.
//

// 消息类型注册表项，维护每种消息类型的元数据
typedef struct msg_type_entry_s {
	const msg_template *mt;     // 消息模板数组指针
	uint16_t entry_count;       // 模板项数量（字段ID的最大值+1）
	uint32_t scratch_sz;        // 附加缓冲区大小
} msg_type_entry;

// 网络传输时的消息字段头结构（紧凑布局）
typedef struct msg_field_hdr_s {
	uint16_t id;                // 字段ID（大端序）
	uint8_t type;               // 字段类型
	uint8_t content[];          // 字段内容（可变长度）
} __attribute__ ((__packed__)) msg_field_hdr;

// 缓冲区类型字段的头大小：字段头 + 4字节长度字段
#define BUF_FIELD_HDR_SZ (sizeof(msg_field_hdr) + sizeof(uint32_t))


//==========================================================
// Globals.
//

// 全局消息类型注册表，索引对应msg_type枚举值
static msg_type_entry g_mte[M_TYPE_MAX];


//==========================================================
// Forward declarations.
//

static uint32_t msg_get_field_wire_size(msg_field_type type, uint32_t field_sz);
static uint32_t msg_field_write_hdr(const msg_field *mf, msg_field_type type, uint8_t *buf);
static uint32_t msg_field_write_buf(const msg_field *mf, msg_field_type type, uint8_t *buf);
static void msg_field_save(msg *m, msg_field *mf);
static bool msgpack_list_unpack_hdr(msgpack_in *mp, const msg *m, int field_id, uint32_t *count_r);


//==========================================================
// Inlines.
// 内联函数
//

/**
 * 检查消息字段类型是否为整数类型
 *
 * 参数：
 * @param type - 消息字段类型
 *
 * 返回值：
 * @return true - 如果是UINT32或UINT64类型
 * @return false - 其他类型
 */
static inline bool
mf_type_is_int(msg_field_type type)
{
	return type == M_FT_UINT32 || type == M_FT_UINT64;
}

/**
 * 获取消息字段的类型定义
 *
 * 参数：
 * @param mf - 消息字段指针
 * @param type - 消息类型
 *
 * 返回值：
 * @return 字段的类型定义
 */
static inline msg_field_type
mf_type(const msg_field *mf, msg_type type)
{
	return g_mte[type].mt[mf->id].type;
}

/**
 * 销毁消息字段，释放相关资源
 *
 * 功能描述：
 * - 如果字段已设置且需要释放内存，则调用cf_free
 * - 重置字段的状态标志
 *
 * 参数：
 * @param mf - 待销毁的消息字段
 */
static inline void
mf_destroy(msg_field *mf)
{
	if (mf->is_set) {
		if (mf->is_free) {
			cf_free(mf->u.any_buf);		// 释放动态分配的内存
			mf->is_free = false;
		}

		mf->is_set = false;				// 标记字段为未设置
	}
}


//==========================================================
// Public API - lifecycle.
//

// 注册消息类型模板，建立类型到字段定义的映射关系
// 参数：
//   type - 消息类型枚举值
//   mt - 消息模板数组，定义该类型的所有字段
//   mt_sz - 模板数组的字节大小
//   scratch_sz - 附加缓冲区大小，用于存储可变长度数据
// 功能：
//   - 验证消息类型有效性，防止重复注册
//   - 创建稀疏数组存储字段定义，以字段ID为索引快速查找
//   - 支持运行时的消息结构验证和类型安全检查
void
msg_type_register(msg_type type, const msg_template *mt, size_t mt_sz,
		size_t scratch_sz)
{
	cf_assert(type < M_TYPE_MAX, CF_MSG, "unknown type %d", type);

	msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = (uint16_t)(mt_sz / sizeof(msg_template));

	// 检查是否已经注册过该类型（支持重复注册，用于版本升级）
	if (mte->mt) {
		// 这种情况在心跳版本跳跃时会发生 - 现在温和处理
		cf_info(CF_MSG, "msg_type_register() type %d already registered", type);
		return;
	}

	cf_assert(mt_count != 0, CF_MSG, "msg_type_register() empty template");

	// 找出最大字段ID，用于分配稀疏数组
	uint16_t max_id = 0;

	for (uint16_t i = 0; i < mt_count; i++) {
		if (mt[i].id >= max_id) {
			max_id = mt[i].id;
		}
	}

	// 分配稀疏数组，大小为最大ID+1
	mte->entry_count = max_id + 1;

	msg_template *table = cf_calloc(mte->entry_count, sizeof(msg_template));

	// 将模板数据复制到稀疏数组中，以字段ID为索引
	for (uint16_t i = 0; i < mt_count; i++) {
		table[mt[i].id] = mt[i];
	}

	mte->mt = table;
	mte->scratch_sz = (uint32_t)scratch_sz;
}

// 检查消息类型是否已注册并有效
// 参数：
//   type - 要检查的消息类型
// 返回值：
//   true - 类型有效且已注册
//   false - 类型无效或未注册
bool
msg_type_is_valid(msg_type type)
{
	return type < M_TYPE_MAX && g_mte[type].mt != NULL;
}

// 创建指定类型的消息对象，使用引用计数管理内存
// 参数：
//   type - 消息类型
// 返回值：
//   新创建的消息对象指针
// 内存布局：
//   [msg结构] + [msg_field数组] + [scratch缓冲区]
// 引用计数：初始引用计数为1，需要配合msg_destroy使用
msg *
msg_create(msg_type type)
{
	cf_assert(type < M_TYPE_MAX && g_mte[type].mt != NULL, CF_MSG, "invalid type %u", type);

	const msg_type_entry *mte = &g_mte[type];
	uint16_t mt_count = mte->entry_count;
	// 计算消息结构大小：基础结构 + 字段数组
	size_t u_sz = sizeof(msg) + (sizeof(msg_field) * mt_count);
	// 总分配大小：结构大小 + 附加缓冲区
	size_t a_sz = u_sz + (size_t)mte->scratch_sz;
	// 使用引用计数分配内存
	msg *m = cf_rc_alloc(a_sz);

	// 初始化消息基本信息
	m->n_fields = mt_count;
	m->bytes_used = (uint32_t)u_sz;
	m->bytes_alloc = (uint32_t)a_sz;
	m->just_parsed = false;     // 标记为非解析状态
	m->type = type;

	// 初始化所有字段为未设置状态
	for (uint16_t i = 0; i < mt_count; i++) {
		msg_field *mf = &m->f[i];

		mf->id = i;
		mf->is_set = false;     // 字段未设置
		mf->is_free = false;    // 内存不需要释放
	}

	return m;
}

// 销毁消息对象，使用引用计数管理内存生命周期
// 参数：
//   m - 要销毁的消息对象
// 功能：
//   - 递减引用计数，只有当引用计数为0时才真正释放内存
//   - 释放所有字段占用的动态内存
//   - 调用msg_put完成最终清理
void
msg_destroy(msg *m)
{
	// 递减引用计数，返回新的计数值
	if (cf_rc_release(m) == 0) {
		// 引用计数为0，清理所有字段的动态内存
		for (uint32_t i = 0; i < m->n_fields; i++) {
			mf_destroy(&m->f[i]);
		}

		// 执行最终的内存回收
		msg_put(m);
	}
}


//==========================================================
// Public API - pack messages into flattened data.
// 公共API - 将消息序列化为扁平化数据
//

/**
 * 计算消息在网络传输时的总字节大小
 *
 * 功能描述：
 * - 计算消息头部大小
 * - 遍历所有已设置的字段，累加其网络传输大小
 * - 用于预分配网络缓冲区
 *
 * 参数：
 * @param m - 消息对象
 *
 * 返回值：
 * @return 消息在网络上的总字节大小
 */
size_t
msg_get_wire_size(const msg *m)
{
	size_t sz = sizeof(msg_hdr);		// 消息头部大小

	// 遍历所有字段，计算已设置字段的网络大小
	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			sz += msg_get_field_wire_size(mf_type(mf, m->type), mf->field_sz);
		}
	}

	return sz;
}

/**
 * 计算消息模板的固定大小部分
 *
 * 功能描述：
 * - 计算模板定义的所有字段的最小网络大小
 * - 不包含可变长度数据的实际大小
 * - 用于估算最小缓冲区需求
 *
 * 参数：
 * @param mt - 消息模板数组
 * @param mt_count - 模板数组大小
 *
 * 返回值：
 * @return 固定部分的字节大小
 */
size_t
msg_get_template_fixed_sz(const msg_template *mt, size_t mt_count)
{
	size_t sz = sizeof(msg_hdr);

	for (size_t i = 0; i < mt_count; i++) {
		sz += msg_get_field_wire_size(mt[i].type, 0);
	}

	return sz;
}

/**
 * 将消息转换为scatter-gather I/O向量格式
 *
 * 功能描述：
 * - 支持零拷贝的高效网络传输
 * - 将整数类型字段打包到连续缓冲区中
 * - 为缓冲区类型字段创建独立的iovec条目
 * - 优化网络I/O性能，减少内存拷贝
 *
 * 参数：
 * @param m - 源消息对象
 * @param buf - 输出缓冲区，存储iovec数组和打包的头部/整数数据
 * @param buf_sz - 缓冲区大小
 * @param msg_sz_r - 输出参数，返回消息总大小
 *
 * 返回值：
 * @return iovec数组的元素个数
 *
 * 缓冲区布局：
 * [iovec数组] + [消息头部] + [整数字段数据] + [缓冲区字段头部]
 *
 * 关键逻辑步骤：
 * 1. 统计字段类型和计算总大小
 * 2. 验证缓冲区空间充足
 * 3. 打包消息头部和整数字段
 * 4. 为缓冲区字段创建独立的iovec条目
 */
size_t
msg_to_iov_buf(const msg *m, uint8_t *buf, size_t buf_sz, uint32_t *msg_sz_r)
{
	uint32_t body_sz = 0;		// 消息体大小
	uint32_t int_fields = 0;	// 整数字段计数
	uint32_t set_fields = 0;	// 已设置字段总数

	// 第一遍遍历：统计字段信息和计算总大小
	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_type type = mf_type(mf, m->type);

			set_fields++;
			body_sz += msg_get_field_wire_size(type, mf->field_sz);

			if (mf_type_is_int(type)) {
				int_fields++;
			}
		}
	}

	// 设置iovec数组指针和计算所需iovec数量
	struct iovec *iov = (struct iovec *)buf;
	uint32_t buf_fields = set_fields - int_fields;	// 缓冲区字段数量
	uint32_t max_iov = MAX(1, 2 * buf_fields);		// 最大iovec数量(每个缓冲区字段需要2个)

	// 验证缓冲区空间充足
	cf_assert(buf_sz >= max_iov * sizeof(struct iovec) + sizeof(msg_hdr) +
			int_fields * (sizeof(msg_field_hdr) + sizeof(uint64_t)),
			AS_FABRIC, "buf_sz %lu too small", buf_sz);

	// 设置数据写入指针，跳过iovec数组空间
	uint8_t *ptr = buf + max_iov * sizeof(struct iovec);
	msg_hdr * const hdr = (msg_hdr *)ptr;
	uint16_t first_buf = m->n_fields;

	// 设置消息头部
	hdr->size = cf_swap_to_be32(body_sz);		// 消息体大小(大端序)
	hdr->type = cf_swap_to_be16(m->type);		// 消息类型(大端序)
	iov->iov_base = ptr;						// 第一个iovec指向消息头部
	ptr += sizeof(msg_hdr);

	// 第二遍遍历：打包所有整数类型字段
	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_type type = mf_type(mf, m->type);

			if (mf_type_is_int(type)) {
				// 将整数字段写入连续缓冲区
				ptr += msg_field_write_buf(mf, type, ptr);
			}
			else if (first_buf == m->n_fields) {
				// 记录第一个缓冲区字段的位置
				first_buf = i;
			}
		}
	}

	// 设置第一个iovec的长度(包含消息头部和所有整数字段)
	iov->iov_len = ptr - (uint8_t *)iov->iov_base;

	// 如果没有缓冲区字段，直接返回
	if (buf_fields == 0) {
		*msg_sz_r = body_sz + sizeof(msg_hdr);
		return 1;
	}

	// 处理第一个缓冲区字段
	{
		const msg_field *mf = &m->f[first_buf];
		msg_field_type type = mf_type(mf, m->type);

		// 写入字段头部到连续缓冲区
		msg_field_write_hdr(mf, type, ptr);
		iov->iov_len += BUF_FIELD_HDR_SZ;
		ptr += BUF_FIELD_HDR_SZ;
		iov++;

		// 创建独立的iovec条目指向字段数据
		iov->iov_base = mf->u.any_buf;
		iov->iov_len = mf->field_sz;
		iov++;
	}

	// 处理剩余的缓冲区字段
	for (uint16_t i = first_buf + 1; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];

		if (! mf->is_set) {
			continue;
		}

		msg_field_type type = mf_type(mf, m->type);

		if (! mf_type_is_int(type)) {
			// 为字段头部创建iovec条目
			msg_field_write_hdr(mf, type, ptr);
			iov->iov_base = ptr;
			iov->iov_len = BUF_FIELD_HDR_SZ;
			ptr += BUF_FIELD_HDR_SZ;
			iov++;

			// 为字段数据创建iovec条目
			iov->iov_base = mf->u.any_buf;
			iov->iov_len = mf->field_sz;
			iov++;
		}
	}

	// 验证指针未越界
	cf_assert(ptr <= buf + buf_sz, AS_PARTICLE, "ptr out of bounds %p > buf %p + buf_sz %zu", ptr, buf, buf_sz);
	*msg_sz_r = body_sz + sizeof(msg_hdr);

	// 返回实际使用的iovec数量
	return iov - (struct iovec *)buf;
}

/**
 * 将消息序列化到连续的缓冲区中
 *
 * 功能描述：
 * - 将消息对象序列化为网络传输格式的连续字节流
 * - 先写入消息头部，再依次写入所有已设置的字段
 * - 使用网络字节序确保跨平台兼容性
 *
 * 参数：
 * @param m - 源消息对象
 * @param buf - 目标缓冲区，必须足够大
 *
 * 返回值：
 * @return 实际写入的字节数
 *
 * 关键逻辑步骤：
 * 1. 写入消息头部(类型字段)
 * 2. 遍历并写入所有已设置的字段
 * 3. 回填消息体大小到头部
 */
size_t
msg_to_wire(const msg *m, uint8_t *buf)
{
	msg_hdr *hdr = (msg_hdr *)buf;

	// 先设置消息类型(网络字节序)
	hdr->type = cf_swap_to_be16(m->type);

	buf += sizeof(msg_hdr);

	const uint8_t *body = buf;		// 记录消息体开始位置

	// 遍历所有字段，写入已设置的字段
	for (uint16_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf = &m->f[i];
		msg_field_type type = mf_type(mf, m->type);

		if (mf->is_set) {
			buf += msg_field_write_buf(mf, type, buf);
		}
	}

	// 计算消息体实际大小
	uint32_t body_sz = (uint32_t)(buf - body);

	// 回填消息体大小到头部(网络字节序)
	hdr->size = cf_swap_to_be32(body_sz);

	return sizeof(msg_hdr) + body_sz;
}


//==========================================================
// Public API - parse flattened data into messages.
// 公共API - 将扁平化数据解析为消息对象
//

/**
 * 解析网络缓冲区中的消息数据
 *
 * 功能描述：
 * - 从网络缓冲区解析消息头部和字段数据
 * - 支持零拷贝解析，字段直接指向原始缓冲区
 * - 验证消息完整性和类型匹配
 * - 设置字段指针但不拷贝数据，提高性能
 *
 * 参数：
 * @param m - 目标消息对象，类型必须与解析的消息匹配
 * @param buf - 包含序列化消息的缓冲区
 * @param bufsz - 缓冲区大小
 *
 * 返回值：
 * @return true - 解析成功
 * @return false - 解析失败(缓冲区太小、类型不匹配等)
 *
 * 关键逻辑步骤：
 * 1. 解析并验证消息头部
 * 2. 检查缓冲区大小是否足够
 * 3. 验证消息类型匹配
 * 4. 解析所有字段数据
 */
bool
msg_parse(msg *m, const uint8_t *buf, size_t bufsz)
{
	uint32_t sz;		// 消息体大小
	msg_type type;		// 消息类型

	// 解析消息头部，获取大小和类型信息
	if (! msg_parse_hdr(&sz, &type, buf, bufsz)) {
		cf_warning(CF_MSG, "msg_parse() invalid bufsz %zu too small", bufsz);
		return false;
	}

	// 检查缓冲区大小是否包含完整消息
	if (bufsz < sz + sizeof(msg_hdr)) {
		cf_warning(CF_MSG, "msg_parse() bufsz %zu < msg sz %u + %zu", bufsz, sz, sizeof(msg_hdr));
		return false;
	}

	// 验证消息类型匹配
	if (m->type != type) {
		cf_ticker_warning(CF_MSG, "parsed type %d for msg type %d", type, m->type);
		return false;
	}

	// 解析消息字段数据
	return msg_parse_fields(m, buf + sizeof(msg_hdr), sz);
}

/**
 * 解析消息头部信息
 *
 * 功能描述：
 * - 从缓冲区解析消息头部的大小和类型信息
 * - 转换网络字节序到主机字节序
 * - 验证缓冲区最小大小要求
 *
 * 参数：
 * @param size_r - 输出参数，返回消息体大小
 * @param type_r - 输出参数，返回消息类型
 * @param buf - 包含消息头部的缓冲区
 * @param sz - 缓冲区大小
 *
 * 返回值：
 * @return true - 解析成功
 * @return false - 缓冲区太小，无法包含完整头部
 */
bool
msg_parse_hdr(uint32_t *size_r, msg_type *type_r, const uint8_t *buf, size_t sz)
{
	// 检查缓冲区大小是否足够包含消息头部
	if (sz < sizeof(msg_hdr)) {
		return false;
	}

	const msg_hdr *hdr = (const msg_hdr *)buf;

	// 解析消息体大小(网络字节序转主机字节序)
	*size_r = cf_swap_from_be32(hdr->size);
	// 解析消息类型(网络字节序转主机字节序)
	*type_r = (msg_type)cf_swap_from_be16(hdr->type);

	return true;
}
}

bool
msg_parse_fields(msg *m, const uint8_t *buf, size_t sz)
{
	const uint8_t *eob = buf + sz;
	size_t left = sz;

	while (left != 0) {
		if (left < sizeof(msg_field_hdr) + sizeof(uint32_t)) {
			return false;
		}

		const msg_field_hdr *fhdr = (const msg_field_hdr *)buf;
		buf += sizeof(msg_field_hdr);

		uint32_t id = (uint32_t)cf_swap_from_be16(fhdr->id);
		msg_field_type ft = (msg_field_type)fhdr->type;
		size_t fsz;
		uint32_t fsz_sz = 0;

		switch (ft) {
		case M_FT_UINT32:
			fsz = sizeof(uint32_t);
			break;
		case M_FT_UINT64:
			fsz = sizeof(uint64_t);
			break;
		default:
			fsz = cf_swap_from_be32(*(const uint32_t *)buf);
			fsz_sz = sizeof(uint32_t);
			buf += sizeof(uint32_t);
			break;
		}

		if (left < sizeof(msg_field_hdr) + fsz_sz + fsz) {
			return false;
		}

		msg_field *mf;

		if (id >= m->n_fields) {
			mf = NULL;
		}
		else {
			mf = &m->f[id];
		}

		if (mf && ft != mf_type(mf, m->type)) {
			cf_ticker_warning(CF_MSG, "msg type %d: parsed type %d for field type %d", m->type, ft, mf_type(mf, m->type));
			mf = NULL;
		}

		if (mf) {
			mf->is_set = true;

			switch (mf_type(mf, m->type)) {
			case M_FT_UINT32:
				mf->u.ui32 = cf_swap_from_be32(*(uint32_t *)buf);
				break;
			case M_FT_UINT64:
				mf->u.ui64 = cf_swap_from_be64(*(uint64_t *)buf);
				break;
			case M_FT_STR:
			case M_FT_BUF:
			case M_FT_ARRAY_UINT32:
			case M_FT_ARRAY_UINT64:
			case M_FT_ARRAY_STR:
			case M_FT_ARRAY_BUF:
			case M_FT_MSGPACK:
				mf->field_sz = (uint32_t)fsz;
				mf->u.any_buf = (void *)buf;
				mf->is_free = false;
				break;
			default:
				cf_ticker_detail(CF_MSG, "msg_parse: field type %d not supported - skipping", mf_type(mf, m->type));
				mf->is_set = false;
				break;
			}
		}

		if (eob < buf) {
			break;
		}

		buf += fsz;
		left = (size_t)(eob - buf);
	}

	m->just_parsed = true;

	return true;
}

void
msg_reset(msg *m)
{
	m->bytes_used = (uint32_t)((m->n_fields * sizeof(msg_field)) + sizeof(msg));
	m->just_parsed = false;

	for (uint16_t i = 0; i < m->n_fields; i++) {
		mf_destroy(&m->f[i]);
	}
}

void
msg_preserve_fields(msg *m, uint32_t n_field_ids, ...)
{
	bool reflect[m->n_fields];

	for (uint16_t i = 0; i < m->n_fields; i++) {
		reflect[i] = false;
	}

	va_list argp;
	va_start(argp, n_field_ids);

	for (uint32_t n = 0; n < n_field_ids; n++) {
		reflect[va_arg(argp, int)] = true;
	}

	va_end(argp);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			if (reflect[i]) {
				if (m->just_parsed) {
					msg_field_save(m, mf);
				}
			}
			else {
				mf->is_set = false;
			}
		}
	}

	m->just_parsed = false;
}

void
msg_preserve_all_fields(msg *m)
{
	if (! m->just_parsed) {
		return;
	}

	for (uint32_t i = 0; i < m->n_fields; i++) {
		msg_field *mf = &m->f[i];

		if (mf->is_set) {
			msg_field_save(m, mf);
		}
	}

	m->just_parsed = false;
}


//==========================================================
// Public API - set fields in messages.
// 公共API - 设置消息字段
//

/**
 * 设置32位无符号整数字段
 *
 * 功能描述：
 * - 直接设置字段值，无需内存分配
 * - 标记字段为已设置状态
 *
 * 参数：
 * @param m - 目标消息对象
 * @param field_id - 字段ID
 * @param v - 要设置的32位无符号整数值
 */
void
msg_set_uint32(msg *m, int field_id, uint32_t v)
{
	m->f[field_id].is_set = true;		// 标记字段已设置
	m->f[field_id].u.ui32 = v;			// 设置字段值
}

/**
 * 设置64位无符号整数字段
 *
 * 功能描述：
 * - 直接设置字段值，无需内存分配
 * - 标记字段为已设置状态
 *
 * 参数：
 * @param m - 目标消息对象
 * @param field_id - 字段ID
 * @param v - 要设置的64位无符号整数值
 */
void
msg_set_uint64(msg *m, int field_id, uint64_t v)
{
	m->f[field_id].is_set = true;		// 标记字段已设置
	m->f[field_id].u.ui64 = v;			// 设置字段值
}

/**
 * 设置字符串字段
 *
 * 功能描述：
 * - 支持两种设置模式：拷贝模式和移交模式
 * - 拷贝模式：优先使用消息内置缓冲区，不足时分配新内存
 * - 移交模式：直接使用传入的指针，调用者负责内存生命周期
 * - 自动清理旧字段数据，防止内存泄漏
 *
 * 参数：
 * @param m - 目标消息对象
 * @param field_id - 字段ID
 * @param v - 要设置的字符串指针
 * @param type - 设置类型（MSG_SET_COPY或MSG_SET_HANDOFF_MALLOC）
 *
 * 内存管理策略：
 * - MSG_SET_COPY: 优先使用消息scratch空间，不足时动态分配
 * - MSG_SET_HANDOFF_MALLOC: 直接使用传入指针，标记需要释放
 */
void
msg_set_str(msg *m, int field_id, const char *v, msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	// 清理旧字段数据，防止内存泄漏
	mf_destroy(mf);

	// 计算字符串大小(包含null终止符)
	mf->field_sz = (uint32_t)strlen(v) + 1;

	if (type == MSG_SET_COPY) {
		uint32_t fsz = mf->field_sz;

		// 检查消息内置缓冲区是否有足够空间
		if (m->bytes_alloc - m->bytes_used >= fsz) {
			// 使用消息内置缓冲区，性能更好且无需单独释放
			mf->u.str = (char *)m + m->bytes_used;
			m->bytes_used += fsz;
			mf->is_free = false;		// 不需要单独释放
			memcpy(mf->u.str, v, fsz);
		}
		else {
			// 内置缓冲区不足，动态分配新内存
			mf->u.str = cf_strdup(v);
			mf->is_free = true;			// 需要释放标志
		}
	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		// 移交模式：直接使用传入的指针
		mf->u.str = (char *)v;
		mf->is_free = true;				// 标记需要释放
	}

	mf->is_set = true;					// 标记字段已设置
}

/**
 * 设置二进制缓冲区字段
 *
 * 功能描述：
 * - 支持二进制数据的设置，类似字符串但无需null终止符
 * - 支持拷贝和移交两种内存管理模式
 * - 优先使用消息内置缓冲区提高性能
 *
 * 参数：
 * @param m - 目标消息对象
 * @param field_id - 字段ID
 * @param v - 要设置的二进制数据指针
 * @param sz - 数据大小(字节数)
 * @param type - 设置类型（MSG_SET_COPY或MSG_SET_HANDOFF_MALLOC）
 *
 * 内存管理策略：
 * - MSG_SET_COPY: 拷贝数据到消息缓冲区或动态分配内存
 * - MSG_SET_HANDOFF_MALLOC: 直接使用传入指针，调用者负责生命周期
 */
void
msg_set_buf(msg *m, int field_id, const uint8_t *v, size_t sz,
		msg_set_type type)
{
	msg_field *mf = &m->f[field_id];

	// 清理旧字段数据
	mf_destroy(mf);

	// 设置字段大小
	mf->field_sz = (uint32_t)sz;

	if (type == MSG_SET_COPY) {
		// 检查消息内置缓冲区空间
		if (m->bytes_alloc - m->bytes_used >= sz) {
			// 使用内置缓冲区
			mf->u.buf = (uint8_t *)m + m->bytes_used;
			m->bytes_used += (uint32_t)sz;
			mf->is_free = false;
		}
		else {
			// 动态分配内存
			mf->u.buf = cf_malloc(sz);
			mf->is_free = true;
		}

		// 拷贝数据
		memcpy(mf->u.buf, v, sz);
	}
	else if (type == MSG_SET_HANDOFF_MALLOC) {
		// 移交模式：直接使用传入指针
		mf->u.buf = (void *)v;
		mf->is_free = true;
	}

	mf->is_set = true;		// 标记字段已设置
}
}

void
msg_set_uint32_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint32_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint32_t));
	mf->u.ui32_a = cf_malloc(mf->field_sz);
	mf->is_set = true;
	mf->is_free = true;
}

void
msg_set_uint32_array(msg *m, int field_id, uint32_t idx, uint32_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint32_array() field not set");
	cf_assert(idx < (mf->field_sz >> 2), CF_MSG, "msg_set_uint32_array() idx out of bounds");

	mf->u.ui32_a[idx] = cf_swap_to_be32(v);
}

void
msg_set_uint64_array_size(msg *m, int field_id, uint32_t count)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(! mf->is_set, CF_MSG, "msg_set_uint64_array_size() field already set");

	mf->field_sz = (uint32_t)(count * sizeof(uint64_t));
	mf->u.ui64_a = cf_malloc(mf->field_sz);
	mf->is_set = true;
	mf->is_free = true;
}

void
msg_set_uint64_array(msg *m, int field_id, uint32_t idx, uint64_t v)
{
	msg_field *mf = &m->f[field_id];

	cf_assert(mf->is_set, CF_MSG, "msg_set_uint64_array() field not set");
	cf_assert(idx < (mf->field_sz >> 3), CF_MSG, "msg_set_uint64_array() idx out of bounds");

	mf->u.ui64_a[idx] = cf_swap_to_be64(v);
}

void
msg_msgpack_list_set_uint32(msg *m, int field_id, const uint32_t *buf,
		uint32_t count)
{
	msg_field *mf = &m->f[field_id];
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		a_sz += as_pack_uint64_size((uint64_t)buf[i]);
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		e = as_pack_uint64(&pk, (uint64_t)buf[i]);
		cf_assert(e == 0, CF_MSG, "as_pack_str failed");
	}

	mf->is_free = true;
	mf->is_set = true;
}

void
msg_msgpack_list_set_uint64(msg *m, int field_id, const uint64_t *buf,
		uint32_t count)
{
	msg_field *mf = &m->f[field_id];
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		a_sz += as_pack_uint64_size(buf[i]);
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		e = as_pack_uint64(&pk, buf[i]);
		cf_assert(e == 0, CF_MSG, "as_pack_str failed");
	}

	mf->is_free = true;
	mf->is_set = true;
}

void
msg_msgpack_list_set_buf(msg *m, int field_id, const cf_vector *v)
{
	msg_field *mf = &m->f[field_id];
	uint32_t count = cf_vector_size(v);
	uint32_t a_sz = as_pack_list_header_get_size(count);

	mf_destroy(mf);

	for (uint32_t i = 0; i < count; i++) {
		const msg_buf_ele *ele = cf_vector_getp((cf_vector *)v, i);

		if (! ele->ptr) {
			a_sz++; // TODO - add to common later
		}
		else {
			a_sz += as_pack_str_size(ele->sz);
		}
	}

	mf->field_sz = a_sz;
	mf->u.any_buf = cf_malloc(a_sz);

	as_packer pk = {
			.buffer = mf->u.any_buf,
			.offset = 0,
			.capacity = (int)a_sz,
	};

	int e = as_pack_list_header(&pk, count);

	cf_assert(e == 0, CF_MSG, "as_pack_list_header failed");

	for (uint32_t i = 0; i < count; i++) {
		const msg_buf_ele *ele = cf_vector_getp((cf_vector *)v, i);

		if (! ele->ptr) {
			pk.buffer[pk.offset++] = 0xc0; // TODO - add to common later
		}
		else {
			e = as_pack_str(&pk, ele->ptr, ele->sz);
			cf_assert(e == 0, CF_MSG, "as_pack_str failed");
		}
	}

	mf->is_free = true;
	mf->is_set = true;
}


//==========================================================
// Public API - get fields from messages.
// 公共API - 从消息中获取字段
//

/**
 * 获取消息字段的类型定义
 *
 * 参数：
 * @param m - 消息对象
 * @param field_id - 字段ID
 *
 * 返回值：
 * @return 字段的类型枚举值
 */
msg_field_type
msg_field_get_type(const msg *m, int field_id)
{
	return mf_type(&m->f[field_id], m->type);
}

/**
 * 检查消息字段是否已设置
 *
 * 功能描述：
 * - 检查指定字段是否包含有效数据
 * - 验证字段ID的有效性
 *
 * 参数：
 * @param m - 消息对象
 * @param field_id - 字段ID
 *
 * 返回值：
 * @return true - 字段已设置
 * @return false - 字段未设置
 */
bool
msg_is_set(const msg *m, int field_id)
{
	// 验证字段ID有效性
	cf_assert(field_id >= 0 && field_id < (int)m->n_fields, CF_MSG, "invalid field_id %d", field_id);

	return m->f[field_id].is_set;
}

/**
 * 获取32位无符号整数字段值
 *
 * 功能描述：
 * - 从消息中提取32位无符号整数字段的值
 * - 检查字段是否已设置
 *
 * 参数：
 * @param m - 消息对象
 * @param field_id - 字段ID
 * @param val_r - 输出参数，存储获取的值
 *
 * 返回值：
 * @return 0 - 获取成功
 * @return -1 - 字段未设置
 */
int
msg_get_uint32(const msg *m, int field_id, uint32_t *val_r)
{
	// 检查字段是否已设置
	if (! m->f[field_id].is_set) {
		return -1;
	}

	// 返回字段值
	*val_r = m->f[field_id].u.ui32;

	return 0;
}

/**
 * 获取64位无符号整数字段值
 *
 * 功能描述：
 * - 从消息中提取64位无符号整数字段的值
 * - 检查字段是否已设置
 *
 * 参数：
 * @param m - 消息对象
 * @param field_id - 字段ID
 * @param val_r - 输出参数，存储获取的值
 *
 * 返回值：
 * @return 0 - 获取成功
 * @return -1 - 字段未设置
 */
int
msg_get_uint64(const msg *m, int field_id, uint64_t *val_r)
{
	// 检查字段是否已设置
	if (! m->f[field_id].is_set) {
		return -1;
	}

	// 返回字段值
	*val_r = m->f[field_id].u.ui64;

	return 0;
}

/**
 * 获取字符串字段值
 *
 * 功能描述：
 * - 从消息中获取字符串字段，支持直接访问和拷贝两种模式
 * - 验证字符串格式的有效性(null终止)
 * - 根据获取类型决定内存管理策略
 *
 * 参数：
 * @param m - 消息对象
 * @param field_id - 字段ID
 * @param str_r - 输出参数，存储字符串指针
 * @param type - 获取类型（MSG_GET_DIRECT或MSG_GET_COPY_MALLOC）
 *
 * 返回值：
 * @return 0 - 获取成功
 * @return -1 - 字段未设置或格式无效
 *
 * 获取模式：
 * - MSG_GET_DIRECT: 直接返回内部指针，高效但需注意生命周期
 * - MSG_GET_COPY_MALLOC: 分配新内存并拷贝，调用者负责释放
 */
int
msg_get_str(const msg *m, int field_id, char **str_r, msg_get_type type)
{
	// 检查字段是否已设置
	if (! m->f[field_id].is_set) {
		return -1;
	}

	uint32_t sz = m->f[field_id].field_sz;

	// 验证字符串格式：非空且以null结尾
	if (sz == 0 || m->f[field_id].u.str[sz - 1] != '\0') {
		cf_warning(CF_MSG, "msg_get_str: invalid string");
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		// 直接模式：返回内部指针，高效但需注意生命周期
		*str_r = m->f[field_id].u.str;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		// 拷贝模式：分配新内存并拷贝字符串，调用者负责释放
		*str_r = cf_strdup(m->f[field_id].u.str);
	}
	else {
		cf_crash(CF_MSG, "msg_get_str: illegal msg_get_type");
	}

	return 0;
}
}

int
msg_get_buf(const msg *m, int field_id, uint8_t **buf_r, size_t *sz_r,
		msg_get_type type)
{
	if (! m->f[field_id].is_set) {
		return -1;
	}

	if (type == MSG_GET_DIRECT) {
		*buf_r = m->f[field_id].u.buf;
	}
	else if (type == MSG_GET_COPY_MALLOC) {
		*buf_r = cf_malloc(m->f[field_id].field_sz);
		memcpy(*buf_r, m->f[field_id].u.buf, m->f[field_id].field_sz);
	}
	else {
		cf_crash(CF_MSG, "msg_get_buf: illegal msg_get_type");
	}

	if (sz_r) {
		*sz_r = m->f[field_id].field_sz;
	}

	return 0;
}

int
msg_get_uint32_array(const msg *m, int field_id, uint32_t index,
		uint32_t *val_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be32(mf->u.ui32_a[index]);

	return 0;
}

int
msg_get_uint64_array_count(const msg *m, int field_id, uint32_t *count_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*count_r = mf->field_sz >> 3;

	return 0;
}

int
msg_get_uint64_array(const msg *m, int field_id, uint32_t index,
		uint64_t *val_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return -1;
	}

	*val_r = cf_swap_from_be64(mf->u.ui64_a[index]);

	return 0;
}

bool
msg_msgpack_list_get_count(const msg *m, int field_id, uint32_t *count_r)
{
	msgpack_in mp;

	return msgpack_list_unpack_hdr(&mp, m, field_id, count_r);
}

bool
msg_msgpack_list_get_uint32_array(const msg *m, int field_id, uint32_t *buf_r,
		uint32_t *count_r)
{
	cf_assert(buf_r, CF_MSG, "buf_r is null");

	msgpack_in mp;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&mp, m, field_id, &count)) {
		return false;
	}

	if (*count_r < count) {
		cf_warning(CF_MSG, "count_r %u < %u - too small", *count_r, count);
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint64_t val;


		if (! msgpack_get_uint64(&mp, &val)
				|| (val & (0xFFFFffffUL << 32)) != 0) {
			cf_warning(CF_MSG, "i %u/%u invalid packed uint32 val 0x%lx", i, count, val);
			return false;
		}

		buf_r[i] = (uint32_t)val;
	}

	*count_r = count;

	return true;
}

bool
msg_msgpack_list_get_uint64_array(const msg *m, int field_id, uint64_t *buf_r,
		uint32_t *count_r)
{
	cf_assert(buf_r, CF_MSG, "buf_r is null");

	msgpack_in mp;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&mp, m, field_id, &count)) {
		return false;
	}

	if (*count_r < count) {
		cf_warning(CF_MSG, "count_r %u < %u - too small", *count_r, count);
		return false;
	}

	for (uint32_t i = 0; i < count; i++) {
		uint64_t val;

		if (! msgpack_get_uint64(&mp, &val)) {
			cf_warning(CF_MSG, "i %u/%u invalid packed uint64 val 0x%lx", i, count, val);
			return false;
		}

		buf_r[i] = val;
	}

	*count_r = count;

	return true;
}

bool
msg_msgpack_list_get_buf_array(const msg *m, int field_id, cf_vector *v_r,
		bool init_vec)
{
	msgpack_in mp;
	uint32_t count;

	if (! msgpack_list_unpack_hdr(&mp, m, field_id, &count)) {
		return false;
	}

	if (init_vec) {
		cf_vector_init(v_r, sizeof(msg_buf_ele), count, 0);
	}
	else {
		cf_assert(count <= cf_vector_capacity(v_r), CF_MSG, "count %u > vector cap %u", count, cf_vector_capacity(v_r));
	}

	for (uint32_t i = 0; i < count; i++) {
		msg_buf_ele ele;
		int saved_offset = mp.offset;

		ele.ptr = (uint8_t *)msgpack_get_bin(&mp, &ele.sz);

		if (! ele.ptr) {
			mp.offset = saved_offset;
			ele.sz = 0;

			if (msgpack_sz(&mp) == 0) {
				if (init_vec) {
					cf_vector_destroy(v_r);
				}

				cf_warning(CF_MSG, "i %u/%u invalid packed buf", i, count);

				return false;
			}
		}

		cf_vector_append(v_r, &ele);
	}

	return true;
}


//==========================================================
// Public API - debugging only.
//

void
msg_dump(const msg *m, const char *info)
{
	cf_info(CF_MSG, "msg_dump: %s: msg %p rc %u n-fields %u bytes-used %u bytes-alloc'd %u type %d",
			info, m, cf_rc_count((void*)m), m->n_fields, m->bytes_used,
			m->bytes_alloc, m->type);

	for (uint32_t i = 0; i < m->n_fields; i++) {
		const msg_field *mf =  &m->f[i];

		cf_info(CF_MSG, "mf %02u: id %u is-set %d", i, mf->id, mf->is_set);

		if (mf->is_set) {
			switch (mf_type(mf, m->type)) {
			case M_FT_UINT32:
				cf_info(CF_MSG, "   type UINT32 value %u", mf->u.ui32);
				break;
			case M_FT_UINT64:
				cf_info(CF_MSG, "   type UINT64 value %lu", mf->u.ui64);
				break;
			case M_FT_STR:
				cf_info(CF_MSG, "   type STR sz %u free %c value %s",
						mf->field_sz, mf->is_free ? 't' : 'f', mf->u.str);
				break;
			case M_FT_BUF:
				cf_info(CF_MSG, "   type BUF sz %u free %c value\n%*pH",
						mf->field_sz, mf->is_free ? 't' : 'f', mf->field_sz,
								mf->u.buf);
				break;
			case M_FT_ARRAY_UINT32:
				cf_info(CF_MSG, "   type ARRAY_UINT32: count %u n-uint32 %u free %c",
						mf->field_sz, mf->field_sz >> 2,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 2;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %u",
								j, cf_swap_from_be32(mf->u.ui32_a[j]));
					}
				}
				break;
			case M_FT_ARRAY_UINT64:
				cf_info(CF_MSG, "   type ARRAY_UINT64: count %u n-uint64 %u free %c",
						mf->field_sz, mf->field_sz >> 3,
						mf->is_free ? 't' : 'f');
				{
					uint32_t n_ints = mf->field_sz >> 3;
					for (uint32_t j = 0; j < n_ints; j++) {
						cf_info(CF_MSG, "      idx %u value %lu",
								j, cf_swap_from_be64(mf->u.ui64_a[j]));
					}
				}
				break;
			default:
				cf_info(CF_MSG, "   type %d unknown", mf_type(mf, m->type));
				break;
			}
		}
	}
}


//==========================================================
// Local helpers.
//

static uint32_t
msg_get_field_wire_size(msg_field_type type, uint32_t field_sz)
{
	switch (type) {
	case M_FT_UINT32:
		return sizeof(msg_field_hdr) + sizeof(uint32_t);
	case M_FT_UINT64:
		return sizeof(msg_field_hdr) + sizeof(uint64_t);
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
		break;
	}

	return BUF_FIELD_HDR_SZ + field_sz;
}

static uint32_t
msg_field_write_hdr(const msg_field *mf, msg_field_type type, uint8_t *buf)
{
	msg_field_hdr *hdr = (msg_field_hdr *)buf;

	buf += sizeof(msg_field_hdr);

	hdr->id = cf_swap_to_be16((uint16_t)mf->id);
	hdr->type = (uint8_t)type;

	switch (type) {
	case M_FT_UINT32:
		*(uint32_t *)buf = cf_swap_to_be32(mf->u.ui32);
		return sizeof(msg_field_hdr) + sizeof(uint32_t);
	case M_FT_UINT64:
		*(uint64_t *)buf = cf_swap_to_be64(mf->u.ui64);
		return sizeof(msg_field_hdr) + sizeof(uint64_t);
	default:
		break;
	}

	switch (type) {
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		*(uint32_t *)buf = cf_swap_to_be32(mf->field_sz);
		break;
	default:
		cf_crash(CF_MSG, "unexpected field type %d", type);
	}

	return 0; // incomplete
}

// Returns the number of bytes written.
static uint32_t
msg_field_write_buf(const msg_field *mf, msg_field_type type, uint8_t *buf)
{
	uint32_t hdr_sz = msg_field_write_hdr(mf, type, buf);

	if (hdr_sz != 0) {
		return hdr_sz;
	}

	memcpy(buf + BUF_FIELD_HDR_SZ, mf->u.any_buf, mf->field_sz);

	return (uint32_t)(BUF_FIELD_HDR_SZ + mf->field_sz);
}

static void
msg_field_save(msg *m, msg_field *mf)
{
	switch (mf_type(mf, m->type)) {
	case M_FT_UINT32:
	case M_FT_UINT64:
		break;
	case M_FT_STR:
	case M_FT_BUF:
	case M_FT_ARRAY_UINT32:
	case M_FT_ARRAY_UINT64:
	case M_FT_ARRAY_STR:
	case M_FT_ARRAY_BUF:
	case M_FT_MSGPACK:
		// Should only preserve received messages where buffer pointers point
		// directly into a fabric buffer.
		cf_assert(! mf->is_free, CF_MSG, "invalid msg preserve");

		if (m->bytes_alloc - m->bytes_used >= mf->field_sz) {
			void *buf = ((uint8_t *)m) + m->bytes_used;

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			m->bytes_used += mf->field_sz;
			mf->is_free = false;
		}
		else {
			void *buf = cf_malloc(mf->field_sz);

			memcpy(buf, mf->u.any_buf, mf->field_sz);
			mf->u.any_buf = buf;
			mf->is_free = true;
		}
		break;
	default:
		break;
	}
}

static bool
msgpack_list_unpack_hdr(msgpack_in *mp, const msg *m, int field_id,
		uint32_t *count_r)
{
	const msg_field *mf = &m->f[field_id];

	if (! mf->is_set) {
		return false;
	}

	mp->buf = (const uint8_t *)mf->u.any_buf;
	mp->offset = 0;
	mp->buf_sz = mf->field_sz;

	uint32_t count;

	if (! msgpack_get_list_ele_count(mp, &count)) {
		cf_ticker_warning(CF_MSG, "invalid packed list");
		return false;
	}

	*count_r = count;

	return true;
}
