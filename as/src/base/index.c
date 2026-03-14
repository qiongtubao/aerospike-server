/*
 * index.c
 *
 * Copyright (C) 2012-2021 Aerospike, Inc.
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
 * ======================================================
 * 主索引模块 (Primary Index Module)
 * ======================================================
 *
 * 本模块实现了Aerospike数据库的主索引系统，基于红黑树结构设计，
 * 为每个记录提供高效的索引查找、插入和删除操作。
 *
 * 核心设计特点：
 * - 分片式红黑树：将索引分为多个sprig（小分支），减少锁竞争
 * - 并发安全：使用读写锁机制保证并发访问的安全性
 * - 内存管理：基于Arena内存池进行高效内存分配
 * - 垃圾回收：异步垃圾回收机制避免阻塞主线程
 *
 * 索引结构层次：
 * 1. as_index_tree: 索引树的顶层结构，包含多个sprig
 * 2. as_index_sprig: 索引分片，每个分片是一个独立的红黑树
 * 3. as_index: 单个索引记录，包含记录的摘要和元数据
 *
 * 并发控制策略：
 * - 每个锁对（lock_pair）包含主锁和reduce锁
 * - 主锁用于保护sprig的结构性操作（插入、删除）
 * - reduce锁用于保护遍历操作，避免与结构性操作冲突
 * - 锁的粒度设计为多个sprig共享一个锁对，平衡性能和内存开销
 */

//==========================================================
// Includes.
//

#include "base/index.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "aerospike/as_arch.h"
#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/set_index.h"
#include "base/stats.h"
#include "sindex/gc.h"
#include "sindex/sindex.h"


//==========================================================
// 类型定义和常量 (Typedefs & constants)
//

/**
 * 索引元素栈结构 - 用于红黑树操作时保存父子关系
 * 在树的插入、删除和重平衡操作中，需要追踪从根到目标节点的路径，
 * 此结构体作为栈元素保存这些路径信息
 */
typedef struct as_index_ele_s {
	struct as_index_ele_s* parent;  // 指向父节点的栈元素
	cf_arenax_handle me_h;          // 当前节点的arena句柄
	as_index* me;                   // 指向当前索引节点的指针
} as_index_ele;


//==========================================================
// 全局变量 (Globals)
//

/**
 * 全局垃圾回收队列
 * 存储待销毁的索引树指针，由专门的垃圾回收线程异步处理，
 * 避免在主业务线程中执行耗时的树销毁操作
 */
static cf_queue g_gc_queue;


//==========================================================
// 前向声明 (Forward declarations)
//

// 垃圾回收相关函数
static void* run_index_tree_gc(void* unused);  // 垃圾回收线程主函数
static void as_index_tree_destroy(as_index_tree* tree);  // 销毁索引树

// sprig遍历和缩减操作函数
static bool as_index_sprig_reduce(as_index_sprig* isprig, const cf_digest* keyd, as_index_reduce_fn cb, void* udata);  // 遍历sprig并执行回调
static void as_index_sprig_traverse(as_index_sprig* isprig, const cf_digest* keyd, cf_arenax_handle r_h, as_index_ph_array* ph_a);  // 深度优先遍历sprig
static void as_index_sprig_traverse_purge(as_index_sprig* isprig, cf_arenax_handle r_h);  // 销毁时的完全遍历

// sprig操作的核心函数
static int as_index_sprig_get_insert_vlock(as_index_sprig* isprig, uint8_t tree_id, const cf_digest* keyd, as_index_ref* index_ref);  // 查找或插入元素

// sprig内部操作函数
static int as_index_sprig_search_lockless(as_index_sprig* isprig, const cf_digest* keyd, as_index** ret, cf_arenax_handle* ret_h);  // 无锁搜索
static void as_index_sprig_insert_rebalance(as_index_sprig* isprig, as_index* root_parent, as_index_ele* ele);  // 插入后的红黑树重平衡
static void as_index_sprig_delete_rebalance(as_index_sprig* isprig, as_index* root_parent, as_index_ele* ele);  // 删除后的红黑树重平衡
static void as_index_rotate_left(as_index_ele* a, as_index_ele* b);   // 红黑树左旋操作
static void as_index_rotate_right(as_index_ele* a, as_index_ele* b);  // 红黑树右旋操作

/**
 * 根据sprig索引初始化sprig信息结构
 *
 * @param tree 索引树指针
 * @param isprig 待初始化的sprig信息结构
 * @param sprig_i sprig的索引号
 *
 * 功能说明：
 * 1. 计算对应的锁索引（多个sprig可能共享同一个锁对）
 * 2. 设置sprig的基本信息（析构函数、arena、锁等）
 * 3. 建立sprig与其对应的内存池的关联
 */
static inline void
as_index_sprig_from_i(as_index_tree* tree, as_index_sprig* isprig,
		uint32_t sprig_i)
{
	// 计算锁索引：通过位移操作将sprig索引映射到锁索引
	// 多个sprig共享一个锁对，减少锁的数量和内存开销
	uint32_t lock_i = sprig_i >>
			(tree->shared->locks_shift - tree->shared->sprigs_shift);

	// 设置sprig的析构函数和用户数据，用于记录销毁时的清理工作
	isprig->destructor = tree->shared->destructor;
	isprig->destructor_udata = tree->shared->destructor_udata;

	// 设置arena内存管理器，所有记录都在此arena中分配
	isprig->arena = tree->shared->arena;

	// 设置对应的锁对（包含主锁和reduce锁）
	isprig->pair = tree_locks(tree) + lock_i;

	// 设置sprig根节点指针
	isprig->sprig = tree_sprigs(tree) + sprig_i;

	// 设置对应的内存池，用于优化内存分配性能
	isprig->puddle = tree_puddle_for_sprig(tree, sprig_i);
}


//==========================================================
// 公共API - 垃圾回收系统 (Public API - garbage collection system)
//

/**
 * 初始化索引树垃圾回收系统
 *
 * 功能说明：
 * 1. 初始化全局垃圾回收队列，容量为4096个树指针
 * 2. 创建独立的垃圾回收线程，避免阻塞主业务线程
 * 3. 垃圾回收线程持续监听队列，异步处理树的销毁工作
 */
void
as_index_tree_gc_init()
{
	cf_queue_init(&g_gc_queue, sizeof(as_index_tree*), 4096, true);
	cf_thread_create_detached(run_index_tree_gc, NULL);
}

/**
 * 获取垃圾回收队列的当前大小
 *
 * @return 队列中待处理的索引树数量
 *
 * 用途：
 * - 监控系统负载，评估垃圾回收的积压情况
 * - 用于统计和调试目的
 */
uint32_t
as_index_tree_gc_queue_size()
{
	return cf_queue_sz(&g_gc_queue);
}

/**
 * 将索引树加入垃圾回收队列
 *
 * @param tree 待回收的索引树指针
 *
 * 功能说明：
 * - 将树指针加入队列，由垃圾回收线程异步销毁
 * - 避免在主线程中执行耗时的树遍历和内存释放操作
 * - 提高系统响应性能，特别是在处理大型索引树时
 */
void
as_index_tree_gc(as_index_tree* tree)
{
	cf_queue_push(&g_gc_queue, &tree);
}


//==========================================================
// 公共API - 创建/销毁/获取树大小 (Public API - create/destroy/size a tree)
//

/**
 * 创建新的红黑树索引
 *
 * @param shared 共享的树配置信息
 * @param id 树的标识符
 * @param cb 树销毁时的回调函数
 * @param udata 回调函数的用户数据
 * @return 新创建的索引树指针
 *
 * 功能说明：
 * 1. 计算所需内存大小（锁、sprig、内存池）
 * 2. 使用引用计数分配内存，支持安全的多线程访问
 * 3. 初始化所有锁对（主锁和reduce锁）
 * 4. 初始化所有sprig和内存池为空状态
 */
as_index_tree*
as_index_tree_create(as_index_tree_shared* shared, uint8_t id,
		as_index_tree_done_fn cb, void* udata)
{
	// 计算各组件所需的内存大小
	size_t locks_size = sizeof(cf_mutex) * NUM_LOCK_PAIRS * 2;  // 每个锁对包含两个锁
	size_t sprigs_size = sizeof(as_sprig) * shared->n_sprigs;   // sprig数组
	size_t puddles_size = tree_puddles_size(shared);            // 内存池数组

	// 总内存大小 = 基础结构 + 锁 + sprig + 内存池
	size_t tree_size = sizeof(as_index_tree) +
			locks_size + sprigs_size + puddles_size;

	// 使用引用计数分配内存，确保线程安全
	as_index_tree* tree = cf_rc_alloc(tree_size);

	// 设置树的基本属性
	tree->id = id;
	tree->done_cb = cb;      // 销毁时的回调函数
	tree->udata = udata;     // 用户数据

	tree->shared = shared;   // 共享配置
	tree->n_elements = 0;    // 初始元素数量为0

	// 初始化集合树锁和数组
	cf_mutex_init(&tree->set_trees_lock);
	memset(tree->set_trees, 0, sizeof(tree->set_trees));

	// 初始化所有锁对
	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_init(&pair->lock);        // 主锁，保护结构性操作
		cf_mutex_init(&pair->reduce_lock); // reduce锁，保护遍历操作
		pair++;
	}

	// 初始化树为空状态
	memset(tree_sprigs(tree), 0, sprigs_size);   // 清空所有sprig
	memset(tree_puddles(tree), 0, puddles_size); // 清空所有内存池

	return tree;
}

/**
 * 在系统关闭时阻塞所有记录锁
 *
 * @param tree 要阻塞的索引树
 *
 * 功能说明：
 * - 获取所有锁对的主锁，防止新的操作开始
 * - 用于系统优雅关闭，确保所有操作完成后再销毁资源
 * - 注意：此操作会永久持有锁，仅用于关闭流程
 */
void
as_index_tree_block(as_index_tree* tree)
{
	if (tree == NULL) {
		return;
	}

	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	// 获取所有主锁，阻塞后续操作
	while (pair < pair_end) {
		cf_mutex_lock(&pair->lock);
		pair++;
	}
}

/**
 * 增加索引树的引用计数
 *
 * @param tree 要增加引用的索引树
 *
 * 功能说明：
 * - 线程安全地增加引用计数
 * - 确保树在被引用期间不会被销毁
 * - 配合release函数使用，实现自动内存管理
 */
void
as_index_tree_reserve(as_index_tree* tree)
{
	if (tree != NULL) {
		cf_rc_reserve(tree);
	}
}

/**
 * 释放索引树的引用并在必要时销毁
 *
 * @param ns 命名空间指针
 * @param tree 要释放的索引树
 *
 * 功能说明：
 * 1. 减少引用计数，如果计数未降到0则直接返回
 * 2. 销毁所有关联的集合索引
 * 3. 根据是否存在二级索引选择销毁路径：
 *    - 有二级索引：交给二级索引GC处理（需要清理二级索引引用）
 *    - 无二级索引：直接交给主索引GC处理
 */
void
as_index_tree_release(as_namespace* ns, as_index_tree* tree)
{
	if (tree == NULL) {
		return;
	}

	// 减少引用计数，如果还有其他引用则返回
	if (cf_rc_release(tree) != 0) {
		return;
	}

	// 销毁所有集合索引树
	as_set_index_destroy_all(tree);

	// TODO - 如果树为空可以直接调用as_index_tree_destroy()优化

	// 根据二级索引数量选择销毁路径
	if (as_sindex_n_sindexes(ns) != 0) {
		// 存在二级索引，需要特殊处理以清理二级索引中的引用
		as_sindex_gc_tree(ns, tree);
	}
	else {
		// 无二级索引，直接进入常规垃圾回收流程
		as_index_tree_gc(tree);
	}
}

/**
 * 获取树中元素的数量
 *
 * @param tree 索引树指针
 * @return 树中的元素数量，如果树为空返回0
 *
 * 功能说明：
 * - 返回树中当前的记录总数
 * - 原子操作，线程安全
 * - 用于统计和监控目的
 */
uint64_t
as_index_tree_size(as_index_tree* tree)
{
	return tree == NULL ? 0 : tree->n_elements;
}


//==========================================================
// 公共API - 遍历树 (Public API - reduce a tree)
//

/**
 * 对树中的每个元素执行回调函数，在树锁外部操作
 *
 * @param tree 索引树指针
 * @param cb 回调函数指针
 * @param udata 传递给回调函数的用户数据
 * @return true表示遍历完成，false表示回调函数要求停止遍历
 *
 * 功能说明：
 * - 从树的开头开始完整遍历所有记录
 * - 内部调用as_index_reduce_from实现
 * - 遍历顺序：从最大摘要到最小摘要（为rapid rebalance保持精确顺序）
 */
bool
as_index_reduce(as_index_tree* tree, as_index_reduce_fn cb, void* udata)
{
	return as_index_reduce_from(tree, NULL, cb, udata);
}

/**
 * 从指定边界摘要开始遍历树中的元素
 *
 * @param tree 索引树指针
 * @param keyd 起始边界摘要，NULL表示从最大摘要开始
 * @param cb 回调函数指针
 * @param udata 传递给回调函数的用户数据
 * @return true表示遍历完成，false表示回调函数要求停止遍历
 *
 * 功能说明：
 * 1. 计算起始sprig索引（基于边界摘要）
 * 2. 从起始sprig开始，按照从大到小的顺序遍历所有sprig
 * 3. 根据是否启用puddle选择不同的遍历策略：
 *    - 无puddle：使用标准的引用计数遍历
 *    - 有puddle：使用无引用计数的优化遍历
 * 4. 遍历顺序确保rapid rebalance的正确性
 */
bool
as_index_reduce_from(as_index_tree* tree, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	if (tree == NULL) {
		return true;
	}

	// 按从最大到最小摘要的顺序遍历sprig，为整个树保持此顺序
	// (Rapid rebalance需要精确的顺序)

	// 确定起始sprig索引
	uint32_t start_sprig_i = keyd == NULL ?
			tree->shared->n_sprigs - 1 : as_index_sprig_i_from_keyd(tree, keyd);

	// 从起始sprig开始，倒序遍历所有sprig
	for (int i = (int)start_sprig_i; i >= 0; i--) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, (uint32_t)i);

		// 根据puddle配置选择遍历策略
		if (tree->shared->puddles_offset == 0) {
			// 无puddle：使用标准引用计数遍历
			if (! as_index_sprig_reduce(&isprig, keyd, cb, udata)) {
				return false;
			}
		}
		else {
			// 有puddle：使用无引用计数的优化遍历
			if (! as_index_sprig_reduce_no_rc(&isprig, keyd, cb, udata)) {
				return false;
			}
		}

		keyd = NULL; // 只有第一个sprig需要边界摘要
	}

	return true;
}


//==========================================================
// 公共API - 获取/插入/删除树中的元素 (Public API - get/insert/delete an element in a tree)
//

/**
 * 查找具有指定摘要的元素，如果存在则返回加锁的引用
 *
 * @param tree 索引树指针
 * @param keyd 要查找的记录摘要
 * @param index_ref 输出参数，返回找到的记录引用
 * @return 0-找到记录（引用已返回），-1-未找到记录
 *
 * 功能说明：
 * 1. 根据摘要定位到相应的sprig
 * 2. 在sprig中搜索指定的记录
 * 3. 如果找到，返回带锁的记录引用
 * 4. 调用者负责在使用完毕后释放锁
 */
int
as_index_get_vlock(as_index_tree* tree, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	if (tree == NULL) {
		return -1;
	}

	// 初始化sprig信息，定位到对应的分片
	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	// 在sprig中查找记录
	return as_index_sprig_get_vlock(&isprig, keyd, index_ref);
}

/**
 * 查找或插入具有指定摘要的元素，返回加锁的引用
 *
 * @param tree 索引树指针
 * @param keyd 要查找或插入的记录摘要
 * @param index_ref 输出参数，返回记录引用（新建或已存在）
 * @return 1-创建并插入新记录，0-找到已存在记录，-1-错误（无法分配arena stage）
 *
 * 功能说明：
 * 1. 首先尝试查找记录，如果存在则直接返回
 * 2. 如果不存在，则创建新记录并插入到红黑树中
 * 3. 插入成功后增加树的元素计数
 * 4. 整个操作在锁保护下进行，确保原子性
 * 5. 返回的引用已加锁，调用者负责释放
 */
int
as_index_get_insert_vlock(as_index_tree* tree, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	cf_assert(tree != NULL, AS_INDEX, "inserting in null tree");

	// 初始化sprig信息，定位到对应的分片
	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	// 在sprig中查找或插入记录
	int result = as_index_sprig_get_insert_vlock(&isprig, tree->id, keyd,
			index_ref);

	// 如果是新插入的记录，增加元素计数
	if (result == 1) {
		as_incr_uint64(&tree->n_elements);
	}

	return result;
}

/**
 * 删除具有指定摘要的元素
 *
 * @param tree 索引树指针
 * @param keyd 要删除的记录摘要
 *
 * 重要提示：此函数必须在记录(sprig)锁的保护下调用！
 *
 * 功能说明：
 * 1. 根据摘要定位到相应的sprig
 * 2. 在sprig中删除指定的记录
 * 3. 删除成功后减少树的元素计数
 * 4. 调用者必须确保在调用前已获得相应的sprig锁
 */
void
as_index_delete(as_index_tree* tree, const cf_digest* keyd)
{
	if (tree == NULL) {
		return;
	}

	// 初始化sprig信息，定位到对应的分片
	as_index_sprig isprig;
	as_index_sprig_from_keyd(tree, &isprig, keyd);

	// 在sprig中删除记录，如果成功则减少元素计数
	if (as_index_sprig_delete(&isprig, keyd) == 0) {
		as_decr_uint64(&tree->n_elements);
	}
}


//==========================================================
// 本地辅助函数 - 垃圾回收，通用 (Local helpers - garbage collection, generic)
//

/**
 * 索引树垃圾回收线程的主函数
 *
 * @param unused 未使用的参数
 * @return NULL（线程函数返回值）
 *
 * 功能说明：
 * 1. 持续从垃圾回收队列中取出待销毁的索引树
 * 2. 调用as_index_tree_destroy进行实际的销毁工作
 * 3. 线程永不退出，直到程序结束
 * 4. 异步处理避免阻塞主业务线程
 */
static void*
run_index_tree_gc(void* unused)
{
	as_index_tree* tree;

	while (cf_queue_pop(&g_gc_queue, &tree, CF_QUEUE_FOREVER) == CF_QUEUE_OK) {
		as_index_tree_destroy(tree);
	}

	return NULL;
}

/**
 * 销毁索引树及其所有资源
 *
 * @param tree 待销毁的索引树
 *
 * 功能说明：
 * 1. 遍历所有sprig，清理其中的所有记录
 * 2. 回收arena内存池，释放所有分配的内存
 * 3. 销毁所有锁对（主锁和reduce锁）
 * 4. 调用完成回调函数通知上层
 * 5. 释放树结构本身的内存
 */
static void
as_index_tree_destroy(as_index_tree* tree)
{
	// 遍历所有sprig，清理记录
	for (uint32_t i = 0; i < tree->shared->n_sprigs; i++) {
		as_index_sprig isprig;
		as_index_sprig_from_i(tree, &isprig, i);

		// 深度优先遍历并清理sprig中的所有记录
		as_index_sprig_traverse_purge(&isprig, isprig.sprig->root_h);
	}

	// 回收所有内存池到arena
	cf_arenax_reclaim(tree->shared->arena, tree_puddles(tree),
			tree_puddles_count(tree->shared));

	// 销毁所有锁对
	as_lock_pair* pair = tree_locks(tree);
	as_lock_pair* pair_end = pair + NUM_LOCK_PAIRS;

	while (pair < pair_end) {
		cf_mutex_destroy(&pair->lock);
		cf_mutex_destroy(&pair->reduce_lock);
		pair++;
	}

	// 调用完成回调，通知上层树已销毁
	tree->done_cb(tree->id, tree->udata);

	// 释放树结构的内存
	cf_rc_free(tree);
}


//==========================================================
// Local helpers - reduce a sprig.
//

// Make a callback for a specified number of elements in the tree, from outside
// the tree lock.
static bool
as_index_sprig_reduce(as_index_sprig* isprig, const cf_digest* keyd,
		as_index_reduce_fn cb, void* udata)
{
	cf_mutex_lock(&isprig->pair->reduce_lock);

	// Common to encounter empty sprigs.
	if (isprig->sprig->root_h == SENTINEL_H) {
		cf_mutex_unlock(&isprig->pair->reduce_lock);
		return true;
	}

	as_index_ph stack_phs[MAX_STACK_PHS];
	as_index_ph_array ph_a = {
			.is_stack = true,
			.capacity = MAX_STACK_PHS,
			.phs = stack_phs
	};

	// Traverse just fills array, then we make callbacks outside reduce lock.
	as_index_sprig_traverse(isprig, keyd, isprig->sprig->root_h, &ph_a);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	bool do_more = true;

	for (uint32_t i = 0; i < ph_a.n_used; i++) {
		as_index_ph* ph = &ph_a.phs[i];
		as_index_ref r_ref = {
				.r = ph->r,
				.r_h = ph->r_h,
				.olock = &isprig->pair->lock
		};

		cf_mutex_lock(r_ref.olock);

		uint16_t rc = as_index_release(r_ref.r);

		// Ignore this record if it's been deleted.
		if (! as_index_is_valid_record(r_ref.r)) {
			as_namespace* ns = isprig->destructor_udata;

			if (rc == 0) {
				if (isprig->destructor != NULL) {
					isprig->destructor(r_ref.r, ns);
				}

				cf_arenax_free(isprig->arena, r_ref.r_h, NULL);
			}
			else if (r_ref.r->in_sindex == 1 && rc == 1) {
				as_sindex_gc_record(ns, &r_ref);
			}

			cf_mutex_unlock(r_ref.olock);
			continue;
		}

		if (do_more) {
			// Callback MUST call as_record_done() to unlock record.
			do_more = cb(&r_ref, udata);
		}
		else {
			cf_mutex_unlock(r_ref.olock);
		}
	}

	if (! ph_a.is_stack) {
		cf_free(ph_a.phs);
	}

	return do_more;
}

static void
as_index_sprig_traverse(as_index_sprig* isprig, const cf_digest* keyd,
		cf_arenax_handle r_h, as_index_ph_array* ph_a)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index* r = RESOLVE(r_h);
	int cmp = 0; // initialized to satisfy compiler

	if (keyd == NULL || (cmp = cf_digest_compare(&r->keyd, keyd)) < 0) {
		as_index_sprig_traverse(isprig, keyd, r->left_h, ph_a);
	}

	if (ph_a->n_used == ph_a->capacity) {
		as_index_grow_ph_array(ph_a);
	}

	// We do not collect the element with the boundary digest.

	if (keyd == NULL || cmp < 0) {
		as_index_reserve(r);

		as_index_ph* ph = &ph_a->phs[ph_a->n_used++];

		ph->r = r;
		ph->r_h = r_h;

		keyd = NULL;
	}

	as_index_sprig_traverse(isprig, keyd, r->right_h, ph_a);
}

/**
 * 扩展索引记录指针数组的容量
 *
 * @param ph_a 指向索引记录指针数组结构的指针
 *
 * 功能说明：
 * 1. 当数组容量不足时，将容量扩展为原来的2倍
 * 2. 如果当前使用栈分配，则转换为堆分配
 * 3. 如果已经是堆分配，则重新分配更大的内存
 * 4. 保持数组中已有数据的完整性
 * 5. 用于sprig遍历过程中动态扩展记录缓存
 *
 * 注意：此函数也被集合索引使用，不是本地辅助函数
 */
void
as_index_grow_ph_array(as_index_ph_array* ph_a)
{
	uint32_t new_capacity = ph_a->capacity * 2;  // 容量翻倍
	size_t new_sz = sizeof(as_index_ph) * new_capacity;

	if (ph_a->is_stack) {
		// 从栈分配转换为堆分配
		as_index_ph* phs = cf_malloc(new_sz);

		// 复制现有数据
		memcpy(phs, ph_a->phs, sizeof(as_index_ph) * ph_a->capacity);
		ph_a->phs = phs;
		ph_a->is_stack = false;
	}
	else {
		// 重新分配更大的堆内存
		ph_a->phs = cf_realloc(ph_a->phs, new_sz);
	}

	ph_a->capacity = new_capacity;
}

/**
 * 遍历并清理sprig中的所有记录（用于销毁）
 *
 * @param isprig sprig信息结构
 * @param r_h 当前遍历节点的句柄
 *
 * 功能说明：
 * 1. 递归遍历整个sprig的红黑树
 * 2. 对每个记录调用析构函数进行清理
 * 3. 释放记录在arena中的内存
 * 4. 确保在销毁时没有悬挂的引用
 * 5. 用于索引树完全销毁时的清理工作
 */
static void
as_index_sprig_traverse_purge(as_index_sprig* isprig, cf_arenax_handle r_h)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	as_index* r = RESOLVE(r_h);

	// 递归清理左右子树
	as_index_sprig_traverse_purge(isprig, r->left_h);
	as_index_sprig_traverse_purge(isprig, r->right_h);

	// 在树清理过程中不应该有引用（reduce应该已经保留了树）
	cf_assert(r->rc == 0 || (r->rc == 1 && r->in_sindex == 1), AS_INDEX,
			"purge found non-0 record rc 0x%hx", r->rc);

	// 如果有析构函数，调用它进行记录清理
	if (isprig->destructor != NULL) {
		isprig->destructor(r, isprig->destructor_udata);
	}

	// 释放记录在arena中的内存
	cf_arenax_free(isprig->arena, r_h, isprig->puddle);
}


//==========================================================
// Local helpers - get/insert/delete an element in a sprig.
//

// Used by EE index and set index functions, not a local helper.
int
as_index_sprig_get_vlock(as_index_sprig* isprig, const cf_digest* keyd,
		as_index_ref* index_ref)
{
	cf_mutex_lock(&isprig->pair->lock);

	int rv = as_index_sprig_search_lockless(isprig, keyd, &index_ref->r,
			&index_ref->r_h);

	if (rv != 0) {
		cf_mutex_unlock(&isprig->pair->lock);
		return rv;
	}

	index_ref->puddle = isprig->puddle;
	index_ref->olock = &isprig->pair->lock;

	return 0;
}

static int
as_index_sprig_get_insert_vlock(as_index_sprig* isprig, uint8_t tree_id,
		const cf_digest* keyd, as_index_ref* index_ref)
{
	int cmp = 0;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element's insertion point.
	as_index_ele eles[64]; // must be >= (24 * 2)
	as_index_ele* ele;

	while (true) {
		ele = eles;

		cf_mutex_lock(&isprig->pair->lock);

		// Search for the specified element, or a parent to insert it under.

		root_parent.left_h = isprig->sprig->root_h;
		root_parent.color = BLACK;

		ele->parent = NULL; // we'll never look this far up
		ele->me_h = 0; // root parent has no handle, never used
		ele->me = &root_parent;

		cf_arenax_handle t_h = isprig->sprig->root_h;

		while (t_h != SENTINEL_H) {
			as_index* t = RESOLVE(t_h);

			ele++;
			ele->parent = ele - 1;
			ele->me_h = t_h;
			ele->me = t;

			as_arch_prefetch_nt(t);

			if ((cmp = cf_digest_compare(keyd, &t->keyd)) == 0) {
				// The element already exists, simply return it.

				index_ref->r = t;
				index_ref->r_h = t_h;

				index_ref->puddle = isprig->puddle;
				index_ref->olock = &isprig->pair->lock;

				return 0;
			}

			t_h = cmp > 0 ? t->left_h : t->right_h;
		}

		// We didn't find the tree element, so we'll be inserting it.

		if (cf_mutex_trylock(&isprig->pair->reduce_lock)) {
			break; // no reduce in progress - go ahead and insert new element
		}

		// The tree is being reduced - could take long, unlock so reads and
		// overwrites aren't blocked.
		cf_mutex_unlock(&isprig->pair->lock);

		// Wait until the tree reduce is done...
		cf_mutex_lock(&isprig->pair->reduce_lock);
		cf_mutex_unlock(&isprig->pair->reduce_lock);

		// ... and start over - we unlocked, so the tree may have changed.
	}

	// Create a new element and insert it.

	// Save the root so we can detect whether it changes.
	cf_arenax_handle old_root = isprig->sprig->root_h;

	// Make the new element.
	cf_arenax_handle n_h = cf_arenax_alloc(isprig->arena, isprig->puddle);

	if (n_h == 0) {
		cf_ticker_warning(AS_INDEX, "arenax alloc failed");
		cf_mutex_unlock(&isprig->pair->reduce_lock);
		cf_mutex_unlock(&isprig->pair->lock);
		return -1;
	}

	as_index* n = RESOLVE(n_h);

	*n = (as_index){
		.tree_id = tree_id,
		.keyd = *keyd,
		.left_h = SENTINEL_H,
		.right_h = SENTINEL_H,
		.color = RED
	};

	// Insert the new element n under parent ele.
	if (ele->me == &root_parent || 0 < cmp) {
		ele->me->left_h = n_h;
	}
	else {
		ele->me->right_h = n_h;
	}

	ele++;
	ele->parent = ele - 1;
	ele->me_h = n_h;
	ele->me = n;

	// Rebalance the sprig as needed.
	as_index_sprig_insert_rebalance(isprig, &root_parent, ele);

	// If insertion caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		isprig->sprig->root_h = root_parent.left_h;
	}

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	index_ref->r = n;
	index_ref->r_h = n_h;

	index_ref->puddle = isprig->puddle;
	index_ref->olock = &isprig->pair->lock;

	return 1;
}

// Used by EE index function, not a local helper.
// This MUST be called under the record (sprig) lock!
int
as_index_sprig_delete(as_index_sprig* isprig, const cf_digest* keyd)
{
	as_index* r;
	cf_arenax_handle r_h;

	// Use a stack as_index object for the root's parent, for convenience.
	as_index root_parent;

	// Save parents as we search for the specified element (or its successor).
	as_index_ele eles[128]; // must be >= ((24 * 2) * 2) + 3
	as_index_ele* ele = eles;

	root_parent.left_h = isprig->sprig->root_h;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	r_h = isprig->sprig->root_h;

	while (r_h != SENTINEL_H) {
		r = RESOLVE(r_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r_h;
		ele->me = r;

		as_arch_prefetch_nt(r);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			break; // found, we'll be deleting it
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	if (r_h == SENTINEL_H) {
		return -1; // not found, nothing to delete
	}

	// We found the tree element, so we'll be deleting it.

	// If the tree is being reduced, wait until it's done...
	cf_mutex_lock(&isprig->pair->reduce_lock);

	// Delete the element.

	// Save the root so we can detect whether it changes.
	cf_arenax_handle old_root = isprig->sprig->root_h;

	// Snapshot the element to delete, r. (Already have r_h and r shortcuts.)
	as_index_ele* r_e = ele;

	if (r->left_h != SENTINEL_H && r->right_h != SENTINEL_H) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = RESOLVE(ele->me_h);

		while (ele->me->left_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = RESOLVE(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	as_index_ele* s_e = ele;
	cf_arenax_handle s_h = s_e->me_h;
	as_index* s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	ele->me_h = s->left_h == SENTINEL_H ? s->right_h : s->left_h;
	ele->me = RESOLVE(ele->me_h);

	// Cut s (remember, it could be r) out of the tree.
	ele->parent = s_e->parent;

	if (s_h == s_e->parent->me->left_h) {
		s_e->parent->me->left_h = ele->me_h;
	}
	else {
		s_e->parent->me->right_h = ele->me_h;
	}

	// Rebalance at ele if necessary. (Note - if r != s, r is in the tree, and
	// its parent may change during rebalancing.)
	if (s->color == BLACK) {
		as_index_sprig_delete_rebalance(isprig, &root_parent, ele);
	}

	if (s != r) {
		// s was a successor distinct from r, put it in r's place in the tree.
		s->left_h = r->left_h;
		s->right_h = r->right_h;
		s->color = r->color;

		if (r_h == r_e->parent->me->left_h) {
			r_e->parent->me->left_h = s_h;
		}
		else {
			r_e->parent->me->right_h = s_h;
		}
	}

	// If delete caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		isprig->sprig->root_h = root_parent.left_h;
	}

	// Flag record as deleted.
	as_index_invalidate_record(r);

	cf_mutex_unlock(&isprig->pair->reduce_lock);

	return 0;
}


//==========================================================
// Local helpers - search/rebalance a sprig.
//

static int
as_index_sprig_search_lockless(as_index_sprig* isprig, const cf_digest* keyd,
		as_index** ret, cf_arenax_handle* ret_h)
{
	cf_arenax_handle r_h = isprig->sprig->root_h;

	while (r_h != SENTINEL_H) {
		as_index* r = RESOLVE(r_h);

		as_arch_prefetch_nt(r);

		int cmp = cf_digest_compare(keyd, &r->keyd);

		if (cmp == 0) {
			if (ret_h != NULL) {
				*ret_h = r_h;
			}

			if (ret != NULL) {
				*ret = r;
			}

			return 0; // found
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	return -1; // not found
}

static void
as_index_sprig_insert_rebalance(as_index_sprig* isprig, as_index* root_parent,
		as_index_ele* ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	as_index_ele* r_e = ele;
	as_index_ele* parent_e = r_e->parent;

	while (parent_e->me->color == RED) {
		as_index_ele* grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->right_h;
			as_index* u = RESOLVE(u_h);

			if (u->color == RED) {
				u->color = BLACK;
				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->right_h) {
					// Save original r, which will become new r's parent.
					as_index_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_left(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_right(grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			cf_arenax_handle u_h = grandparent_e->me->left_h;
			as_index* u = RESOLVE(u_h);

			if (u->color == RED) {
				u->color = BLACK;
				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// Move up two layers - r becomes old r's grandparent.
				r_e = parent_e->parent;
				parent_e = r_e->parent;
			}
			else {
				if (r_e->me_h == parent_e->me->left_h) {
					// Save original r, which will become new r's parent.
					as_index_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					as_index_rotate_right(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				as_index_rotate_left(grandparent_e, parent_e);
			}
		}
	}

	RESOLVE(root_parent->left_h)->color = BLACK;
}

static void
as_index_sprig_delete_rebalance(as_index_sprig* isprig, as_index* root_parent,
		as_index_ele* ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	as_index_ele* r_e = ele;

	while (r_e->me->color == BLACK && r_e->me_h != root_parent->left_h) {
		as_index* r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			cf_arenax_handle s_h = r_parent->right_h;
			as_index* s = RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				s_h = r_parent->right_h;
				s = RESOLVE(s_h);
			}

			as_index* s_left = RESOLVE(s->left_h);
			as_index* s_right = RESOLVE(s->right_h);

			if (s_left->color == BLACK && s_right->color == BLACK) {
				s->color = RED;

				r_e = r_e->parent;
			}
			else {
				if (s_right->color == BLACK) {
					s_left->color = BLACK;
					s->color = RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					as_index_rotate_right(s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				RESOLVE(s->right_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_left(r_e->parent, ele);

				RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
		else {
			cf_arenax_handle s_h = r_parent->left_h;
			as_index* s = RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				s_h = r_parent->left_h;
				s = RESOLVE(s_h);
			}

			as_index* s_left = RESOLVE(s->left_h);
			as_index* s_right = RESOLVE(s->right_h);

			if (s_left->color == BLACK && s_right->color == BLACK) {
				s->color = RED;

				r_e = r_e->parent;
			}
			else {
				if (s_left->color == BLACK) {
					s_right->color = BLACK;
					s->color = RED;

					ele++;
					ele->parent = r_e->parent;
					ele->me_h = s_h;
					ele->me = s;

					as_index_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					as_index_rotate_left(s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE_H(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				RESOLVE(s->left_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				as_index_rotate_right(r_e->parent, ele);

				RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
	}

	r_e->me->color = BLACK;
}

static void
as_index_rotate_left(as_index_ele* a, as_index_ele* b)
{
	// Element b is element a's right child - a will become b's left child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *     [x]  b           a  [y]
	 *         / \         / \
	 *        c  [y]     [x]  c
	 */

	// Set a's right child to c, b's former left child.
	a->me->right_h = b->me->left_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's left child to a, and a's parent to b.
	b->me->left_h = a->me_h;
	a->parent = b;
}

static void
as_index_rotate_right(as_index_ele* a, as_index_ele* b)
{
	// Element b is element a's left child - a will become b's right child.

	/*        p      -->      p
	 *        |               |
	 *        a               b
	 *       / \             / \
	 *      b  [x]         [y]  a
	 *     / \                 / \
	 *   [y]  c               c  [x]
	 */

	// Set a's left child to c, b's former right child.
	a->me->left_h = b->me->right_h;

	// Set p's left or right child (whichever a was) to b.
	if (a->me_h == a->parent->me->left_h) {
		a->parent->me->left_h = b->me_h;
	}
	else {
		a->parent->me->right_h = b->me_h;
	}

	// Set b's parent to p, a's old parent.
	b->parent = a->parent;

	// Set b's right child to a, and a's parent to b.
	b->me->right_h = a->me_h;
	a->parent = b;
}
