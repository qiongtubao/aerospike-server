/*
 * set_index.c
 *
 * Copyright (C) 2021 Aerospike, Inc.
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
 * 集合索引模块 (Set Index Module)
 * ======================================================
 *
 * 本模块实现了Aerospike数据库的集合索引系统，为每个集合（Set）提供
 * 独立的索引结构，支持按集合进行高效的记录查询和遍历操作。
 *
 * 核心设计特点：
 * - 分层索引架构：在主索引之上构建集合索引，实现快速的集合过滤
 * - 独立红黑树：每个集合维护独立的红黑树，减少跨集合操作的开销
 * - 微型arena管理：使用轻量级的uarena进行内存管理，优化小对象分配
 * - 异步填充：支持在线启用集合索引，后台异步填充现有记录
 *
 * 索引结构层次：
 * 1. as_set_index_tree: 集合索引树的顶层结构
 * 2. ssprig: 集合索引分片，将记录按摘要分布到不同的红黑树
 * 3. index_ele: 集合索引元素，存储记录句柄和摘要片段
 *
 * 内存管理策略：
 * - uarena: 微型arena，专为小对象（index_ele）设计的内存管理器
 * - stage机制: 按固定大小的stage分配内存，支持快速分配和释放
 * - 自由列表: 维护已释放对象的链表，支持内存重用
 *
 * 并发控制：
 * - 复用主索引的锁机制，每个ssprig与对应的主索引sprig共享锁
 * - 平衡锁：全局平衡锁保护集合索引的创建和销毁操作
 * - 引用计数：集合索引树使用引用计数管理生命周期
 */

//==========================================================
// Includes.
//

#include "base/set_index.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "arenax.h"
#include "cf_mutex.h"
#include "cf_thread.h"
#include "log.h"
#include "vmapx.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "sindex/gc.h"

//#include "warnings.h"


//==========================================================
// 类型定义和常量 (Typedefs & constants)
//

/**
 * 栈元素结构 - 用于红黑树操作时保存父子关系
 * 在集合索引树的插入、删除和重平衡操作中，需要追踪从根到目标节点的路径
 */
typedef struct stack_ele_s {
	struct stack_ele_s* parent;  // 指向父节点的栈元素
	uarena_handle me_h;          // 当前节点的微型arena句柄
	index_ele* me;               // 指向当前索引元素的指针
} stack_ele;

/**
 * 填充信息结构 - 用于异步填充集合索引
 * 当启用集合索引时，需要遍历现有记录并将其添加到集合索引中
 */
typedef struct populate_info_s {
	as_namespace* ns;    // 命名空间指针
	as_set* p_set;       // 集合指针
	uint16_t set_id;     // 集合ID
	uint32_t pid;        // 分区ID（原子递增，用于工作分配）
} populate_info;

/**
 * 填充回调信息结构 - 传递给填充回调函数的上下文
 * 包含填充过程中需要的所有上下文信息
 */
typedef struct populate_cb_info_s {
	as_namespace* ns;           // 命名空间指针
	as_set* p_set;              // 集合指针
	uint16_t set_id;            // 集合ID
	as_index_tree* tree;        // 主索引树
	as_set_index_tree* stree;   // 集合索引树
} populate_cb_info;

#define N_POPULATE_THREADS 4  // 并行填充的线程数量

//--------------------------------------
// uarena常量 (uarena constants)
//

#define STAGES_STEP 8                           // stage扩展步长
#define STAGE_CAPACITY (1 << ELE_ID_N_BITS)     // 每个stage的容量 (256)
#define STAGE_SIZE (STAGE_CAPACITY * ELE_SIZE)  // 每个stage的大小 (4K)


//==========================================================
// 全局变量 (Globals)
//

/**
 * 全局平衡锁
 * 保护集合索引的创建和销毁操作，确保与rebalance操作不冲突
 * 在启用/禁用集合索引时需要获取此锁，避免竞态条件
 */
cf_mutex g_balance_lock = CF_MUTEX_INIT;

/**
 * 全局填充队列
 * 存储待填充的集合索引信息，由专门的填充线程处理
 * 当启用集合索引时，将填充任务加入此队列异步处理
 */
static cf_queue g_populate_q;


//==========================================================
// Forward declarations.
//

static inline bool is_set_indexed(const as_namespace* ns, uint16_t set_id);
static inline bool is_set_populated(const as_namespace* ns, uint16_t set_id);

static int populate_q_reduce_cb(void* buf, void* udata);
static void* run_populate_q(void* udata);
static void* run_populate(void* udata);
static bool populate_reduce_cb(as_index_ref* r_ref, void* udata);

static inline as_set_index_tree* stree_create(void);
static inline void stree_destroy(as_set_index_tree* stree);
static inline as_set_index_tree* stree_reserve(as_index_tree* tree, uint16_t set_id);
static inline void stree_release(as_set_index_tree* stree);

static inline void ssi_from_keyd(as_index_tree* tree, as_set_index_tree* stree, const cf_digest* keyd, ssprig_info* ssi);
static inline uint32_t ssprig_i_from_keyd(const cf_digest* keyd);
static inline uint32_t stub_from_keyd(const cf_digest* keyd);
static inline void ssri_from_ssprig_i(as_index_tree* tree, as_set_index_tree* stree, const cf_digest* keyd, uint32_t keyd_stub, uint32_t ssprig_i, ssprig_reduce_info* ssri);

static void ssprig_delete(ssprig_info* ssi);
static bool ssprig_reduce(ssprig_reduce_info* ssri, as_index_reduce_fn cb, void* udata);
static void ssprig_traverse(ssprig_reduce_info* ssri, uarena_handle r_h, as_index_ph_array* ph_a);

static void insert_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele);
static void delete_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele);
static void rotate_left(stack_ele* a, stack_ele* b);
static void rotate_right(stack_ele* a, stack_ele* b);

//--------------------------------------
// uarena API.
//

static void uarena_init(uarena* ua);
static void uarena_destroy(uarena* ua);
static void uarena_add_stage(uarena* ua);
static uarena_handle uarena_alloc(uarena* ua);
static void uarena_free(uarena* ua, uarena_handle h);


//==========================================================
// Inlines & macros.
//

//--------------------------------------
// uarena API.
//

#define NEXT_FREE_H(_ua, _h) (*(uarena_handle*)uarena_resolve(_ua, _h))

static inline void
uarena_set_handle(uarena_handle* h, uint32_t stage_id, uint32_t ele_id)
{
	*h = (stage_id << ELE_ID_N_BITS) | ele_id;
}


//==========================================================
// 公共API - 启动 (Public API - startup)
//

/**
 * 初始化集合索引系统
 *
 * 功能说明：
 * 1. 初始化全局填充队列，容量为4个填充任务
 * 2. 创建填充队列处理线程，负责异步处理集合索引的填充任务
 * 3. 系统启动时调用一次，准备集合索引的基础设施
 */
void
as_set_index_init(void)
{
	cf_queue_init(&g_populate_q, sizeof(populate_info), 4, true);

	cf_thread_create_detached(run_populate_q, NULL);
}


//==========================================================
// 公共API - 集合索引树生命周期 (Public API - set-index tree lifecycle)
//

/**
 * 为所有启用索引的集合创建集合索引树
 * 在分区锁保护下可能被调用
 *
 * @param ns 命名空间指针
 * @param tree 主索引树指针
 *
 * 功能说明：
 * 1. 遍历命名空间中的所有集合
 * 2. 为每个启用索引的集合创建对应的集合索引树
 * 3. 通常在分区初始化或rebalance时调用
 */
void
as_set_index_create_all(as_namespace* ns, as_index_tree* tree)
{
	uint32_t n_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint16_t set_id = 1; set_id <= n_sets; set_id++) {
		if (is_set_indexed(ns, set_id)) {
			tree->set_trees[set_id] = stree_create();
		}
	}
}

/**
 * 销毁索引树中的所有集合索引树
 *
 * @param tree 主索引树指针
 *
 * 功能说明：
 * 1. 遍历所有可能的集合索引树
 * 2. 验证引用计数并直接销毁每个集合索引树
 * 3. 销毁集合索引树锁
 * 4. 通常在主索引树销毁时调用
 */
void
as_set_index_destroy_all(as_index_tree* tree)
{
	for (uint32_t set_id = 1; set_id <= AS_SET_MAX_COUNT; set_id++) {
		as_set_index_tree* stree = tree->set_trees[set_id];

		if (stree != NULL) {
			// TODO - 偏执检查 - 最终可能会移除或简化
			uint32_t rc = cf_rc_count(stree);
			cf_assert(rc == 1, AS_INDEX, "bad stree rc %u id %u", rc, set_id);

			stree_destroy(stree); // 可以直接销毁stree
		}
	}

	cf_mutex_destroy(&tree->set_trees_lock);
}

/**
 * 为指定集合创建集合索引树
 *
 * @param tree 主索引树指针
 * @param set_id 集合ID
 *
 * 功能说明：
 * - 创建新的集合索引树并关联到主索引树
 * - 通常在动态启用集合索引时调用
 */
void
as_set_index_tree_create(as_index_tree* tree, uint16_t set_id)
{
	tree->set_trees[set_id] = stree_create();
}

/**
 * 销毁指定集合的集合索引树
 *
 * @param tree 主索引树指针
 * @param set_id 集合ID
 *
 * 功能说明：
 * 1. 获取集合索引树锁，确保线程安全
 * 2. 将集合索引树从主索引树中移除
 * 3. 释放集合索引树的引用计数
 * 4. 如果引用计数降到0，集合索引树将被自动销毁
 */
void
as_set_index_tree_destroy(as_index_tree* tree, uint16_t set_id)
{
	cf_mutex_lock(&tree->set_trees_lock);

	as_set_index_tree* stree = tree->set_trees[set_id];

	tree->set_trees[set_id] = NULL;
	stree_release(stree);

	cf_mutex_unlock(&tree->set_trees_lock);
}

/**
 * 获取全局平衡锁
 *
 * 功能说明：
 * - 保护集合索引的创建和销毁操作
 * - 确保与rebalance操作不冲突
 * - 必须与as_set_index_balance_unlock配对使用
 */
void
as_set_index_balance_lock(void)
{
	cf_mutex_lock(&g_balance_lock);
}

/**
 * 释放全局平衡锁
 *
 * 功能说明：
 * - 释放由as_set_index_balance_lock获取的锁
 * - 允许其他线程进行集合索引操作
 */
void
as_set_index_balance_unlock(void)
{
	cf_mutex_unlock(&g_balance_lock);
}


//==========================================================
// 公共API - 事务处理 (Public API - transactions)
//

/**
 * 将记录插入到集合索引中
 *
 * @param ns 命名空间指针
 * @param tree 主索引树指针
 * @param set_id 集合ID
 * @param r_h 记录在arena中的句柄
 *
 * 功能说明：
 * 1. 检查集合是否启用了索引，未启用则直接返回
 * 2. 获取集合索引树的引用，确保在操作期间不被销毁
 * 3. 根据记录摘要确定应插入的ssprig位置
 * 4. 执行实际的插入操作
 * 5. 释放集合索引树引用
 */
void
as_set_index_insert(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		uint64_t r_h)
{
	if (! is_set_indexed(ns, set_id)) {
		return;
	}

	// 获取集合索引树引用，确保操作期间的稳定性
	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return;
	}

	// 解析记录指针，获取记录摘要
	as_index* r = cf_arenax_resolve(tree->shared->arena, r_h);
	ssprig_info ssi;

	// 根据记录摘要初始化ssprig信息
	ssi_from_keyd(tree, stree, &r->keyd, &ssi);

	// 插入记录到对应的ssprig
	if (! ssprig_insert(&ssi, r_h)) {
		cf_warning(AS_INDEX, "insert found existing element - unexpected");
	}

	stree_release(stree);
}

/**
 * 从集合索引中删除记录
 *
 * @param ns 命名空间指针
 * @param tree 主索引树指针
 * @param set_id 集合ID
 * @param r_h 记录在arena中的句柄
 *
 * 功能说明：
 * 1. 检查集合是否启用了索引，未启用则直接返回
 * 2. 获取集合索引树的引用
 * 3. 根据记录摘要定位到对应的ssprig
 * 4. 从ssprig中删除记录
 * 5. 释放集合索引树引用
 */
void
as_set_index_delete(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		uint64_t r_h)
{
	if (! is_set_indexed(ns, set_id)) {
		return;
	}

	// 获取集合索引树引用
	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return;
	}

	// 解析记录指针，获取记录摘要
	as_index* r = cf_arenax_resolve(tree->shared->arena, r_h);
	ssprig_info ssi;

	// 根据记录摘要初始化ssprig信息并执行删除
	ssi_from_keyd(tree, stree, &r->keyd, &ssi);
	ssprig_delete(&ssi);

	stree_release(stree);
}

/**
 * 删除活跃记录的集合索引
 *
 * @param ns 命名空间指针
 * @param tree 主索引树指针
 * @param r 记录指针
 * @param r_h 记录在arena中的句柄
 *
 * 功能说明：
 * - 检查记录是否为活跃状态，如果是则从集合索引中删除
 * - 用于记录过期或删除时的清理工作
 */
void
as_set_index_delete_live(as_namespace* ns, as_index_tree* tree, as_record* r,
		uint64_t r_h)
{
	if (as_record_is_live(r)) {
		as_set_index_delete(ns, tree, as_index_get_set_id(r), r_h);
	}
}

/**
 * 遍历指定集合的所有记录
 *
 * @param ns 命名空间指针
 * @param tree 主索引树指针
 * @param set_id 集合ID
 * @param keyd 起始边界摘要，NULL表示从头开始
 * @param cb 回调函数指针
 * @param udata 传递给回调函数的用户数据
 * @return true表示成功启动遍历，false表示集合未准备好或操作失败
 *
 * 功能说明：
 * 1. 检查集合索引是否已填充完成
 * 2. 获取集合索引树引用
 * 3. 确定遍历起始点（ssprig和摘要片段）
 * 4. 从起始ssprig开始倒序遍历所有ssprig
 * 5. 根据puddle配置选择适当的遍历策略
 * 6. 释放集合索引树引用
 */
bool
as_set_index_reduce(as_namespace* ns, as_index_tree* tree, uint16_t set_id,
		cf_digest* keyd, as_index_reduce_fn cb, void* udata)
{
	if (! is_set_populated(ns, set_id)) {
		return false;
	}

	// 获取集合索引树引用
	as_set_index_tree* stree = stree_reserve(tree, set_id);

	if (stree == NULL) {
		return false;
	}

	uint32_t start_sprig_i;
	uint32_t keyd_stub;

	// 确定遍历起始点
	if (keyd == NULL) {
		start_sprig_i = N_SET_SPRIGS - 1;  // 从最后一个ssprig开始
		keyd_stub = 0;
	}
	else {
		start_sprig_i = ssprig_i_from_keyd(keyd);  // 根据摘要确定起始ssprig
		keyd_stub = stub_from_keyd(keyd);          // 提取摘要片段
	}

	// 倒序遍历所有ssprig
	for (int i = (int)start_sprig_i; i >= 0; i--, keyd = NULL) {
		// 优化：跳过空的ssprig（非常常见）
		if (stree->roots[i] == SENTINEL_H) {
			continue;
		}

		ssprig_reduce_info ssri;
		ssri_from_ssprig_i(tree, stree, keyd, keyd_stub, (uint32_t)i, &ssri);

		// 根据puddle配置选择遍历策略
		if (tree->shared->puddles_offset == 0) {
			// 无puddle：使用标准引用计数遍历
			if (! ssprig_reduce(&ssri, cb, udata)) {
				break; // 不关心为什么结束遍历
			}
		}
		else {
			// 有puddle：使用无引用计数的优化遍历
			if (! ssprig_reduce_no_rc(tree, &ssri, cb, udata)) {
				break; // 不关心为什么结束遍历
			}
		}
	}

	stree_release(stree);

	return true;
}


//==========================================================
// 公共API - 信息和统计 (Public API - info & stats)
//

/**
 * 启用指定集合的索引功能
 *
 * @param ns 命名空间指针
 * @param p_set 集合指针
 * @param set_id 集合ID
 *
 * 功能说明：
 * 1. 检查集合索引是否已启用，避免重复启用
 * 2. 获取全局平衡锁，确保与rebalance操作同步
 * 3. 为所有分区创建集合索引结构
 * 4. 标记集合索引为填充状态并启用
 * 5. 如果集合为空，直接完成；否则启动异步填充
 */
void
as_set_index_enable(as_namespace* ns, as_set* p_set, uint16_t set_id)
{
	if (p_set->index_enabled) {
		return;
	}

	// 确保rebalance或此调用创建stree
	cf_mutex_lock(&g_balance_lock);

	// 为所有分区创建集合索引
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_create_set_index(ns, pid, set_id);
	}

	// 标记为正在填充并启用索引（使用release语义确保可见性）
	p_set->index_populating = true;
	as_store_bool_rls(&p_set->index_enabled, true);

	cf_mutex_unlock(&g_balance_lock);

	// 内存屏障确保上述操作的可见性
	as_fence_seq();

	// 如果集合为空，直接完成
	if (p_set->n_objects == 0) {
		p_set->index_populating = false;

		cf_info(AS_INDEX, "{%s|%s} done populating set-index (0 ms)", ns->name,
				p_set->name);

		return;
	}

	// 准备填充信息并加入队列
	populate_info popi = {
			.ns = ns,
			.p_set = p_set,
			.set_id = set_id
	};

	cf_queue_push(&g_populate_q, &popi);
}

/**
 * 禁用指定集合的索引功能
 *
 * @param ns 命名空间指针
 * @param p_set 集合指针
 * @param set_id 集合ID
 *
 * 功能说明：
 * 1. 检查集合索引是否已启用，未启用则直接返回
 * 2. 获取全局平衡锁，确保与rebalance操作同步
 * 3. 禁用集合索引并销毁所有分区的集合索引结构
 * 4. 从填充队列中移除可能存在的填充任务
 * 5. 等待正在进行的填充操作完成
 */
void
as_set_index_disable(as_namespace* ns, as_set* p_set, uint16_t set_id)
{
	if (! p_set->index_enabled) {
		return;
	}

	// 确保rebalance不会设置NULL stree - destroy假设它们不是NULL
	cf_mutex_lock(&g_balance_lock);

	// 禁用索引标志
	p_set->index_enabled = false;

	// 销毁所有分区的集合索引
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_destroy_set_index(ns, pid, set_id);
	}

	cf_mutex_unlock(&g_balance_lock);

	// 如果还在队列中等待启用，从队列中移除
	cf_queue_reduce(&g_populate_q, populate_q_reduce_cb, p_set);

	// 如果取消了填充，确保取消完成（以便我们可以重新填充）
	while (p_set->index_populating) {
		usleep(100);
	}
}

/**
 * 计算集合索引使用的字节数
 *
 * @param ns 命名空间指针
 * @return 集合索引总共使用的字节数
 *
 * 功能说明：
 * 1. 遍历命名空间中的所有集合
 * 2. 统计启用了索引的集合中的对象总数
 * 3. 根据index_ele的大小计算总内存使用量
 * 4. 用于监控和容量规划
 */
uint64_t
as_set_index_used_bytes(const as_namespace* ns)
{
	uint64_t n_objects = 0;
	uint32_t n_sets = cf_vmapx_count(ns->p_sets_vmap);

	for (uint32_t set_ix = 0; set_ix < n_sets; set_ix++) {
		as_set* p_set;

		// 从vmap中获取集合指针
		if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
				CF_VMAPX_OK) {
			cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
		}

		// 只统计启用了索引的集合
		if (p_set->index_enabled) {
			n_objects += p_set->n_objects;
		}
	}

	// 返回总内存使用量（对象数 × 索引元素大小）
	return n_objects * sizeof(index_ele);
}


//==========================================================
// 本地辅助函数 - 配置 (Local helpers - configuration)
//

/**
 * 检查指定集合是否启用了索引
 *
 * @param ns 命名空间指针
 * @param set_id 集合ID
 * @return true表示集合启用了索引，false表示未启用
 *
 * 功能说明：
 * 1. 检查集合ID的有效性
 * 2. 从命名空间的集合映射中获取集合指针
 * 3. 使用内存屏障确保读取操作的可见性
 * 4. 返回集合的索引启用状态
 */
static inline bool
is_set_indexed(const as_namespace* ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return false;
	}

	uint32_t set_ix = (uint32_t)(set_id - 1);  // 集合索引从0开始
	as_set* p_set;

	// 从vmap中获取集合指针
	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
			CF_VMAPX_OK) {
		cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
	}

	// 内存屏障确保读取的一致性
	as_fence_seq();

	return p_set->index_enabled;
}

/**
 * 检查指定集合的索引是否已填充完成
 *
 * @param ns 命名空间指针
 * @param set_id 集合ID
 * @return true表示集合索引已填充完成，false表示未完成或未启用
 *
 * 功能说明：
 * 1. 检查集合ID的有效性
 * 2. 从命名空间的集合映射中获取集合指针
 * 3. 检查集合索引是否启用且填充完成
 * 4. 使用acquire语义确保读取操作的正确性
 */
static inline bool
is_set_populated(const as_namespace* ns, uint16_t set_id)
{
	if (set_id == INVALID_SET_ID) {
		return false;
	}

	uint32_t set_ix = (uint32_t)(set_id - 1);  // 集合索引从0开始
	as_set* p_set;

	// 从vmap中获取集合指针
	if (cf_vmapx_get_by_index(ns->p_sets_vmap, set_ix, (void**)&p_set) !=
			CF_VMAPX_OK) {
		cf_crash(AS_INDEX, "failed to get set index %u from vmap", set_ix);
	}

	// 使用acquire语义读取索引启用状态，并检查是否填充完成
	return as_load_bool_acq(&p_set->index_enabled) && ! p_set->index_populating;
}


//==========================================================
// Local helpers - population.
//

static int
populate_q_reduce_cb(void* buf, void* udata)
{
	as_set* p_set = (as_set*)udata;

	if (((populate_info*)buf)->p_set == p_set) {
		p_set->index_populating = false;
		return -2;
	}

	return 0;
}

static void*
run_populate_q(void* udata)
{
	while (true) {
		populate_info popi;

		cf_queue_pop(&g_populate_q, &popi, CF_QUEUE_FOREVER);

		cf_info(AS_INDEX, "{%s|%s} start populating set-index ...",
				popi.ns->name, popi.p_set->name);

		uint64_t start_ms = cf_getms();

		cf_tid tids[N_POPULATE_THREADS];

		for (uint32_t n = 0; n < N_POPULATE_THREADS; n++) {
			tids[n] = cf_thread_create_joinable(run_populate, &popi);
		}

		for (uint32_t n = 0; n < N_POPULATE_THREADS; n++) {
			cf_thread_join(tids[n]);
		}

		popi.p_set->index_populating = false;

		cf_info(AS_INDEX, "{%s|%s} done populating set-index (%lu ms)",
				popi.ns->name, popi.p_set->name, cf_getms() - start_ms);
	}

	return NULL;
}

static void*
run_populate(void* udata)
{
	populate_info* popi = (populate_info*)udata;
	uint32_t pid;

	while ((pid = as_faa_uint32(&popi->pid, 1)) < AS_PARTITIONS) {
		as_partition_reservation rsv;
		as_partition_reserve(popi->ns, pid, &rsv);

		if (rsv.tree == NULL) {
			as_partition_release(&rsv);
			continue;
		}

		// Populate can be cancelled abruptly - check each step for that.

		as_set_index_tree* stree = stree_reserve(rsv.tree, popi->set_id);

		if (stree == NULL) {
			as_partition_release(&rsv);
			break;
		}

		populate_cb_info cbi = {
				.ns = popi->ns,
				.p_set = popi->p_set,
				.set_id = popi->set_id,
				.tree = rsv.tree,
				.stree = stree
		};

		if (! as_index_reduce_live(rsv.tree, populate_reduce_cb, &cbi)) {
			stree_release(stree);
			as_partition_release(&rsv);
			break;
		}

		stree_release(stree);
		as_partition_release(&rsv);
	}

	return NULL;
}

static bool
populate_reduce_cb(as_index_ref* r_ref, void* udata)
{
	populate_cb_info* cbi = (populate_cb_info*)udata;

	if (! cbi->p_set->index_enabled) {
		as_record_done(r_ref, cbi->ns);
		return false;
	}

	if (as_index_get_set_id(r_ref->r) != cbi->set_id) {
		as_record_done(r_ref, cbi->ns);
		return true;
	}

	ssprig_info ssi;

	ssi_from_keyd(cbi->tree, cbi->stree, &r_ref->r->keyd, &ssi);
	ssprig_insert(&ssi, r_ref->r_h);

	as_record_done(r_ref, cbi->ns);

	return true;
}


//==========================================================
// Local helpers - as_set_index_tree lifecycle.
//

static inline as_set_index_tree*
stree_create(void)
{
	as_set_index_tree* stree = cf_rc_alloc(sizeof(as_set_index_tree));

	memset(stree, 0, sizeof(as_set_index_tree));
	uarena_init(&stree->ua);

	return stree;
}

static inline void
stree_destroy(as_set_index_tree* stree)
{
	uarena_destroy(&stree->ua);
	cf_rc_free(stree);
}

static inline as_set_index_tree*
stree_reserve(as_index_tree* tree, uint16_t set_id)
{
	cf_mutex_lock(&tree->set_trees_lock);

	as_set_index_tree* stree = tree->set_trees[set_id];

	if (stree != NULL) {
		cf_rc_reserve(stree);
	}

	cf_mutex_unlock(&tree->set_trees_lock);

	return stree;
}

static inline void
stree_release(as_set_index_tree* stree)
{
	if (cf_rc_release(stree) == 0) {
		stree_destroy(stree);
	}
}


//==========================================================
// Local helpers - sprig parameter utilities.
//

static inline void
ssi_from_keyd(as_index_tree* tree, as_set_index_tree* stree,
		const cf_digest* keyd, ssprig_info* ssi)
{
	// Get the 8 + 23 most significant non-pid bits in the digest. Note - this
	// is hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	uint32_t bits = (((uint32_t)keyd->digest[1] & 0xF0) << 23) |
			((uint32_t)keyd->digest[2] << 19) |
			((uint32_t)keyd->digest[3] << 11) |
			((uint32_t)keyd->digest[4] << 3) |
			((uint32_t)keyd->digest[5] >> 5);

	uint32_t ssprig_i = bits >> 23;

	ssi->arena = tree->shared->arena;
	ssi->ua = &stree->ua;
	ssi->root = stree->roots + ssprig_i;
	ssi->keyd_stub = bits & ((1 << 23) - 1);
	ssi->keyd = keyd;
}

static inline uint32_t
ssprig_i_from_keyd(const cf_digest* keyd)
{
	// Get the 8 most significant non-pid bits in the digest. Note - this is
	// hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	return ((uint32_t)keyd->digest[1] & 0xF0) |
			((uint32_t)keyd->digest[2] >> 4);
}

static inline uint32_t
stub_from_keyd(const cf_digest* keyd)
{
	// Get the 23 most significant non-sprig bits in the digest. Note - this
	// is hardwired around the way we currently extract the 12-bit partition-ID
	// from the digest.
	return (((uint32_t)keyd->digest[2] & 0x0F) << 19) |
			((uint32_t)keyd->digest[3] << 11) |
			((uint32_t)keyd->digest[4] << 3) |
			((uint32_t)keyd->digest[5] >> 5);
}

static inline void
ssri_from_ssprig_i(as_index_tree* tree, as_set_index_tree* stree,
		const cf_digest* keyd, uint32_t keyd_stub, uint32_t ssprig_i,
		ssprig_reduce_info* ssri)
{
	ssprig_info* ssi = (ssprig_info*)ssri;

	ssi->arena = tree->shared->arena;
	ssi->ua = &stree->ua;
	ssi->root = stree->roots + ssprig_i;
	ssi->keyd_stub = keyd_stub;
	ssi->keyd = keyd;

	ssri->destructor = tree->shared->destructor;
	ssri->destructor_udata = tree->shared->destructor_udata;
	ssri->olock = &(tree_locks(tree) + ssprig_i)->lock;
}


//==========================================================
// Local helpers - red-black sprigs.
//

// Accessed by enterprise split.
bool
ssprig_insert(ssprig_info* ssi, uint64_t key_r_h)
{
	// Shortcut inserting into empty sprig. May be common for set-indexes.
	if (*ssi->root == SENTINEL_H) {
		uarena_handle n_h = uarena_alloc(ssi->ua);
		index_ele* n = UA_RESOLVE(n_h);

		*n = (index_ele){
				.key_r_h = key_r_h,
				.keyd_stub = ssi->keyd_stub,
				.color = BLACK,
				.left_h = SENTINEL_H,
				.right_h = SENTINEL_H
		};

		*ssi->root = n_h;

		return true;
	}

	int cmp = 0;

	// Use a stack index_ele object for the root's parent, for convenience.
	index_ele root_parent;

	// Save parents as we search for the specified element's insertion point.
	stack_ele eles[64]; // enough for 16M elements per sprig
	stack_ele* ele = eles;

	// Search for the specified element, or a parent to insert it under.

	root_parent.left_h = *ssi->root;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	uarena_handle t_h = *ssi->root;

	while (t_h != SENTINEL_H) {
		index_ele* t = UA_RESOLVE(t_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = t_h;
		ele->me = t;

		// FIXME - is this a bad idea given our index-ele size & arenas?
//		_mm_prefetch(t, _MM_HINT_NTA);

		if ((cmp = ssprig_ele_cmp(ssi, t)) == 0) {
			return false; // element already exists
		}

		t_h = cmp > 0 ? t->left_h : t->right_h;
	}

	// We didn't find the tree element - create a new element and insert it.

	// Save the root so we can detect whether it changes.
	uarena_handle old_root = *ssi->root;

	// Make the new element.
	uarena_handle n_h = uarena_alloc(ssi->ua);
	index_ele* n = UA_RESOLVE(n_h);

	*n = (index_ele){
			.key_r_h = key_r_h,
			.keyd_stub = ssi->keyd_stub,
			.color = RED,
			.left_h = SENTINEL_H,
			.right_h = SENTINEL_H
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
	insert_rebalance(ssi, &root_parent, ele);

	// If insertion caused the root to change, save the new root.
	if (root_parent.left_h != old_root) {
		*ssi->root = root_parent.left_h;
	}

	return true;
}

static void
ssprig_delete(ssprig_info* ssi)
{
	index_ele* r;
	uarena_handle r_h;

	// Use a stack index_ele object for the root's parent, for convenience.
	index_ele root_parent;

	// Save parents as we search for the specified element (or its successor).
	stack_ele eles[128]; // enough for 16M elements per sprig
	stack_ele* ele = eles;

	root_parent.left_h = *ssi->root;
	root_parent.color = BLACK;

	ele->parent = NULL; // we'll never look this far up
	ele->me_h = 0; // root parent has no handle, never used
	ele->me = &root_parent;

	r_h = *ssi->root;

	while (r_h != SENTINEL_H) {
		r = UA_RESOLVE(r_h);

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r_h;
		ele->me = r;

//		_mm_prefetch(r, _MM_HINT_NTA);

		int cmp = ssprig_ele_cmp(ssi, r);

		if (cmp == 0) {
			break; // found, we'll be deleting it
		}

		r_h = cmp > 0 ? r->left_h : r->right_h;
	}

	if (r_h == SENTINEL_H) {
		return; // element not found - can happen while populating
	}

	// We found the tree element - delete the element.

	// Save the root so we can detect whether it changes.
	uarena_handle old_root = *ssi->root;

	// Snapshot the element to delete, r. (Already have r_h and r shortcuts.)
	stack_ele* r_e = ele;

	if (r->left_h != SENTINEL_H && r->right_h != SENTINEL_H) {
		// Search down for a "successor"...

		ele++;
		ele->parent = ele - 1;
		ele->me_h = r->right_h;
		ele->me = UA_RESOLVE(ele->me_h);

		while (ele->me->left_h != SENTINEL_H) {
			ele++;
			ele->parent = ele - 1;
			ele->me_h = ele->parent->me->left_h;
			ele->me = UA_RESOLVE(ele->me_h);
		}
	}
	// else ele is left at r, i.e. s == r

	// Snapshot the successor, s. (Note - s could be r.)
	stack_ele* s_e = ele;
	uarena_handle s_h = s_e->me_h;
	index_ele* s = s_e->me;

	// Get the appropriate child of s. (Note - child could be sentinel.)
	ele++;

	ele->me_h = s->left_h == SENTINEL_H ? s->right_h : s->left_h;
	ele->me = UA_RESOLVE(ele->me_h);

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
		delete_rebalance(ssi, &root_parent, ele);
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
		*ssi->root = root_parent.left_h;
	}

	uarena_free(ssi->ua, r_h);
}

static bool
ssprig_reduce(ssprig_reduce_info* ssri, as_index_reduce_fn cb, void* udata)
{
	ssprig_info* ssi = (ssprig_info*)ssri;

	cf_mutex_lock(ssri->olock);

	// Very common to encounter empty sprigs - check again under lock.
	if (*ssi->root == SENTINEL_H) {
		cf_mutex_unlock(ssri->olock);
		return true;
	}

	as_index_ph stack_phs[MAX_STACK_PHS];
	as_index_ph_array ph_a = {
			.is_stack = true,
			.capacity = MAX_STACK_PHS,
			.phs = stack_phs
	};

	// Traverse just fills array, then we make callbacks afterwards.
	ssprig_traverse(ssri, *ssi->root, &ph_a);

	cf_mutex_unlock(ssri->olock);

	bool do_more = true;

	for (uint32_t i = 0; i < ph_a.n_used; i++) {
		as_index_ph* ph = &ph_a.phs[i];
		as_index_ref r_ref = {
				.r = ph->r,
				.r_h = ph->r_h,
				.olock = ssri->olock
		};

		cf_mutex_lock(r_ref.olock);

		uint16_t rc = as_index_release(r_ref.r);

		// Ignore this record if it's been deleted.
		if (! as_index_is_valid_record(r_ref.r)) {
			as_namespace* ns = ssri->destructor_udata;

			if (rc == 0) {
				if (ssri->destructor != NULL) {
					ssri->destructor(r_ref.r, ns);
				}

				cf_arenax_free(ssi->arena, r_ref.r_h, NULL);
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
ssprig_traverse(ssprig_reduce_info* ssri, uarena_handle r_h,
		as_index_ph_array* ph_a)
{
	if (r_h == SENTINEL_H) {
		return;
	}

	ssprig_info* ssi = (ssprig_info*)ssri;

	index_ele* r = uarena_resolve(ssi->ua, r_h);
	int cmp = 0; // initialized to satisfy compiler

	if (ssi->keyd == NULL || (cmp = ssprig_ele_cmp(ssi, r)) > 0) {
		ssprig_traverse(ssri, r->left_h, ph_a);
	}

	if (ph_a->n_used == ph_a->capacity) {
		as_index_grow_ph_array(ph_a);
	}

	// We do not collect the element with the boundary digest.

	if (ssi->keyd == NULL || cmp > 0) {
		as_index* key_r = cf_arenax_resolve(ssi->arena, r->key_r_h);

		as_index_reserve(key_r);

		as_index_ph* ph = &ph_a->phs[ph_a->n_used++];

		ph->r = key_r;
		ph->r_h = r->key_r_h;

		ssi->keyd = NULL;
	}

	ssprig_traverse(ssri, r->right_h, ph_a);
}


//==========================================================
// Local helpers - red-black sprig utilities.
//

static void
insert_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele)
{
	// Entering here, ele is the last element on the stack. It turns out during
	// insert rebalancing we won't ever need new elements on the stack, but make
	// this resemble delete rebalance - define r_e to go back up the tree.
	stack_ele* r_e = ele;
	stack_ele* parent_e = r_e->parent;

	while (parent_e->me->color == RED) {
		stack_ele* grandparent_e = parent_e->parent;

		if (r_e->parent->me_h == grandparent_e->me->left_h) {
			// Element u is r's 'uncle'.
			uarena_handle u_h = grandparent_e->me->right_h;
			index_ele* u = UA_RESOLVE(u_h);

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
					stack_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					rotate_left(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				rotate_right(grandparent_e, parent_e);
			}
		}
		else {
			// Element u is r's 'uncle'.
			uarena_handle u_h = grandparent_e->me->left_h;
			index_ele* u = UA_RESOLVE(u_h);

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
					stack_ele* r0_e = r_e;

					// Move up one layer - r becomes old r's parent.
					r_e = parent_e;

					// Then rotate r back down a layer.
					rotate_right(r_e, r0_e);

					parent_e = r_e->parent;
					// Note - grandparent_e is unchanged.
				}

				parent_e->me->color = BLACK;
				grandparent_e->me->color = RED;

				// r and parent move up a layer as grandparent rotates down.
				rotate_left(grandparent_e, parent_e);
			}
		}
	}

	UA_RESOLVE(root_parent->left_h)->color = BLACK;
}

static void
delete_rebalance(ssprig_info* ssi, index_ele* root_parent, stack_ele* ele)
{
	// Entering here, ele is the last element on the stack. It's possible as r_e
	// crawls up the tree, we'll need new elements on the stack, in which case
	// ele keeps building the stack down while r_e goes up.
	stack_ele* r_e = ele;

	while (r_e->me->color == BLACK && r_e->me_h != root_parent->left_h) {
		index_ele* r_parent = r_e->parent->me;

		if (r_e->me_h == r_parent->left_h) {
			uarena_handle s_h = r_parent->right_h;
			index_ele* s = UA_RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_left(r_e->parent, ele);

				s_h = r_parent->right_h;
				s = UA_RESOLVE(s_h);
			}

			index_ele* s_left = UA_RESOLVE(s->left_h);
			index_ele* s_right = UA_RESOLVE(s->right_h);

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

					stack_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->left_h;
					ele->me = s_left;

					rotate_right(s_e, ele);

					s_h = r_parent->right_h;
					s = s_left; // same as RESOLVE(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				UA_RESOLVE(s->right_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_left(r_e->parent, ele);

				UA_RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
		else {
			uarena_handle s_h = r_parent->left_h;
			index_ele* s = UA_RESOLVE(s_h);

			if (s->color == RED) {
				s->color = BLACK;
				r_parent->color = RED;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_right(r_e->parent, ele);

				s_h = r_parent->left_h;
				s = UA_RESOLVE(s_h);
			}

			index_ele* s_left = UA_RESOLVE(s->left_h);
			index_ele* s_right = UA_RESOLVE(s->right_h);

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

					stack_ele* s_e = ele;

					ele++;
					// ele->parent will be set by rotation.
					ele->me_h = s->right_h;
					ele->me = s_right;

					rotate_left(s_e, ele);

					s_h = r_parent->left_h;
					s = s_right; // same as RESOLVE(s_h)
				}

				s->color = r_parent->color;
				r_parent->color = BLACK;
				UA_RESOLVE(s->left_h)->color = BLACK;

				ele++;
				// ele->parent will be set by rotation.
				ele->me_h = s_h;
				ele->me = s;

				rotate_right(r_e->parent, ele);

				UA_RESOLVE(root_parent->left_h)->color = BLACK;

				return;
			}
		}
	}

	r_e->me->color = BLACK;
}

static void
rotate_left(stack_ele* a, stack_ele* b)
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
rotate_right(stack_ele* a, stack_ele* b)
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


//==========================================================
// 微型Arena API (uarena API)
//

/**
 * 初始化微型arena
 *
 * @param ua 微型arena指针
 *
 * 功能说明：
 * 1. 初始化arena的锁，保证线程安全的分配和释放
 * 2. 添加第一个内存stage，准备进行内存分配
 * 3. 设置分配指针从ID 1开始（ID 0保留为sentinel）
 * 4. 清零sentinel位置，作为无效句柄的标识
 */
static void
uarena_init(uarena* ua)
{
	cf_mutex_init(&ua->lock);

	uarena_add_stage(ua);

	ua->at_ele_id = 1;  // ID 0保留为sentinel
	memset(uarena_resolve(ua, 0), 0, ELE_SIZE);  // 清零sentinel元素
}

/**
 * 销毁微型arena及其所有资源
 *
 * @param ua 微型arena指针
 *
 * 功能说明：
 * 1. 释放所有已分配的内存stage
 * 2. 释放stage指针数组
 * 3. 销毁arena锁
 * 4. 确保没有内存泄漏
 */
static void
uarena_destroy(uarena* ua)
{
	// 释放所有stage的内存
	for (uint32_t i = 0; i < ua->n_stages; i++) {
		cf_free(ua->stages[i]);
	}

	// 释放stage指针数组
	cf_free(ua->stages);

	// 销毁锁
	cf_mutex_destroy(&ua->lock);
}

/**
 * 向微型arena添加新的内存stage
 *
 * @param ua 微型arena指针
 *
 * 功能说明：
 * 1. 分配新的固定大小内存块（4KB）
 * 2. 按需扩展stage指针数组（每次扩展8个指针）
 * 3. 将新stage添加到arena中，增加可用内存容量
 */
static void
uarena_add_stage(uarena* ua)
{
	// 分配固定大小的内存stage
	uint8_t* stage = cf_malloc(STAGE_SIZE);

	// 按需扩展stage指针数组
	if (ua->n_stages % STAGES_STEP == 0) {
		ua->stages = realloc(ua->stages,
				(ua->n_stages + STAGES_STEP) * sizeof(uint8_t*));
	}

	// 添加新stage到数组
	ua->stages[ua->n_stages++] = stage;
}

/**
 * 从微型arena分配一个元素
 *
 * @param ua 微型arena指针
 * @return 分配的元素句柄，0表示分配失败
 *
 * 功能说明：
 * 1. 线程安全地分配单个index_ele大小的内存
 * 2. 优先从自由列表中重用已释放的元素
 * 3. 如果自由列表为空，则从当前stage的末尾分配
 * 4. 如果当前stage已满，自动添加新stage
 * 5. 返回可用于后续解析的句柄
 */
static uarena_handle
uarena_alloc(uarena* ua)
{
	cf_mutex_lock(&ua->lock);

	uarena_handle h;

	// 优先检查自由列表
	if (ua->free_h != 0) {
		h = ua->free_h;
		ua->free_h = NEXT_FREE_H(ua, h);  // 获取下一个自由元素
	}
	// 否则从当前stage末尾分配
	else {
		// 如果当前stage已满，添加新stage
		if (ua->at_ele_id >= STAGE_CAPACITY) {
			uarena_add_stage(ua);
			ua->at_stage_id++;
			ua->at_ele_id = 0;
		}

		// 构造新的句柄并移动分配指针
		uarena_set_handle(&h, ua->at_stage_id, ua->at_ele_id);
		ua->at_ele_id++;
	}

	cf_mutex_unlock(&ua->lock);

	return h;
}

/**
 * 释放微型arena中的元素
 *
 * @param ua 微型arena指针
 * @param h 要释放的元素句柄
 *
 * 功能说明：
 * 1. 线程安全地将元素添加到自由列表
 * 2. 使用链表结构管理自由元素
 * 3. 支持内存重用，提高分配效率
 * 4. 不实际释放内存，而是标记为可重用
 */
static void
uarena_free(uarena* ua, uarena_handle h)
{
	cf_mutex_lock(&ua->lock);

	// 将释放的元素链接到自由列表头部
	NEXT_FREE_H(ua, h) = ua->free_h;
	ua->free_h = h;

	cf_mutex_unlock(&ua->lock);
}
