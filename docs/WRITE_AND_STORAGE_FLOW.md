# Aerospike 写入与存储流程解析

本文档说明 Aerospike Server 中**数据如何被写入**以及**数据如何存储**，并指向关键代码与注释。

---

## 一、写入流程总览

```
客户端/代理/批量子请求
        │
        ▼
  as_write_start()     [as/src/transaction/write.c]
        │
        ├─ XDR 过滤、存储过载检查
        ├─ rw_request 创建并插入 (ns_ix, key_digest) 哈希
        ├─ 可选：重复解析 (dup_res_start)
        │
        ▼
  write_master()       [as/src/transaction/write.c]
        │
        ├─ 预处理、策略与 set 校验
        ├─ 索引查找或创建 (as_index_lookup / as_index_create)，并加锁
        ├─ as_storage_record_create() 或 as_storage_record_open()
        ├─ 设置 replica 目标、keep_pickle 等
        │
        ▼
  write_master_apply()  [as/src/transaction/write.c]
        │
        ├─ as_storage_rd_load_bins()  从存储加载当前 bins
        ├─ prepare_bin_metadata / advance_record_version / set_xdr_write
        ├─ write_master_bin_ops()     应用消息中的 bin 操作
        ├─ 判删、校验、transition_delete_metadata
        │
        ▼
  as_storage_record_write(rd)   [as/src/storage/storage.c]
        │
        └─ 按 ns->storage_type 分发:
             ├─ as_storage_record_write_mem()   [drv_mem_ce.c -> drv_mem.c]
             ├─ as_storage_record_write_pmem()
             └─ as_storage_record_write_ssd()
```

---

## 二、存储层：数据如何落盘（以 Memory 引擎为例）

Memory 引擎写入路径：

1. **as_storage_record_write_mem** (`as/src/storage/drv_mem_ce.c`)  
   - 若是纯删除（无 pickle 且无 bins）则不再写设备，由上层删索引；否则调用 `write_record(rd)`。

2. **write_record** (`as/src/storage/drv_mem.c`)  
   - 根据 key digest 选择写入设备：`rd->mem = &mems->mems[mem_get_file_id(mems, &r->keyd)]`。  
   - 若是覆盖写，会记录旧 `rblock_id`/`n_rblocks`，写入成功后释放旧 block。  
   - 调用 `write_bins(rd)`（在 CE 中即 `buffer_bins(rd)`）。

3. **buffer_bins** (`as/src/storage/drv_mem.c`)  
   - 计算 flat 大小：无 pickle 时用 `as_flat_record_size(rd)`，有则用 `rd->orig_pickle_sz`。  
   - 若当前 **mwb**（mem write block）剩余空间不足，则将当前 mwb 入队刷盘并获取新 mwb。  
   - 在 mwb 中预留 `write_sz`（按 RBLOCK 对齐），得到写入位置 `mwb_pos`。  
   - **将记录写入 mwb**：  
     - 若没有预打包：`as_flat_pack_record(rd, n_rblocks, false, flat_in_mwb)` 将 `rd` 打包成 `as_flat_record` 写到 `mwb->base_addr[mwb_pos]`。  
     - 若有 pickle（如副本写）：直接 `memcpy(flat, flat_sz)` 到同一位置。  
   - 写入 **end_mark**。  
   - 若需要发副本（`rd->keep_pickle`），则复制一份到 `rd->pickle`。  
   - 更新索引中的存储位置：`r->file_id`、`r->rblock_id`、`r->n_rblocks`，并更新 namespace/set 的统计。

---

## 三、数据存储格式：as_flat_record

- **定义**：`as/include/storage/flat.h` 中的 `as_flat_record`。  
- **布局概要**：  
  - 固定头部：magic、n_rblocks、若干标志位、tree_id、keyd、last_update_time、generation。  
  - 可变部分在 `data[]` 中：可选 extra_flags、void_time、set 名、key、n_bins、压缩信息，然后是按顺序排列的 **bins**。

- **bin 的 flat 格式**（与 `flatten_bins` / `as_flat_unpack_bins` 对应）：  
  - 每个 bin：`[name_len][name][可选 meta][粒子 flat 数据]`。  
  - 读时通过 `as_flat_unpack_bins` 解析回 `as_bin` 数组。

- **打包入口**：  
  - `as_flat_pack_record(rd, n_rblocks, dirty, flat)`：先 `flatten_record_meta` 写元数据并得到 `data[]` 起始指针，再 `flatten_bins(rd, buf, NULL)` 写所有 bin。  
  - 实现见 `as/src/storage/flat.c`（已加中文注释）。

---

## 四、关键文件与函数索引

| 阶段           | 文件 | 函数/位置 | 说明 |
|----------------|------|-----------|------|
| 写入口         | `as/src/transaction/write.c` | `as_write_start` | 写事务入口，XDR/过载检查，rw_request，write_master |
| 主分写         | 同上 | `write_master` | 索引查找/创建，打开/创建 rd，调用 write_master_apply |
| 应用写并落盘   | 同上 | `write_master_apply` | 加载 bins、bin ops、**as_storage_record_write**、sindex |
| 存储分发       | `as/src/storage/storage.c` | `as_storage_record_write` | 按 storage_type 调用 mem/ssd/pmem 写 |
| Memory 写入口  | `as/src/storage/drv_mem_ce.c` | `as_storage_record_write_mem` | 判断是否写设备，调用 write_record |
| 单条记录写     | `as/src/storage/drv_mem.c` | `write_record` | 选设备，write_bins，成功后释放旧 block |
| 写入写块       | `as/src/storage/drv_mem.c` | `buffer_bins` | 预留 mwb 空间、打包或 memcpy flat、end_mark、更新 r->rblock_id 等 |
| 记录打包       | `as/src/storage/flat.c` | `as_flat_pack_record` | 将 rd 打包成 as_flat_record |
| 元数据打包     | `as/src/storage/flat.c` | `flatten_record_meta` | 写 flat 头部与 data[] 前部 |
| Bins 打包      | `as/src/storage/flat.c` | `flatten_bins` | 将 rd->bins 按 flat 格式写入 buf |

---

## 五、索引与存储的对应关系

- **as_index**（内存中的记录索引）中每条 **as_record** 保存：  
  - `file_id`、`rblock_id`、`n_rblocks`：表示该记录在哪个设备的哪个“块”以及占多少 rblock。  
- 读记录时：通过 `as_storage_record_open` 打开 rd，再根据 `rblock_id` 在对应设备的写块/文件中定位到 `as_flat_record`，用 `as_flat_unpack_*` 解析出 bins。  
- 写记录时：在 mwb 中分配一段连续空间写入 as_flat_record，然后把这段空间的起始位置换算成 `rblock_id` 写回 `r->rblock_id`、`r->n_rblocks`。

以上流程在代码中已用中文注释标出关键步骤，便于对照阅读。
