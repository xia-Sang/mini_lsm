# Go 语言实现的 LSM 树

![LSM 树结构](/asserts/image.png)


本项目是一个基于 Go 语言的简单 LSM（Log-Structured Merge-tree）树实现。LSM 树是一种常用于数据库系统的数据结构，特别适合写密集型场景。这个项目主要用于学习和理解 LSM 树的基本概念和工作原理。

## 特性

- 使用内存中的 MemTable 实现高效写操作
- 通过预写日志（WAL）实现持久化存储
- 自动将 MemTable 压缩到 SST 文件
- 多级存储结构，优化读写性能
- 支持基本操作：Put、Delete 和 Query


## 当前局限性

- [x] level compact目前只是合并所有数据
- [x] 单体架构，没有考虑更复杂的场景，不过学习使用足够了

## 待改进项目

⚡ 补充bloom filter 

⚡ 完善命令行

⚡ 支持C/S模式

⚡ 实现raft协议


## 贡献指南

欢迎提交 Pull Request 来改进这个项目！

## 参考资料
>本项目的实现参考了以下资源：

1. [xiaoxuxiansheng 的 golsm](https://github.com/xiaoxuxiansheng/golsm/blob/main/tree.go)
2. [hengfeiyang 的 lsmdb](https://github.com/hengfeiyang/lsmdb)
3. [nananatsu 的 simple-raft](https://github.com/nananatsu/simple-raft)

