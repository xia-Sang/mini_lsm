# 实现一个lsm
![alt text](/asserts/image.png)

# 存在小问题
- [x] 个人感觉level compact细节有点问题，反正可以跑就可以
- [x] 没有处理好协程，仅仅是顺序执行，效率低下，不过学习使用足够了
- [x] 单体架构，没有考虑更复杂的场景，不过学习使用足够了

# 后续完善
- [?] 完善compact部分，level compact和后台并发运行？
- [?] 完善后台
- [?] 补充bloom filter
- [?] 完善命令行
- [?] 支持C/S模式
- [?] 实现raft协议

# 参考
1. [xiaoxuxiansheng](https://github.com/xiaoxuxiansheng/golsm/blob/main/tree.go)
2. [hengfeiyang](https://github.com/hengfeiyang/lsmdb)
3. [nananatsu](https://github.com/nananatsu/simple-raft)