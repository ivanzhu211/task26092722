# byodstore

BYOD 安全容器的本地数据存储库。仅使用 Go 标准库，状态与数据全部存放在一个本地目录中。

## 用法

```go
s, _ := byodstore.Open("/path/to/data")
defer s.Close()

var id [16]byte // 128 位容器 ID
s.Create(id)
s.Put(id, "entry", reader)          // 流式写入，整体替换
rc, _ := s.Get(id, "entry")         // 流式读取
s.Suspend(id)                       // 拒绝一切数据访问，内容保留
s.Resume(id)
first, _ := s.Wipe(id)              // 幂等；恰好一个调用得到 first == true
```

`Close` 后用同一目录重新 `Open`，状态与已提交内容保持一致。

## 设计要点

- **布局**：`dir/c-<hex(id)>/` 下，`entry-<sha256(name)>` 为已提交条目，`tmp-*` 为写入中的临时文件，`state` 记录 active/suspended/wiped。
- **无全局锁**：`Store` 的锁只保护容器注册表；每个容器一把锁，且绝不在整段流期间持有。不同容器、同容器的多 Get / 多条目 Put 均可并行。
- **Put**：先写临时文件并 fsync，再在容器锁内原子 rename 提交；任何失败都删除临时文件，同名条目保持旧值。源 Reader 由独立 goroutine 泵入有界 channel（每流内存有界），因此 Suspend/Wipe 通过关闭代际 channel 即可让卡住的 Put 立即返回错误，无需等待源 Reader。
- **失效语义**：每次状态迁移递增代际号。已发出的流在每次 Read 前校验代际，Suspend/Wipe 返回后开始的 Read 一律返回错误，Resume 也不复活。
- **Wipe**：在容器锁内置终态、unlink 全部条目与已登记的临时文件后同步返回——返回时目录中已无任何含容器字节的文件；卡住的 Put 之后继续写只会落到已 unlink 的 inode 上，提交也会因代际不符而失败。

## 测试

```sh
go test -race ./...
```

包含并发压力测试：多容器并行 Get 流、Put（部分源 Reader 中途卡住）与随机 Suspend/Resume/Wipe；Wipe 返回后立即扫描整个数据目录、释放卡住的 Put 后再扫一次，发现该容器字节即失败；失效流出字节、Put 返回值与实际内容不符同样失败。
