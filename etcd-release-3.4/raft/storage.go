package raft

import (
	"errors"
	"sync"

	pb "go.etcd.io/etcd/raft/raftpb"
)

// ErrCompacted在请求的索引不可用时由Storage.Entries/Compact返回，因为它先于最后一个快照。
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate在被请求的索引比现有快照更早时由Storage.CreateSnapshot返回。
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// 当请求的日志项不可用时，存储接口返回ErrUnavailable。
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// 当所需的快照暂时不可用时，存储接口返回ErrSnapshotTemporarilyUnavailable。
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage是应用程序实现的接口，用于从存储中检索日志条目。如果任何存储方法返回错误，raft实例将无法操作并拒绝参与选举;在这种情况下，应用程序负责清理和恢复。
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	InitialState() (pb.HardState, pb.ConfState, error) // 返回Storage中记录的状态信息，返回的是HardState和ConfState实例。

	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) // // Entries方法返回指定范围的Entry记录[lo,hi).maxSize限定了返回的Entry集合的字节数上限。Entries返回至少一个条目(如果有的话)

	Term(i uint64) (uint64, error) // 查询指定Index对应的Entry的Term值。

	LastIndex() (uint64, error) // 该方法返回Storage中记录的第一条Entry的索引值。

	FirstIndex() (uint64, error) // 该方法返回Storage中记录的第一条Entry的索引值。在该Entry之前的所有Entry都已经被包含最近的一次snapshot中。

	Snapshot() (pb.Snapshot, error) // 返回最近一次生成的快照数据。
}

// MemoryStorage实现内存中数组支持的存储接口。
type MemoryStorage struct {
	// 为保护所有的字段访问。MemoryStorage的大多数方法在raft goroutine上运行，但Append()在应用程序goroutine上运行。
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i]有raft日志位置 i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage 创建一个空的MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// 当从头开始时，在第0项处填充一个伪条目.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState 实现Storage接口.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState 保存当前的HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries 实现Storage接口. 负责查询指定范围的Entry.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {  // 如果待查询的最小Index值(lo)小于FirstIndex.则直接抛出异常。
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 { // 如果待查询的最大Index值(lo)大于LastIndex.则直接抛出异常。
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	if len(ms.ents) == 1 { // 如果MemoryStorage.ents只包含一条Entry,则其为空Entry。直接抛出异常。
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset] // 获取lo~hi之间的Entry,并返回。
	return limitSize(ents, maxSize), nil // 限制返回Entry切片的总字节大小
}

// Term 实现Storage接口.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex 实现Storage接口.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex 实现Storage接口.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot 实现Storage接口.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot 用给定快照的内容覆盖此存储对象的内容.
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock() // 加锁同步，MemoryStorage实现了sync.Mutex
	defer ms.Unlock() // 方法结束后，释放锁。

	// 通过快照的元数据比较当前MemoryStorage中记录的Snapshot与待处理的Snapshot数据新旧程度。
	msIndex := ms.snapshot.Metadata.Index // ms的Snapshot最后一条记录的Index的值。
	snapIndex := snap.Metadata.Index // 待处理Snapshot最后一条记录的Index的值。
	if msIndex >= snapIndex { // 比较两个pb.Snapshot所包含的最后一条记录的Index的值，如果待处理Snapshot数据比较旧，则直接抛出异常。
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap // 更新MemoryStorage.snapshot字段
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}} // 重置MemoryStorage.ents字段，此时在ents中只有一个空的Entry实例。
	return nil
}
// 参数说明：i是新建snapshot包含的最大索引值，cs是当前集群的状态，data是新建snapshot的具体数据
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// 边界检查，i必须大于当前snapshot包含的最大Index值，并且小于MemoryStorage的LastIndex值，否则抛出异常。
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}
	// 更新MemoryStorage.snapshot的元数据
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data // 更新具体快照数据
	return ms.snapshot, nil
}

// Compact将MemoryStorage.ents中指定索引之前的Entry记录全部抛弃，从而实现压缩MemoryStorage.ents的目的。
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// 创建新的切片，用来存储compactIndex之后的Entry
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 将compactIndex之后的Entry拷贝到ents中，并更新MemoryStorage.ents字段。
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
// 设置完快照数据之后，就可以开始向MemoryStorage中追加Entry记录了。
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 { // 检测entries切片的长度。
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex() // 获取当前MemoryStorage的FistIndex值
	last := entries[0].Index + uint64(len(entries)) - 1 // 获取待添加的最后一条Entry的Index值。

	if last < first { // entries切片中所有的Entry都已经过时，无须添加任何Entry
		return nil
	}
	if first > entries[0].Index { // first之前的Entry已经记入Snapshot中，不应该再记录到ents中，所以将这部分Entry截掉。
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index // 计算entries切片中第一条可用的Entry与first之间的差距。
	switch {
	case uint64(len(ms.ents)) > offset:
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...) // 保留MemoryStorage.ents中first~offset的部分。offset之后的被抛弃
		ms.ents = append(ms.ents, entries...) // 然后将待追加的Entry追加到MemoryStorage.ents中。
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...) // 直接将待追加的Entry追加到MemoryStorage.ents中。
	default:
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
