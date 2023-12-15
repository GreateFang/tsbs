package query

import (
	"fmt"
	"sync"
)

type VastbaseDB struct {
	HumanLabel       []byte
	HumanDescription []byte

	Hypertable []byte // e.g. "cpu"
	SqlQuery   []byte
	id         uint64
}

// VastbaseDBPool is a sync.Pool of VastbaseDB Query types
var VastbaseDBPool = sync.Pool{
	New: func() interface{} {
		return &VastbaseDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Hypertable:       make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewVastbaseDB returns a new VastbaseDB Query instance
func NewVastbaseDB() *VastbaseDB {
	return VastbaseDBPool.Get().(*VastbaseDB)
}

// GetID returns the ID of this Query
func (q *VastbaseDB) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *VastbaseDB) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *VastbaseDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Hypertable: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Hypertable, q.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *VastbaseDB) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *VastbaseDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *VastbaseDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Hypertable = q.Hypertable[:0]
	q.SqlQuery = q.SqlQuery[:0]

	VastbaseDBPool.Put(q)
}
