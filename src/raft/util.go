package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	// 使用 append 来将 rf.me, rf.currentTerm, rf.state 和 a 展开为一个参数列表
	args := append([]interface{}{rf.me, rf.currentTerm, rf.state, rf.votedFor}, a...)
	DPrintf("[me: %d, term: %d, state: %d, voteFor: %d]\t\t"+format, args...)
}

type Overtime_t int

const (
	ElectionTime Overtime_t = iota
	HeartBeatTime
)

func RandDuration(ot Overtime_t) int64 {
	if ot == ElectionTime {
		return 150 + (rand.Int63() % 200)
	} else {
		return 100
	}
}
