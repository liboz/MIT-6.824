package raft

import (
	"log"
	"os"
	"strconv"
)

// Debugging
var Debug = parseEnvOrDefault("RAFT_DEBUG", 0)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrint(v ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Print(v...)
	}
	return
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseEnvOrDefault(envVarName string, defaultValue int) int {
	raftDebug := os.Getenv(envVarName)
	i, err := strconv.Atoi(raftDebug)
	if err != nil {
		return defaultValue
	} else {
		return i
	}
}
