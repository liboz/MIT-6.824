# MIT-6.824

Just a repo for my work learning Go/distributed system by doing the coursework for [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html)

# Sharded KV
[Full readme](src/shardkv/README.md)

(in the `shardmaster` and `shardkv` folder)
```
go test 
```


Run a test repeatedly
```
./go-test-many.sh
```

Note that shardmaster has been modified from the vanila version by adding `QueryHigher` to allow for a client to fetch all configs higher than a certain number instead of querying it one by one. This was to be higher performance.

# KV
[Full readme](src/kvraft/README.md)
```
go test -race
```

Run a test repeatedly
```
./go-test-many.sh
```

Linearizability Visualizer has been modified to see lines more clearly

# Raft
[Full readme](src/raft/README.md)
```
go test -race
```

Run a test repeatedly
```
./go-test-many.sh
```


# MapReduce

[Full readme](src/mr/README.md)

```
go build -buildmode=plugin ../mrapps/wc.go

rm mr-out*

go run mrmaster.go pg-*.txt
```

Other Window:

```
go run mrworker.go wc.so
```

Test Script:

```
sh ./test-mr.sh
```

Run many `test-mr.sh` many times:

```
./go-test-many.sh
```
