# MIT-6.824

Just a repo for my work learning Go/distributed system by doing the coursework for [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html)

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
