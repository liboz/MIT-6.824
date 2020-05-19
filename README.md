# MIT-6.824

Just a repo for my work learning Go by doing the coursework for [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html)

# MapReduce

Full readme at https://github.com/liboz/MIT-6.824/blob/master/src/mr/README.md

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
