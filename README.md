Inspired by Manuel Rigger's paper [Testing Database Engines via Pivoted Query Synthesis](https://arxiv.org/pdf/2001.04174.pdf)

## How to run
```
make pivot
bin/pivot -d "root@tcp(127.0.0.1:4000)/"
```

## Support Method

- [ ] Expression
  - [x] OR
  - [x] NOT
- [ ] View
- [ ] Partition

## Issues
- https://github.com/pingcap/tidb/issues/16716
- https://github.com/pingcap/tidb/issues/16679
- https://github.com/pingcap/tidb/issues/16599
- https://github.com/pingcap/tidb/issues/16677
