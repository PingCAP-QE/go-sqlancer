Inspired by Manuel Rigger's paper [Testing Database Engines via Pivoted Query Synthesis](https://arxiv.org/pdf/2001.04174.pdf)

## How to run
```
make
bin/go-sqlancer -dsn "root:@tcp(127.0.0.1:4000)/"
```

## Support Method

- Expression
  - [x] XOR
  - [x] AND
  - [x] OR
  - [x] NOT
  - [x] GT
  - [x] LT
  - [x] NE
  - [x] EQ
  - [x] GE
  - [x] LE
- [x] View
- [x] Table partition

## Issues

- https://github.com/pingcap/tidb/issues/16716
- https://github.com/pingcap/tidb/issues/16679
- https://github.com/pingcap/tidb/issues/16599
- https://github.com/pingcap/tidb/issues/16677
- https://github.com/pingcap/tidb/issues/16788
- https://github.com/tidb-challenge-program/bug-hunting-issue/issues/64
