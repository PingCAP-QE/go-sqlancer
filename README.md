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
- Hint
  - [x] hash_agg
  - [x] stream_agg
  - [x] agg_to_cop
  - [x] read_consistent_replica
  - [x] no_index_merge
  - [x] use_toja
  - [x] enable_plan_cache
  - [x] use_cascades
  - [x] hash_join
  - [x] merge_join
  - [x] inl_join
  - [x] memory_quota
  - [x] max_execution_time
  - [x] use_index
  - [x] ignore_index
  - [x] use_index_merge
  - [ ] qb_name
  - [ ] time_range
  - [ ] read_from_storage
  - [ ] query_type
  - [ ] inl_hash_join
  - [ ] inl_merge_join

## Issues

- https://github.com/pingcap/tidb/issues/16716
- https://github.com/pingcap/tidb/issues/16679
- https://github.com/pingcap/tidb/issues/16599
- https://github.com/pingcap/tidb/issues/16677
- https://github.com/pingcap/tidb/issues/16788
- https://github.com/pingcap/tidb/issues/16896
- https://github.com/tidb-challenge-program/bug-hunting-issue/issues/64
