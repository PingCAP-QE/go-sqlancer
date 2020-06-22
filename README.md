# Go-sqlancer

[![Test](https://github.com/chaos-mesh/go-sqlancer/workflows/Test/badge.svg)](https://github.com/chaos-mesh/go-sqlancer/actions?query=workflow%3ATest)
[![Go Report Card](https://goreportcard.com/badge/github.com/chaos-mesh/go-sqlancer)](https://goreportcard.com/report/github.com/chaos-mesh/go-sqlancer)

Inspired by Manuel Rigger's paper [Testing Database Engines via Pivoted Query Synthesis](https://arxiv.org/pdf/2001.04174.pdf).

## Testing approaches

Go-sqlancer has supported Pivoted Query Synthesis (PQS), Non-optimizing Reference Engine Construction (NoREC) and Ternary Logic Partitioning (TLP). You can use `-mode` to specify the testing approach.

## Quickstart

```bash
make
bin/go-sqlancer -dsn "root:@tcp(127.0.0.1:4000)/"
```

And other flags you can set:

```bash
Usage of go-sqlancer:
  -approach string
        use NoRec or PQS method or both, split by vertical bar (default "pqs|norec|tlp")
  -depth int
        sql depth (default 1)
  -dsn string
        dsn of target db for testing
  -duration duration
        fuzz duration (default 5h0m0s)
  -enable-expr-idx
        enable create expression index
  -enable-hint
        enable sql hint for TiDB
  -log-level string
        set log level: info, warn, error, debug [default: info] (default "info")
  -silent
        silent when verify failed
  -view-count int
        count of views to be created (default 10)
```

## Supported Statement

### Functions & Operators

> XOR, AND, OR, NOT, GT, LT, NE, EQ, GE, LE, IF, CASE, IN, BETWEEN, etc.

* https://github.com/pingcap/tidb/issues/16716

```SQL
create table t(a float);
insert t values(NULL);
select * from t where (!(a and a)) is null;

---
tidb> select * from t where (!(a and a)) is null;
Empty set (0.00 sec)
----
mysql> select * from t where (!(a and a)) is null;
+------+
| a    |
+------+
| NULL |
+------+
1 row in set (0.00 sec)
---

```

* https://github.com/pingcap/tidb/issues/16679

```SQL
create table t0(c0 int);
insert into t0 values(null);

---
tidb> select * from t0 where ((!(1.5071004017670217e-01=t0.c0))) IS NULL;
Empty set (0.00 sec)

tidb> select ((!(1.5071004017670217e-01=null))) IS NULL;
+--------------------------------------------+
| ((!(1.5071004017670217e-01=null))) IS NULL |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
1 row in set (0.00 sec)
```

* https://github.com/pingcap/tidb/issues/16788

```SQL
create table t(c int);
insert into t values(1), (NULL);

---
tidb> select c, c = 0.5 from t;
+------+---------+
| c    | c = 0.5 |
+------+---------+
|    1 |       0 |
| NULL |       0 |
+------+---------+
2 rows in set (0.01 sec)
---
mysql> select c, c = 0.5 from t;
+------+---------+
| c    | c = 0.5 |
+------+---------+
|    1 |       0 |
| NULL |    NULL |
+------+---------+
2 rows in set (0.00 sec)
```

* https://github.com/tidb-challenge-program/bug-hunting-issue/issues/64

```SQL
mysql> desc table_int_float;
+-----------+---------+------+------+---------+----------------+
| Field     | Type    | Null | Key  | Default | Extra          |
+-----------+---------+------+------+---------+----------------+
| id        | int(16) | NO   | PRI  | NULL    | auto_increment |
| col_int   | int(16) | YES  |      | NULL    |                |
| col_float | float   | YES  | MUL  | NULL    |                |
+-----------+---------+------+------+---------+----------------+
3 rows in set (0.00 sec)
mysql> select col_float from table_varchar_float;
+-----------+
| col_float |
+-----------+
|      NULL |
+-----------+

---
tidb> SELECT * FROM table_varchar_float WHERE !(table_varchar_float.col_float and 1) IS NULL;
Empty set (0.00 sec)
```

### View

### Table partition

* https://github.com/pingcap/tidb/issues/16896

```SQL
create table t(id int not null auto_increment, col_int int not null, col_float float, primary key(id, col_int)) partition by range(col_int) (partition p0 values less than (100), partition pn values less than (MAXVALUE));
insert into t values(1, 10, 1), (101, 100, 101);

---
tidb> SELECT /*+ use_cascades(TRUE)*/ * from t;
Empty set (0.00 sec)

tidb> SELECT * from t;
+-----+---------+-----------+
| id  | col_int | col_float |
+-----+---------+-----------+
| 101 |     100 |       101 |
|   1 |      10 |         1 |
+-----+---------+-----------+
2 rows in set (0.00 sec)
```

### SQL Hint

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

## Issues found by go-sqlancer

[Fuzz Issues](https://github.com/orgs/pingcap/projects/16)
