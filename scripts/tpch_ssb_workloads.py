# SPDX-License-Identifier: Apache-2.0
"""Deterministic multi-table TPC-H and SSB workloads with dual offline oracles.

The schemas, seeded row generators, query declarations, pure-Python reference
oracles, and SQLite SQL oracle are completely independent of ShardLoom runtime
code and produce comparison expectations without external query fallback.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import datetime as dt
import math
from pathlib import Path
import random
import sqlite3
from typing import Any

FLOAT_REL_TOL = 1e-6
FLOAT_ABS_TOL = 1e-6

TPCH_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "region": [
        ("r_regionkey", "int64"),
        ("r_name", "utf8"),
        ("r_comment", "utf8"),
    ],
    "nation": [
        ("n_nationkey", "int64"),
        ("n_name", "utf8"),
        ("n_regionkey", "int64"),
        ("n_comment", "utf8"),
    ],
    "supplier": [
        ("s_suppkey", "int64"),
        ("s_name", "utf8"),
        ("s_address", "utf8"),
        ("s_nationkey", "int64"),
        ("s_phone", "utf8"),
        ("s_acctbal", "float64"),
        ("s_comment", "utf8"),
    ],
    "part": [
        ("p_partkey", "int64"),
        ("p_name", "utf8"),
        ("p_mfgr", "utf8"),
        ("p_brand", "utf8"),
        ("p_type", "utf8"),
        ("p_size", "int64"),
        ("p_container", "utf8"),
        ("p_retailprice", "float64"),
        ("p_comment", "utf8"),
    ],
    "partsupp": [
        ("ps_partkey", "int64"),
        ("ps_suppkey", "int64"),
        ("ps_availqty", "int64"),
        ("ps_supplycost", "float64"),
        ("ps_comment", "utf8"),
    ],
    "customer": [
        ("c_custkey", "int64"),
        ("c_name", "utf8"),
        ("c_address", "utf8"),
        ("c_nationkey", "int64"),
        ("c_phone", "utf8"),
        ("c_acctbal", "float64"),
        ("c_mktsegment", "utf8"),
        ("c_comment", "utf8"),
    ],
    "orders": [
        ("o_orderkey", "int64"),
        ("o_custkey", "int64"),
        ("o_orderstatus", "utf8"),
        ("o_totalprice", "float64"),
        ("o_orderdate", "utf8"),
        ("o_orderpriority", "utf8"),
        ("o_clerk", "utf8"),
        ("o_shippriority", "int64"),
        ("o_comment", "utf8"),
    ],
    "lineitem": [
        ("l_orderkey", "int64"),
        ("l_partkey", "int64"),
        ("l_suppkey", "int64"),
        ("l_linenumber", "int64"),
        ("l_quantity", "float64"),
        ("l_extendedprice", "float64"),
        ("l_discount", "float64"),
        ("l_tax", "float64"),
        ("l_returnflag", "utf8"),
        ("l_linestatus", "utf8"),
        ("l_shipdate", "utf8"),
        ("l_commitdate", "utf8"),
        ("l_receiptdate", "utf8"),
        ("l_shipinstruct", "utf8"),
        ("l_shipmode", "utf8"),
        ("l_comment", "utf8"),
    ],
}

SSB_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "date_dim": [
        ("d_datekey", "int64"),
        ("d_date", "utf8"),
        ("d_dayofweek", "utf8"),
        ("d_month", "utf8"),
        ("d_year", "int64"),
        ("d_yearmonthnum", "int64"),
        ("d_yearmonth", "utf8"),
        ("d_daynuminweek", "int64"),
        ("d_daynuminmonth", "int64"),
        ("d_daynuminyear", "int64"),
        ("d_monthnuminyear", "int64"),
        ("d_weeknuminyear", "int64"),
        ("d_sellingseason", "utf8"),
        ("d_lastdayinweekfl", "int64"),
        ("d_lastdayinmonthfl", "int64"),
        ("d_holidayfl", "int64"),
        ("d_weekdayfl", "int64"),
    ],
    "customer": [
        ("c_custkey", "int64"),
        ("c_name", "utf8"),
        ("c_address", "utf8"),
        ("c_city", "utf8"),
        ("c_nation", "utf8"),
        ("c_region", "utf8"),
        ("c_phone", "utf8"),
        ("c_mktsegment", "utf8"),
    ],
    "supplier": [
        ("s_suppkey", "int64"),
        ("s_name", "utf8"),
        ("s_address", "utf8"),
        ("s_city", "utf8"),
        ("s_nation", "utf8"),
        ("s_region", "utf8"),
        ("s_phone", "utf8"),
    ],
    "part": [
        ("p_partkey", "int64"),
        ("p_name", "utf8"),
        ("p_mfgr", "utf8"),
        ("p_category", "utf8"),
        ("p_brand1", "utf8"),
        ("p_color", "utf8"),
        ("p_type", "utf8"),
        ("p_size", "int64"),
        ("p_container", "utf8"),
    ],
    "lineorder": [
        ("lo_orderkey", "int64"),
        ("lo_linenumber", "int64"),
        ("lo_custkey", "int64"),
        ("lo_partkey", "int64"),
        ("lo_suppkey", "int64"),
        ("lo_orderdate", "int64"),
        ("lo_orderpriority", "utf8"),
        ("lo_shippriority", "utf8"),
        ("lo_quantity", "int64"),
        ("lo_extendedprice", "float64"),
        ("lo_ordertotalprice", "float64"),
        ("lo_discount", "int64"),
        ("lo_revenue", "float64"),
        ("lo_supplycost", "float64"),
        ("lo_tax", "int64"),
        ("lo_commitdate", "int64"),
        ("lo_shipmode", "utf8"),
    ],
}

_REGIONS = ("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST")
_NATIONS = (
    ("ALGERIA", 0), ("ETHIOPIA", 0), ("KENYA", 0), ("MOROCCO", 0), ("MOZAMBIQUE", 0),
    ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("PERU", 1), ("UNITED STATES", 1),
    ("INDIA", 2), ("INDONESIA", 2), ("JAPAN", 2), ("CHINA", 2), ("VIETNAM", 2),
    ("FRANCE", 3), ("GERMANY", 3), ("ROMANIA", 3), ("RUSSIA", 3), ("UNITED KINGDOM", 3),
    ("EGYPT", 4), ("IRAN", 4), ("IRAQ", 4), ("JORDAN", 4), ("SAUDI ARABIA", 4),
)
_SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD")
_PRIORITIES = ("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW")
_SHIPMODES = ("REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB")
_CONTAINERS = ("SM CASE", "SM BOX", "MED BAG", "LG DRUM", "WRAP CASE")
_MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)
_DAYS_OF_WEEK = (
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
)


@dataclass(frozen=True)
class QuerySpec:
    query_id: int
    suite: str
    code: str
    sql: str
    tables: tuple[str, ...]
    ordered: bool = True


TPCH_QUERIES: tuple[QuerySpec, ...] = (
    QuerySpec(
        1,
        "tpch",
        "TPCH-Q1",
        "SELECT l_returnflag, l_linestatus, "
        "SUM(l_quantity) AS sum_qty, "
        "SUM(l_extendedprice) AS sum_base_price, "
        "SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price, "
        "SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge, "
        "AVG(l_quantity) AS avg_qty, "
        "AVG(l_extendedprice) AS avg_price, "
        "AVG(l_discount) AS avg_disc, "
        "COUNT(*) AS count_order "
        "FROM lineitem "
        "WHERE l_shipdate <= '1998-09-02' "
        "GROUP BY l_returnflag, l_linestatus "
        "ORDER BY l_returnflag ASC, l_linestatus ASC",
        ("lineitem",),
    ),
    QuerySpec(
        2,
        "tpch",
        "TPCH-Q3",
        "SELECT l.l_orderkey AS l_orderkey, "
        "SUM(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue, "
        "o.o_orderdate AS o_orderdate, "
        "o.o_shippriority AS o_shippriority "
        "FROM customer AS c "
        "INNER JOIN orders AS o ON c.c_custkey = o.o_custkey "
        "INNER JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey "
        "WHERE c.c_mktsegment = 'BUILDING' "
        "AND o.o_orderdate < '1995-03-15' "
        "AND l.l_shipdate > '1995-03-15' "
        "GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority "
        "ORDER BY revenue DESC, o.o_orderdate ASC, l.l_orderkey ASC "
        "LIMIT 10",
        ("customer", "orders", "lineitem"),
    ),
    QuerySpec(
        3,
        "tpch",
        "TPCH-Q4",
        "SELECT o.o_orderpriority AS o_orderpriority, COUNT(*) AS order_count "
        "FROM orders AS o "
        "WHERE o.o_orderdate >= '1993-07-01' "
        "AND o.o_orderdate < '1993-10-01' "
        "AND EXISTS (SELECT 1 FROM lineitem AS l "
        "WHERE l.l_orderkey = o.o_orderkey AND l.l_commitdate < l.l_receiptdate) "
        "GROUP BY o.o_orderpriority "
        "ORDER BY o.o_orderpriority ASC",
        ("orders", "lineitem"),
    ),
    QuerySpec(
        4,
        "tpch",
        "TPCH-Q5",
        "SELECT n.n_name AS n_name, "
        "SUM(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue "
        "FROM customer AS c "
        "INNER JOIN orders AS o ON c.c_custkey = o.o_custkey "
        "INNER JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey "
        "INNER JOIN supplier AS s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey "
        "INNER JOIN nation AS n ON s.s_nationkey = n.n_nationkey "
        "INNER JOIN region AS r ON n.n_regionkey = r.r_regionkey "
        "WHERE r.r_name = 'ASIA' "
        "AND o.o_orderdate >= '1994-01-01' "
        "AND o.o_orderdate < '1995-01-01' "
        "GROUP BY n.n_name "
        "ORDER BY revenue DESC, n.n_name ASC",
        ("customer", "orders", "lineitem", "supplier", "nation", "region"),
    ),
    QuerySpec(
        5,
        "tpch",
        "TPCH-Q6",
        "SELECT SUM(l_extendedprice * l_discount) AS revenue "
        "FROM lineitem "
        "WHERE l_shipdate >= '1994-01-01' "
        "AND l_shipdate < '1995-01-01' "
        "AND l_discount >= 0.05 AND l_discount <= 0.07 "
        "AND l_quantity < 24.0",
        ("lineitem",),
        ordered=False,
    ),
    QuerySpec(
        6,
        "tpch",
        "TPCH-Q10",
        "SELECT c.c_custkey AS c_custkey, c.c_name AS c_name, "
        "SUM(l.l_extendedprice * (1.0 - l.l_discount)) AS revenue, "
        "c.c_acctbal AS c_acctbal, n.n_name AS n_name, "
        "c.c_address AS c_address, c.c_phone AS c_phone, c.c_comment AS c_comment "
        "FROM customer AS c "
        "INNER JOIN orders AS o ON c.c_custkey = o.o_custkey "
        "INNER JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey "
        "INNER JOIN nation AS n ON c.c_nationkey = n.n_nationkey "
        "WHERE o.o_orderdate >= '1993-10-01' "
        "AND o.o_orderdate < '1994-01-01' "
        "AND l.l_returnflag = 'R' "
        "GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, n.n_name, c.c_address, c.c_comment "
        "ORDER BY revenue DESC, c.c_custkey ASC "
        "LIMIT 20",
        ("customer", "orders", "lineitem", "nation"),
    ),
    QuerySpec(
        7,
        "tpch",
        "TPCH-Q12",
        "SELECT l.l_shipmode AS l_shipmode, "
        "SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' "
        "THEN 1 ELSE 0 END) AS high_line_count, "
        "SUM(CASE WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' "
        "THEN 1 ELSE 0 END) AS low_line_count "
        "FROM orders AS o "
        "INNER JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey "
        "WHERE l.l_shipmode IN ('MAIL', 'SHIP') "
        "AND l.l_commitdate < l.l_receiptdate "
        "AND l.l_shipdate < l.l_commitdate "
        "AND l.l_receiptdate >= '1994-01-01' "
        "AND l.l_receiptdate < '1995-01-01' "
        "GROUP BY l.l_shipmode "
        "ORDER BY l.l_shipmode ASC",
        ("orders", "lineitem"),
    ),
    QuerySpec(
        8,
        "tpch",
        "TPCH-Q14",
        "SELECT 100.0 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' "
        "THEN l.l_extendedprice * (1.0 - l.l_discount) ELSE 0.0 END) / "
        "SUM(l.l_extendedprice * (1.0 - l.l_discount)) AS promo_revenue "
        "FROM lineitem AS l "
        "INNER JOIN part AS p ON l.l_partkey = p.p_partkey "
        "WHERE l.l_shipdate >= '1995-09-01' AND l.l_shipdate < '1995-10-01'",
        ("lineitem", "part"),
        ordered=False,
    ),
)

SSB_QUERIES: tuple[QuerySpec, ...] = (
    QuerySpec(
        9,
        "ssb",
        "SSB-Q1.1",
        "SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue "
        "FROM lineorder AS lo "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "WHERE d.d_year = 1993 AND lo.lo_discount BETWEEN 1 AND 3 AND lo.lo_quantity < 25",
        ("lineorder", "date_dim"),
        ordered=False,
    ),
    QuerySpec(
        10,
        "ssb",
        "SSB-Q1.2",
        "SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue "
        "FROM lineorder AS lo "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "WHERE d.d_yearmonthnum = 199401 AND lo.lo_discount BETWEEN 4 AND 6 AND lo.lo_quantity BETWEEN 26 AND 35",
        ("lineorder", "date_dim"),
        ordered=False,
    ),
    QuerySpec(
        11,
        "ssb",
        "SSB-Q2.1",
        "SELECT SUM(lo.lo_revenue) AS revenue, d.d_year AS d_year, p.p_brand1 AS p_brand1 "
        "FROM lineorder AS lo "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "INNER JOIN part AS p ON lo.lo_partkey = p.p_partkey "
        "INNER JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey "
        "WHERE p.p_category = 'MFGR#12' AND s.s_region = 'AMERICA' "
        "GROUP BY d.d_year, p.p_brand1 "
        "ORDER BY d.d_year ASC, p.p_brand1 ASC",
        ("lineorder", "date_dim", "part", "supplier"),
    ),
    QuerySpec(
        12,
        "ssb",
        "SSB-Q3.1",
        "SELECT c.c_nation AS c_nation, s.s_nation AS s_nation, d.d_year AS d_year, "
        "SUM(lo.lo_revenue) AS revenue "
        "FROM customer AS c "
        "INNER JOIN lineorder AS lo ON lo.lo_custkey = c.c_custkey "
        "INNER JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "WHERE c.c_region = 'ASIA' AND s.s_region = 'ASIA' "
        "AND d.d_year >= 1992 AND d.d_year <= 1997 "
        "GROUP BY c.c_nation, s.s_nation, d.d_year "
        "ORDER BY d.d_year ASC, revenue DESC, c.c_nation ASC, s.s_nation ASC",
        ("customer", "lineorder", "supplier", "date_dim"),
    ),
    QuerySpec(
        13,
        "ssb",
        "SSB-Q4.1",
        "SELECT d.d_year AS d_year, c.c_nation AS c_nation, "
        "SUM(lo.lo_revenue - lo.lo_supplycost) AS profit "
        "FROM lineorder AS lo "
        "INNER JOIN customer AS c ON lo.lo_custkey = c.c_custkey "
        "INNER JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey "
        "INNER JOIN part AS p ON lo.lo_partkey = p.p_partkey "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "WHERE c.c_region = 'AMERICA' AND s.s_region = 'AMERICA' "
        "AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2') "
        "GROUP BY d.d_year, c.c_nation "
        "ORDER BY d.d_year ASC, c.c_nation ASC",
        ("lineorder", "customer", "supplier", "part", "date_dim"),
    ),
    QuerySpec(
        14,
        "ssb",
        "SSB-Q4.2",
        "SELECT d.d_year AS d_year, s.s_nation AS s_nation, p.p_category AS p_category, "
        "SUM(lo.lo_revenue - lo.lo_supplycost) AS profit "
        "FROM lineorder AS lo "
        "INNER JOIN customer AS c ON lo.lo_custkey = c.c_custkey "
        "INNER JOIN supplier AS s ON lo.lo_suppkey = s.s_suppkey "
        "INNER JOIN part AS p ON lo.lo_partkey = p.p_partkey "
        "INNER JOIN date_dim AS d ON lo.lo_orderdate = d.d_datekey "
        "WHERE c.c_region = 'AMERICA' AND s.s_region = 'AMERICA' "
        "AND (d.d_year = 1997 OR d.d_year = 1998) "
        "AND (p.p_mfgr = 'MFGR#1' OR p.p_mfgr = 'MFGR#2') "
        "GROUP BY d.d_year, s.s_nation, p.p_category "
        "ORDER BY d.d_year ASC, s.s_nation ASC, p.p_category ASC",
        ("lineorder", "customer", "supplier", "part", "date_dim"),
    ),
)

ALL_QUERIES: tuple[QuerySpec, ...] = TPCH_QUERIES + SSB_QUERIES


def select_queries(suite: str, query_codes: list[str] | None = None) -> list[QuerySpec]:
    if suite == "tpch":
        selected = list(TPCH_QUERIES)
    elif suite == "ssb":
        selected = list(SSB_QUERIES)
    elif suite == "all":
        selected = list(ALL_QUERIES)
    else:
        raise ValueError(f"unknown suite: {suite}")
    if query_codes:
        wanted = {code.strip().upper() for code in query_codes if code.strip()}
        selected = [spec for spec in selected if spec.code.upper() in wanted]
        if len(selected) != len(wanted):
            raise ValueError(f"unknown query codes in {sorted(wanted)}")
    return selected


def generate_tpch_tables(scale_rows: int = 12000, *, seed: int = 20261124) -> dict[str, list[dict[str, Any]]]:
    if scale_rows < 60:
        raise ValueError("scale_rows must be >= 60")
    rng = random.Random(seed)
    region = [
        {"r_regionkey": idx, "r_name": name, "r_comment": f"region-{idx}"}
        for idx, name in enumerate(_REGIONS)
    ]
    nation = [
        {"n_nationkey": idx, "n_name": name, "n_regionkey": region_key, "n_comment": f"nation-{idx}"}
        for idx, (name, region_key) in enumerate(_NATIONS)
    ]
    sup_count = max(25, scale_rows // 160)
    supplier = [
        {
            "s_suppkey": key,
            "s_name": f"Supplier#{key:09d}",
            "s_address": f"Addr-{key}",
            "s_nationkey": (key - 1) % len(nation),
            "s_phone": f"{10 + ((key - 1) % 25):02d}-{100 + key:04d}",
            "s_acctbal": round((key * 37.5) % 9000.0 - 500.0, 2),
            "s_comment": f"sup-comment-{key}",
        }
        for key in range(1, sup_count + 1)
    ]
    part_count = max(40, scale_rows // 50)
    part_types = ("PROMO BURNISHED COPPER", "PROMO PLATED TIN", "STANDARD ANODIZED STEEL", "SMALL BRUSHED BRASS")
    part = [
        {
            "p_partkey": key,
            "p_name": f"part-{key}",
            "p_mfgr": f"MFGR#{((key - 1) % 5) + 1}",
            "p_brand": f"Brand#{((key - 1) % 5) + 1}{((key - 1) % 5) + 1}",
            "p_type": part_types[(key - 1) % len(part_types)],
            "p_size": ((key - 1) % 50) + 1,
            "p_container": _CONTAINERS[(key - 1) % len(_CONTAINERS)],
            "p_retailprice": round(900.0 + (key % 200) * 1.25, 2),
            "p_comment": f"part-comment-{key}",
        }
        for key in range(1, part_count + 1)
    ]
    partsupp = [
        {
            "ps_partkey": p["p_partkey"],
            "ps_suppkey": supplier[((p["p_partkey"] - 1 + offset) % sup_count)]["s_suppkey"],
            "ps_availqty": 100 + ((p["p_partkey"] * 17 + offset * 31) % 900),
            "ps_supplycost": round(25.0 + ((p["p_partkey"] + offset) % 250) * 0.75, 2),
            "ps_comment": f"ps-{p['p_partkey']}-{offset}",
        }
        for p in part
        for offset in (0, 1)
    ]
    cust_count = max(50, scale_rows // 25)
    customer = [
        {
            "c_custkey": key,
            "c_name": f"Customer#{key:09d}",
            "c_address": f"CustAddr-{key}",
            "c_nationkey": (key - 1) % len(nation),
            "c_phone": f"{10 + ((key - 1) % 25):02d}-{200 + key:04d}",
            "c_acctbal": round(100.0 + ((key * 53.0) % 8000.0), 2),
            "c_mktsegment": _SEGMENTS[(key - 1) % len(_SEGMENTS)],
            "c_comment": f"cust-comment-{key}",
        }
        for key in range(1, cust_count + 1)
    ]
    # Anchor key calendar windows first so every query matches non-empty subsets
    # even at tiny unit-test scale_rows.
    anchor_dates = ("1993-07-15", "1993-10-15", "1994-03-10", "1994-08-20", "1995-01-15", "1995-09-12")
    order_count = max(80, scale_rows // 3)
    base_order_day = dt.date(1992, 1, 1)
    orders: list[dict[str, Any]] = []
    for key in range(1, order_count + 1):
        if key <= 12:
            odate = anchor_dates[(key - 1) % len(anchor_dates)]
            custkey = ((key - 1) % cust_count) + 1
            # Ensure BUILDING segment exists before 1995-03-15 and ASIA customers exist in 1994.
            if key in (1, 2):
                custkey = 2  # c_custkey=2 -> BUILDING, nation=1 (ARGENTINA)
            elif key in (3, 4):
                custkey = 11  # c_custkey=11 -> JAPAN/ASIA region 2
        else:
            odate = (base_order_day + dt.timedelta(days=rng.randint(0, 2350))).isoformat()
            custkey = rng.randint(1, cust_count)
        orders.append(
            {
                "o_orderkey": key,
                "o_custkey": custkey,
                "o_orderstatus": ("F", "O", "P")[key % 3],
                "o_totalprice": round(500.0 + (key % 500) * 19.37, 2),
                "o_orderdate": odate,
                "o_orderpriority": _PRIORITIES[(key - 1) % len(_PRIORITIES)],
                "o_clerk": f"Clerk#{((key - 1) % 20) + 1:09d}",
                "o_shippriority": key % 4,
                "o_comment": f"order-{key}",
            }
        )
    lineitem: list[dict[str, Any]] = []
    for idx in range(scale_rows):
        ord_row = orders[idx % 12] if idx < 16 else orders[idx % order_count]
        odate = dt.date.fromisoformat(ord_row["o_orderdate"])
        partkey = ((idx * 7) % part_count) + 1
        # Keep Q5 ASIA supplier matches dense for ASIA customers cleanly on idx % 5 == 0.
        cust_nation = customer[ord_row["o_custkey"] - 1]["c_nationkey"]
        if idx < 16 or idx % 9 == 0:
            matching_sups = [s["s_suppkey"] for s in supplier if s["s_nationkey"] == cust_nation]
            suppkey = matching_sups[idx % len(matching_sups)] if matching_sups else ((idx % sup_count) + 1)
        else:
            suppkey = (idx % sup_count) + 1
        if idx < 16:
            ship_dt = odate + dt.timedelta(days=12 + (idx % 10))
            commit_dt = odate + dt.timedelta(days=20 + (idx % 10))
            receipt_dt = odate + dt.timedelta(days=30 + (idx % 10))
            shipmode = ("MAIL", "SHIP")[idx % 2]
            discount = (0.05, 0.06, 0.07)[idx % 3]
            qty = float(10 + (idx % 12))
            retflag = "R" if idx % 2 == 0 or idx in (1, 7) else "N"
            partkey = 1 if idx % 3 == 0 else ((idx % part_count) + 1)
            # Force some rows onto 1995-09-12 and 1994-06-01 for Q6 & Q14 anchors.
            if idx == 4:
                ship_dt = dt.date(1994, 6, 1)
            elif idx == 5:
                ship_dt = dt.date(1995, 9, 12)
            elif idx == 6:
                ship_dt = dt.date(1995, 9, 15)
                partkey = 3  # STANDARD
            elif idx == 7:
                ship_dt = dt.date(1995, 4, 1)
        else:
            ship_dt = odate + dt.timedelta(days=rng.randint(1, 90))
            commit_dt = odate + dt.timedelta(days=rng.randint(5, 80))
            receipt_dt = commit_dt + dt.timedelta(days=rng.randint(-10, 25))
            shipmode = _SHIPMODES[rng.randrange(len(_SHIPMODES))]
            discount = rng.randint(0, 10) / 100.0
            qty = float(rng.randint(1, 50))
            retflag = "R" if ship_dt <= dt.date(1995, 6, 17) and idx % 2 == 0 else ("A" if idx % 3 == 0 else "N")
        ext_price = round(qty * (100.0 + (partkey % 90) * 1.13 + (idx % 7) * 0.29), 2)
        tax = ((idx % 9) * 0.01)
        lineitem.append(
            {
                "l_orderkey": ord_row["o_orderkey"],
                "l_partkey": partkey,
                "l_suppkey": suppkey,
                "l_linenumber": (idx // order_count) + 1,
                "l_quantity": qty,
                "l_extendedprice": ext_price,
                "l_discount": round(discount, 2),
                "l_tax": round(tax, 2),
                "l_returnflag": retflag,
                "l_linestatus": "F" if ord_row["o_orderstatus"] == "F" else "O",
                "l_shipdate": ship_dt.isoformat(),
                "l_commitdate": commit_dt.isoformat(),
                "l_receiptdate": receipt_dt.isoformat(),
                "l_shipinstruct": ("DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN")[idx % 4],
                "l_shipmode": shipmode,
                "l_comment": f"li-{idx}",
            }
        )
    return {
        "region": region,
        "nation": nation,
        "supplier": supplier,
        "part": part,
        "partsupp": partsupp,
        "customer": customer,
        "orders": orders,
        "lineitem": lineitem,
    }


def generate_ssb_tables(scale_rows: int = 12000, *, seed: int = 20261124) -> dict[str, list[dict[str, Any]]]:
    if scale_rows < 60:
        raise ValueError("scale_rows must be >= 60")
    rng = random.Random(seed + 17)
    date_keys: list[int] = []
    date_dim: list[dict[str, Any]] = []
    for year in range(1992, 1999):
        for month in (1, 4, 7, 10):
            for day in (1, 10, 20):
                d = dt.date(year, month, day)
                dkey = year * 10000 + month * 100 + day
                date_keys.append(dkey)
                date_dim.append(
                    {
                        "d_datekey": dkey,
                        "d_date": d.isoformat(),
                        "d_dayofweek": _DAYS_OF_WEEK[d.weekday()],
                        "d_month": _MONTHS[month - 1],
                        "d_year": year,
                        "d_yearmonthnum": year * 100 + month,
                        "d_yearmonth": f"{_MONTHS[month - 1][:3]}{year}",
                        "d_daynuminweek": d.weekday() + 1,
                        "d_daynuminmonth": day,
                        "d_daynuminyear": d.timetuple().tm_yday,
                        "d_monthnuminyear": month,
                        "d_weeknuminyear": d.isocalendar().week,
                        "d_sellingseason": ("Winter", "Spring", "Summer", "Fall")[(month - 1) // 3],
                        "d_lastdayinweekfl": int(d.weekday() == 6),
                        "d_lastdayinmonthfl": 0,
                        "d_holidayfl": int(month == 1 and day == 1),
                        "d_weekdayfl": int(d.weekday() < 5),
                    }
                )
    cust_count = max(50, scale_rows // 25)
    customer = [
        {
            "c_custkey": key,
            "c_name": f"Cust#{key:09d}",
            "c_address": f"SsbCust-{key}",
            "c_city": f"{_NATIONS[(key - 1) % len(_NATIONS)][0][:5]}{key % 10}",
            "c_nation": _NATIONS[(key - 1) % len(_NATIONS)][0],
            "c_region": _REGIONS[_NATIONS[(key - 1) % len(_NATIONS)][1]],
            "c_phone": f"{10 + (key % 25):02d}-{300 + key:04d}",
            "c_mktsegment": _SEGMENTS[(key - 1) % len(_SEGMENTS)],
        }
        for key in range(1, cust_count + 1)
    ]
    sup_count = max(25, scale_rows // 160)
    supplier = [
        {
            "s_suppkey": key,
            "s_name": f"Supp#{key:09d}",
            "s_address": f"SsbSupp-{key}",
            "s_city": f"{_NATIONS[(key - 1) % len(_NATIONS)][0][:5]}{key % 10}",
            "s_nation": _NATIONS[(key - 1) % len(_NATIONS)][0],
            "s_region": _REGIONS[_NATIONS[(key - 1) % len(_NATIONS)][1]],
            "s_phone": f"{10 + (key % 25):02d}-{400 + key:04d}",
        }
        for key in range(1, sup_count + 1)
    ]
    part_count = max(40, scale_rows // 50)
    part = [
        {
            "p_partkey": key,
            "p_name": f"ssb-part-{key}",
            "p_mfgr": f"MFGR#{((key - 1) % 5) + 1}",
            "p_category": f"MFGR#{((key - 1) % 5) + 1}{((key - 1) % 2) + 1}",
            "p_brand1": f"MFGR#{((key - 1) % 5) + 1}{((key - 1) % 2) + 1}{((key - 1) % 4) + 1:02d}",
            "p_color": ("red", "blue", "green", "ivory", "coral")[(key - 1) % 5],
            "p_type": ("STANDARD", "SMALL", "LARGE")[key % 3],
            "p_size": ((key - 1) % 50) + 1,
            "p_container": _CONTAINERS[(key - 1) % len(_CONTAINERS)],
        }
        for key in range(1, part_count + 1)
    ]
    america_custs = [c["c_custkey"] for c in customer if c["c_region"] == "AMERICA"]
    america_sups = [s["s_suppkey"] for s in supplier if s["s_region"] == "AMERICA"]
    asia_custs = [c["c_custkey"] for c in customer if c["c_region"] == "ASIA"]
    asia_sups = [s["s_suppkey"] for s in supplier if s["s_region"] == "ASIA"]
    mfgr12_parts = [p["p_partkey"] for p in part if p["p_category"] == "MFGR#12"]
    lineorder: list[dict[str, Any]] = []
    anchor_orderdates = (19930110, 19940110, 19950410, 19970710, 19980110)
    for idx in range(scale_rows):
        if idx < 20:
            dkey = anchor_orderdates[idx % len(anchor_orderdates)]
            if idx % 2 == 0:
                ckey = america_custs[idx % len(america_custs)]
                skey = america_sups[idx % len(america_sups)]
            else:
                ckey = asia_custs[idx % len(asia_custs)]
                skey = asia_sups[idx % len(asia_sups)]
            pkey = mfgr12_parts[idx % len(mfgr12_parts)]
            qty = 20 if idx % 2 == 0 else 30
            disc = 2 if idx % 2 == 0 else 5
        else:
            dkey = date_keys[rng.randrange(len(date_keys))]
            ckey = rng.randint(1, cust_count)
            skey = rng.randint(1, sup_count)
            pkey = rng.randint(1, part_count)
            qty = rng.randint(1, 50)
            disc = rng.randint(0, 10)
        ext = round(qty * (100.0 + (pkey % 70) * 1.37 + (idx % 5) * 0.41), 2)
        rev = round(ext * (1.0 - disc / 100.0), 2)
        supplycost = round(ext * 0.65, 2)
        lineorder.append(
            {
                "lo_orderkey": (idx // 3) + 1,
                "lo_linenumber": (idx % 3) + 1,
                "lo_custkey": ckey,
                "lo_partkey": pkey,
                "lo_suppkey": skey,
                "lo_orderdate": dkey,
                "lo_orderpriority": _PRIORITIES[idx % len(_PRIORITIES)],
                "lo_shippriority": str(idx % 3),
                "lo_quantity": qty,
                "lo_extendedprice": ext,
                "lo_ordertotalprice": round(ext * 1.05, 2),
                "lo_discount": disc,
                "lo_revenue": rev,
                "lo_supplycost": supplycost,
                "lo_tax": idx % 8,
                "lo_commitdate": dkey,
                "lo_shipmode": _SHIPMODES[idx % len(_SHIPMODES)],
            }
        )
    return {
        "date_dim": date_dim,
        "customer": customer,
        "supplier": supplier,
        "part": part,
        "lineorder": lineorder,
    }


def python_oracle_query(spec: QuerySpec, tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Pure-Python independent reference evaluator for every declared TPC-H and SSB query."""
    code = spec.code
    if code == "TPCH-Q1":
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in tables["lineitem"]:
            if row["l_shipdate"] <= "1998-09-02":
                groups[(row["l_returnflag"], row["l_linestatus"])].append(row)
        out = []
        for (rf, ls), members in sorted(groups.items()):
            n = len(members)
            sq = sum(r["l_quantity"] for r in members)
            sp = sum(r["l_extendedprice"] for r in members)
            sd = sum(r["l_discount"] for r in members)
            out.append(
                {
                    "l_returnflag": rf,
                    "l_linestatus": ls,
                    "sum_qty": sq,
                    "sum_base_price": sp,
                    "sum_disc_price": sum(r["l_extendedprice"] * (1.0 - r["l_discount"]) for r in members),
                    "sum_charge": sum(r["l_extendedprice"] * (1.0 - r["l_discount"]) * (1.0 + r["l_tax"]) for r in members),
                    "avg_qty": sq / n,
                    "avg_price": sp / n,
                    "avg_disc": sd / n,
                    "count_order": n,
                }
            )
        return out
    if code == "TPCH-Q3":
        building = {c["c_custkey"] for c in tables["customer"] if c["c_mktsegment"] == "BUILDING"}
        ords = {o["o_orderkey"]: o for o in tables["orders"] if o["o_custkey"] in building and o["o_orderdate"] < "1995-03-15"}
        q3_groups: dict[tuple[int, str, int], float] = defaultdict(float)
        for l in tables["lineitem"]:
            o = ords.get(l["l_orderkey"])
            if o is not None and l["l_shipdate"] > "1995-03-15":
                q3_groups[(l["l_orderkey"], o["o_orderdate"], o["o_shippriority"])] += l["l_extendedprice"] * (1.0 - l["l_discount"])
        rows = [
            {"l_orderkey": k, "revenue": rev, "o_orderdate": d, "o_shippriority": p}
            for (k, d, p), rev in q3_groups.items()
        ]
        return sorted(rows, key=lambda r: (-r["revenue"], r["o_orderdate"], r["l_orderkey"]))[:10]
    if code == "TPCH-Q4":
        late = {l["l_orderkey"] for l in tables["lineitem"] if l["l_commitdate"] < l["l_receiptdate"]}
        counts: dict[str, int] = defaultdict(int)
        for o in tables["orders"]:
            if "1993-07-01" <= o["o_orderdate"] < "1993-10-01" and o["o_orderkey"] in late:
                counts[o["o_orderpriority"]] += 1
        return [{"o_orderpriority": k, "order_count": v} for k, v in sorted(counts.items())]
    if code == "TPCH-Q5":
        asia_regions = {r["r_regionkey"] for r in tables["region"] if r["r_name"] == "ASIA"}
        asia_nations = {n["n_nationkey"]: n for n in tables["nation"] if n["n_regionkey"] in asia_regions}
        sups = {s["s_suppkey"]: s for s in tables["supplier"] if s["s_nationkey"] in asia_nations}
        custs = {c["c_custkey"]: c for c in tables["customer"]}
        ords = {o["o_orderkey"]: o for o in tables["orders"] if "1994-01-01" <= o["o_orderdate"] < "1995-01-01"}
        rev_by_nation: dict[str, float] = defaultdict(float)
        for l in tables["lineitem"]:
            o = ords.get(l["l_orderkey"])
            s = sups.get(l["l_suppkey"])
            if o is None or s is None:
                continue
            c = custs.get(o["o_custkey"])
            if c is not None and c["c_nationkey"] == s["s_nationkey"]:
                nname = asia_nations[s["s_nationkey"]]["n_name"]
                rev_by_nation[nname] += l["l_extendedprice"] * (1.0 - l["l_discount"])
        return sorted(
            [{"n_name": k, "revenue": v} for k, v in rev_by_nation.items()],
            key=lambda r: (-r["revenue"], r["n_name"]),
        )
    if code == "TPCH-Q6":
        total = sum(
            l["l_extendedprice"] * l["l_discount"]
            for l in tables["lineitem"]
            if "1994-01-01" <= l["l_shipdate"] < "1995-01-01"
            and 0.05 <= l["l_discount"] <= 0.07
            and l["l_quantity"] < 24.0
        )
        return [{"revenue": total}]
    if code == "TPCH-Q10":
        nations = {n["n_nationkey"]: n["n_name"] for n in tables["nation"]}
        custs = {c["c_custkey"]: c for c in tables["customer"]}
        ords = {o["o_orderkey"]: o for o in tables["orders"] if "1993-10-01" <= o["o_orderdate"] < "1994-01-01"}
        rev_by_cust: dict[int, float] = defaultdict(float)
        for l in tables["lineitem"]:
            if l["l_returnflag"] == "R" and l["l_orderkey"] in ords:
                rev_by_cust[ords[l["l_orderkey"]]["o_custkey"]] += l["l_extendedprice"] * (1.0 - l["l_discount"])
        rows = []
        for ck, rev in rev_by_cust.items():
            c = custs[ck]
            rows.append(
                {
                    "c_custkey": c["c_custkey"],
                    "c_name": c["c_name"],
                    "revenue": rev,
                    "c_acctbal": c["c_acctbal"],
                    "n_name": nations[c["c_nationkey"]],
                    "c_address": c["c_address"],
                    "c_phone": c["c_phone"],
                    "c_comment": c["c_comment"],
                }
            )
        return sorted(rows, key=lambda r: (-r["revenue"], r["c_custkey"]))[:20]
    if code == "TPCH-Q12":
        ords = {o["o_orderkey"]: o for o in tables["orders"]}
        q12_groups: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for l in tables["lineitem"]:
            if (
                l["l_shipmode"] in ("MAIL", "SHIP")
                and l["l_commitdate"] < l["l_receiptdate"]
                and l["l_shipdate"] < l["l_commitdate"]
                and "1994-01-01" <= l["l_receiptdate"] < "1995-01-01"
                and l["l_orderkey"] in ords
            ):
                prio = ords[l["l_orderkey"]]["o_orderpriority"]
                slot = 0 if prio in ("1-URGENT", "2-HIGH") else 1
                q12_groups[l["l_shipmode"]][slot] += 1
        return [
            {"l_shipmode": mode, "high_line_count": counts[0], "low_line_count": counts[1]}
            for mode, counts in sorted(q12_groups.items())
        ]
    if code == "TPCH-Q14":
        parts = {p["p_partkey"]: p for p in tables["part"]}
        promo = 0.0
        total = 0.0
        for l in tables["lineitem"]:
            if "1995-09-01" <= l["l_shipdate"] < "1995-10-01" and l["l_partkey"] in parts:
                net = l["l_extendedprice"] * (1.0 - l["l_discount"])
                total += net
                if parts[l["l_partkey"]]["p_type"].startswith("PROMO"):
                    promo += net
        return [{"promo_revenue": 100.0 * promo / total}]
    if code == "SSB-Q1.1":
        dates = {d["d_datekey"] for d in tables["date_dim"] if d["d_year"] == 1993}
        rev = sum(
            lo["lo_extendedprice"] * lo["lo_discount"]
            for lo in tables["lineorder"]
            if lo["lo_orderdate"] in dates and 1 <= lo["lo_discount"] <= 3 and lo["lo_quantity"] < 25
        )
        return [{"revenue": float(rev)}]
    if code == "SSB-Q1.2":
        dates = {d["d_datekey"] for d in tables["date_dim"] if d["d_yearmonthnum"] == 199401}
        rev = sum(
            lo["lo_extendedprice"] * lo["lo_discount"]
            for lo in tables["lineorder"]
            if lo["lo_orderdate"] in dates and 4 <= lo["lo_discount"] <= 6 and 26 <= lo["lo_quantity"] <= 35
        )
        return [{"revenue": float(rev)}]
    if code == "SSB-Q2.1":
        dates = {d["d_datekey"]: d["d_year"] for d in tables["date_dim"]}
        parts = {p["p_partkey"]: p["p_brand1"] for p in tables["part"] if p["p_category"] == "MFGR#12"}
        sups = {s["s_suppkey"] for s in tables["supplier"] if s["s_region"] == "AMERICA"}
        q21: dict[tuple[int, str], float] = defaultdict(float)
        for lo in tables["lineorder"]:
            if lo["lo_orderdate"] in dates and lo["lo_partkey"] in parts and lo["lo_suppkey"] in sups:
                q21[(dates[lo["lo_orderdate"]], parts[lo["lo_partkey"]])] += lo["lo_revenue"]
        return [
            {"revenue": rev, "d_year": yr, "p_brand1": brand}
            for (yr, brand), rev in sorted(q21.items())
        ]
    if code == "SSB-Q3.1":
        dates = {d["d_datekey"]: d["d_year"] for d in tables["date_dim"] if 1992 <= d["d_year"] <= 1997}
        custs = {c["c_custkey"]: c["c_nation"] for c in tables["customer"] if c["c_region"] == "ASIA"}
        sups = {s["s_suppkey"]: s["s_nation"] for s in tables["supplier"] if s["s_region"] == "ASIA"}
        q31: dict[tuple[str, str, int], float] = defaultdict(float)
        for lo in tables["lineorder"]:
            if lo["lo_custkey"] in custs and lo["lo_suppkey"] in sups and lo["lo_orderdate"] in dates:
                q31[(custs[lo["lo_custkey"]], sups[lo["lo_suppkey"]], dates[lo["lo_orderdate"]])] += lo["lo_revenue"]
        rows = [
            {"c_nation": cn, "s_nation": sn, "d_year": yr, "revenue": rev}
            for (cn, sn, yr), rev in q31.items()
        ]
        return sorted(rows, key=lambda r: (r["d_year"], -r["revenue"], r["c_nation"], r["s_nation"]))
    if code == "SSB-Q4.1":
        dates = {d["d_datekey"]: d["d_year"] for d in tables["date_dim"]}
        custs = {c["c_custkey"]: c["c_nation"] for c in tables["customer"] if c["c_region"] == "AMERICA"}
        sups = {s["s_suppkey"] for s in tables["supplier"] if s["s_region"] == "AMERICA"}
        parts = {p["p_partkey"] for p in tables["part"] if p["p_mfgr"] in ("MFGR#1", "MFGR#2")}
        q41: dict[tuple[int, str], float] = defaultdict(float)
        for lo in tables["lineorder"]:
            if (
                lo["lo_custkey"] in custs
                and lo["lo_suppkey"] in sups
                and lo["lo_partkey"] in parts
                and lo["lo_orderdate"] in dates
            ):
                q41[(dates[lo["lo_orderdate"]], custs[lo["lo_custkey"]])] += lo["lo_revenue"] - lo["lo_supplycost"]
        return [
            {"d_year": yr, "c_nation": cn, "profit": prof}
            for (yr, cn), prof in sorted(q41.items())
        ]
    if code == "SSB-Q4.2":
        dates = {d["d_datekey"]: d["d_year"] for d in tables["date_dim"] if d["d_year"] in (1997, 1998)}
        custs = {c["c_custkey"] for c in tables["customer"] if c["c_region"] == "AMERICA"}
        sups = {s["s_suppkey"]: s["s_nation"] for s in tables["supplier"] if s["s_region"] == "AMERICA"}
        parts = {p["p_partkey"]: p["p_category"] for p in tables["part"] if p["p_mfgr"] in ("MFGR#1", "MFGR#2")}
        q42: dict[tuple[int, str, str], float] = defaultdict(float)
        for lo in tables["lineorder"]:
            if (
                lo["lo_custkey"] in custs
                and lo["lo_suppkey"] in sups
                and lo["lo_partkey"] in parts
                and lo["lo_orderdate"] in dates
            ):
                q42[(dates[lo["lo_orderdate"]], sups[lo["lo_suppkey"]], parts[lo["lo_partkey"]])] += (
                    lo["lo_revenue"] - lo["lo_supplycost"]
                )
        return [
            {"d_year": yr, "s_nation": sn, "p_category": cat, "profit": prof}
            for (yr, sn, cat), prof in sorted(q42.items())
        ]
    raise ValueError(f"no python oracle for query {code}")


def sqlite_oracle_suite(
    specs: list[QuerySpec],
    schemas: dict[str, list[tuple[str, str]]],
    tables: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """Independent stdlib SQLite SQL oracle over generated tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        for table_name, cols in schemas.items():
            col_defs = ", ".join(
                f"{name} {'INTEGER' if dtype == 'int64' else ('REAL' if dtype == 'float64' else 'TEXT')} NOT NULL"
                for name, dtype in cols
            )
            conn.execute(f"CREATE TABLE {table_name} ({col_defs})")
            placeholders = ", ".join("?" for _ in cols)
            col_names = [name for name, _ in cols]
            conn.executemany(
                f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})",
                ([row[c] for c in col_names] for row in tables[table_name]),
            )
        results: dict[str, list[dict[str, Any]]] = {}
        for spec in specs:
            cur = conn.execute(spec.sql)
            results[spec.code] = [dict(row) for row in cur.fetchall()]
        return results
    finally:
        conn.close()


def _values_equivalent(actual: Any, expected: Any) -> bool:
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    if actual is None or expected is None:
        return actual is expected
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        af, ef = float(actual), float(expected)
        if not (math.isfinite(af) and math.isfinite(ef)):
            return False
        return math.isclose(af, ef, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL)
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _values_equivalent(actual[k], expected[k]) for k in expected
        )
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _values_equivalent(a, b) for a, b in zip(actual, expected)
        )
    return actual == expected


def _row_sort_key(row: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    items = []
    for k in sorted(row):
        v = row[k]
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            items.append((k, round(float(v), 5)))
        else:
            items.append((k, str(v) if v is not None else ""))
    return tuple(items)


def results_equivalent(
    actual: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    *,
    ordered: bool = True,
) -> bool:
    if len(actual) != len(expected):
        return False
    if ordered:
        return _values_equivalent(actual, expected)
    return _values_equivalent(
        sorted(actual, key=_row_sort_key),
        sorted(expected, key=_row_sort_key),
    )


def verify_dual_oracles(
    suite: str,
    specs: list[QuerySpec],
    tables: dict[str, list[dict[str, Any]]],
    *,
    duckdb_oracle: bool = False,
) -> dict[str, dict[str, Any]]:
    schemas = TPCH_SCHEMAS if suite == "tpch" else SSB_SCHEMAS
    sql_results = sqlite_oracle_suite(specs, schemas, tables)
    verified: dict[str, dict[str, Any]] = {}
    duck_conn = None
    if duckdb_oracle:
        import duckdb  # optional CLI comparison flag only
        import pyarrow as pa

        duck_conn = duckdb.connect(":memory:")
        for table_name, cols in schemas.items():
            arrow_schema = pa.schema(
                [
                    pa.field(
                        name,
                        pa.int64() if dt_name == "int64" else (pa.float64() if dt_name == "float64" else pa.string()),
                        nullable=False,
                    )
                    for name, dt_name in cols
                ]
            )
            duck_conn.register(table_name, pa.Table.from_pylist(tables[table_name], schema=arrow_schema))
    try:
        for spec in specs:
            py_rows = python_oracle_query(spec, tables)
            sq_rows = sql_results[spec.code]
            if not py_rows:
                raise ValueError(f"oracle produced empty rows for {spec.code}; increase scale_rows")
            if not results_equivalent(sq_rows, py_rows, ordered=spec.ordered):
                raise ValueError(
                    f"offline oracle mismatch on {spec.code}: sqlite={sq_rows[:3]} python={py_rows[:3]}"
                )
            oracles = ["python_reference", "sqlite3_sql"]
            if duck_conn is not None:
                cur = duck_conn.execute(spec.sql)
                col_names = [desc[0] for desc in cur.description]
                dk_rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
                if not results_equivalent(dk_rows, py_rows, ordered=spec.ordered):
                    raise ValueError(f"optional duckdb oracle mismatch on {spec.code}")
                oracles.append("duckdb_sql")
            verified[spec.code] = {"expected": py_rows, "oracles": oracles}
    finally:
        if duck_conn is not None:
            duck_conn.close()
    return verified


def build_source_bindings(
    spec: QuerySpec,
    table_paths: dict[str, Path],
    input_format: str,
) -> dict[str, dict[str, str]]:
    """Bind strictly only the tables referenced in `spec.sql`."""
    return {
        str(table_paths[table]): {"input_format": input_format}
        for table in spec.tables
    }
