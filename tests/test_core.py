#!/usr/bin/env python3
"""核心逻辑单元测试（仅标准库，本机可直接运行）：

  python3 -I tests/test_core.py

覆盖：LSN 换算、时间规范化、wal2json 事务应用的幂等与原子性、
     读事务快照一致性（每次取数只读同一已提交状态）。
"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from canonical import canonical_line, norm_ts            # noqa: E402
from changes import apply_transaction                    # noqa: E402
from lsnutil import int_to_lsn, lsn_to_int               # noqa: E402
from store import MirrorNotReady, Store, VersionNameTaken  # noqa: E402
from versions import diff_catalogs                       # noqa: E402

TS = "2026-11-14 08:00:00.123456+00"


def change(kind, eid, ts=TS, lat=34.567, lon=100.123, mag=4.5):
    if kind == "delete":
        return {
            "kind": "delete", "schema": "public", "table": "seismic_events",
            "oldkeys": {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid]},
        }
    return {
        "kind": kind, "schema": "public", "table": "seismic_events",
        "columnnames": ["event_id", "origin_time", "latitude", "longitude", "magnitude"],
        "columntypes": ["text", "timestamp with time zone", "numeric", "numeric", "numeric"],
        "columnvalues": [eid, ts, lat, lon, mag],
    }


def change_rename(eid_old, eid_new):
    ch = change("update", eid_new)
    ch["oldkeys"] = {"keynames": ["event_id"], "keytypes": ["text"], "keyvalues": [eid_old]}
    return ch


class LsnTest(unittest.TestCase):
    def test_roundtrip(self):
        for s in ("0/0", "0/1B3C4D0", "1B3/4A0", "FF/FFFFFFFF", "2/9A08C800"):
            self.assertEqual(int_to_lsn(lsn_to_int(s)), s)
        self.assertLess(lsn_to_int("0/10"), lsn_to_int("1/0"))


class NormTsTest(unittest.TestCase):
    def test_load_path_equals_replication_path(self):
        from_load = norm_ts(datetime(2026, 11, 14, 8, 0, 0, 123456, tzinfo=timezone.utc))
        from_wal = norm_ts("2026-11-14 08:00:00.123456+00")
        self.assertEqual(from_load, from_wal)
        self.assertEqual(from_wal, "2026-11-14T08:00:00.123456Z")

    def test_other_offsets_normalized_to_utc(self):
        self.assertEqual(
            norm_ts("2026-11-14 16:00:00+08:00"), "2026-11-14T08:00:00.000000Z"
        )

    def test_canonical_line_decimal_float_equal(self):
        a = canonical_line("EQ1", "T", Decimal("34.500"), Decimal("100.100"), Decimal("4.0"))
        b = canonical_line("EQ1", "T", 34.5, 100.1, 4.0)
        self.assertEqual(a, b)
        self.assertEqual(a, "EQ1|T|34.500|100.100|4.0")


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "mirror.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def apply(self, payload, lsn):
        with self.store.write_txn() as cur:
            counts = apply_transaction(cur, payload)
            Store.set_meta(cur, "last_applied_lsn", lsn)
            return counts

    def rows(self):
        return self.store.read(
            "SELECT event_id, origin_time, latitude, longitude, magnitude FROM events ORDER BY event_id"
        )


class ApplyTest(StoreTestBase):
    def test_insert_update_delete_flow(self):
        self.apply({"change": [change("insert", "A"), change("insert", "B", mag=6.1)]}, "0/10")
        self.assertEqual([r[0] for r in self.rows()], ["A", "B"])

        self.apply({"change": [change("update", "A", mag=5.5)]}, "0/20")
        row_a = [r for r in self.rows() if r[0] == "A"][0]
        self.assertEqual(row_a[4], 5.5)
        self.assertEqual(row_a[1], "2026-11-14T08:00:00.123456Z")   # 时间已规范化

        self.apply({"change": [change("delete", "B")]}, "0/30")
        self.assertEqual([r[0] for r in self.rows()], ["A"])
        self.assertEqual(self.store.get_meta("last_applied_lsn"), "0/30")

    def test_replay_is_idempotent(self):
        """保存后未确认 -> 源库重发 -> 幂等重放，最终状态不变、不重。"""
        txns = [
            ({"change": [change("insert", "A"), change("insert", "B")]}, "0/10"),
            ({"change": [change("update", "A", mag=7.2)]}, "0/20"),
            ({"change": [change("delete", "B")]}, "0/30"),
        ]
        for payload, lsn in txns:
            self.apply(payload, lsn)
        before = self.rows()
        for payload, lsn in txns:          # 模拟崩溃重启后整段重发
            self.apply(payload, lsn)
        self.assertEqual(self.rows(), before)

    def test_old_value_never_overwrites_new(self):
        """按 LSN 顺序应用：旧事务重放只会被后续新事务再次覆盖为新值。"""
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        # 崩溃重放：旧 insert 又来一遍，随后新 update 也重放，最终仍是新值
        self.apply({"change": [change("insert", "A", mag=3.0)]}, "0/10")
        self.apply({"change": [change("update", "A", mag=8.8)]}, "0/20")
        self.assertEqual(self.rows()[0][4], 8.8)

    def test_primary_key_rename(self):
        self.apply({"change": [change("insert", "OLD")]}, "0/10")
        self.apply({"change": [change_rename("OLD", "NEW")]}, "0/20")
        self.assertEqual([r[0] for r in self.rows()], ["NEW"])

    def test_truncate_ignored(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        counts = self.apply({"change": [{"kind": "truncate", "schema": "public", "table": "seismic_events"}]}, "0/20")
        self.assertEqual(counts["other"], 1)
        self.assertEqual(len(self.rows()), 1)

    def test_transaction_atomicity_on_failure(self):
        """一笔源事务应用一半失败 -> 整体回滚，数据与位置都不前移。"""
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as cur:
                apply_transaction(cur, {"change": [change("insert", "A"), change("insert", "B")]})
                Store.set_meta(cur, "last_applied_lsn", "0/10")
                raise RuntimeError("模拟写入失败")
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.store.get_meta("last_applied_lsn"))

    def test_whole_transaction_visible_together(self):
        """一笔多行事务提交后整笔可见；读事务只看到提交前或提交后。"""
        with self.store.write_txn() as cur:
            apply_transaction(cur, {"change": [change("insert", f"E{i:02d}") for i in range(20)]})
            Store.set_meta(cur, "last_applied_lsn", "0/10")
        self.assertEqual(len(self.rows()), 20)


class SnapshotReadTest(StoreTestBase):
    def test_read_txn_sees_one_committed_state(self):
        self.apply({"change": [change("insert", "A")]}, "0/10")
        txn = self.store.read_txn()
        try:
            n1 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
            # 读事务打开期间，写者提交了新事务
            self.apply({"change": [change("insert", "B")]}, "0/20")
            n2 = txn.execute("SELECT count(*) FROM events").fetchone()[0]
        finally:
            txn.close()
        self.assertEqual((n1, n2), (1, 1))          # 同一已提交状态
        self.assertEqual(self.store.read("SELECT count(*) FROM events")[0][0], 2)  # 新读见新态


class VersionSaveTest(StoreTestBase):
    """目录版本留存：行、续采位置、就绪状态来自同一已提交镜像。"""

    def make_ready(self, lsn="0/10"):
        self.apply({"change": [change("insert", "A"), change("insert", "B", mag=6.1)]}, lsn)
        with self.store.write_txn() as cur:
            Store.set_meta(cur, "load_state", "done")

    def version_rows(self, vid):
        return self.store.read(
            "SELECT event_id, origin_time, latitude, longitude, magnitude"
            " FROM version_events WHERE version_id=? ORDER BY event_id", (vid,)
        )

    def test_save_captures_rows_lsn_and_ready_state(self):
        self.make_ready()
        vid, n, lsn = self.store.save_version("第43周交付")
        self.assertEqual((n, lsn), (2, "0/10"))
        vs = self.store.list_versions()
        self.assertEqual(len(vs), 1)
        self.assertEqual(vs[0][1], "第43周交付")          # name
        self.assertEqual(vs[0][3], "0/10")                # source_lsn
        self.assertEqual(vs[0][4], 2)                     # row_count
        self.assertEqual([r[0] for r in self.version_rows(vid)], ["A", "B"])

    def test_saved_version_is_frozen_snapshot(self):
        """留存后镜像继续变化，已存版本内容不变。"""
        self.make_ready()
        vid, _n, _lsn = self.store.save_version("v1")
        self.apply({"change": [change("update", "A", mag=9.9), change("delete", "B")]}, "0/20")
        rows = self.version_rows(vid)
        self.assertEqual([r[0] for r in rows], ["A", "B"])
        self.assertEqual(rows[0][4], 4.5)                 # 仍是留存时的旧值

    def test_save_requires_ready_state(self):
        """未就绪（idle/loading/catchup）时不得存下目录。"""
        self.apply({"change": [change("insert", "A")]}, "0/10")   # 状态仍是 idle
        for state in ("idle", "loading", "catchup"):
            with self.store.write_txn() as cur:
                Store.set_meta(cur, "load_state", state)
            with self.assertRaises(MirrorNotReady):
                self.store.save_version(f"v-{state}")
        self.assertEqual(self.store.list_versions(), [])
        self.assertEqual(
            self.store.read("SELECT count(*) FROM version_events")[0][0], 0
        )

    def test_save_name_must_be_unique(self):
        self.make_ready()
        self.store.save_version("v1")
        with self.assertRaises(VersionNameTaken):
            self.store.save_version("v1")
        self.assertEqual(len(self.store.list_versions()), 1)

    def test_failed_save_leaves_no_half_product(self):
        """留存中失败：事务整体回滚，不留可选的半成品，镜像本身也不受影响。"""
        self.make_ready()

        def boom(msg, cur=None):
            raise RuntimeError("模拟留存中失败")

        self.store.log = boom
        with self.assertRaises(RuntimeError):
            self.store.save_version("v1")
        self.assertEqual(self.store.list_versions(), [])
        self.assertEqual(self.store.read("SELECT count(*) FROM version_events")[0][0], 0)
        self.assertEqual(self.store.read("SELECT count(*) FROM events")[0][0], 2)

    def test_restart_cleanup_removes_half_product(self):
        """进程重启：任何非 ready 的残留版本在启动时被清除，不可被选择。"""
        with self.store.write_txn() as cur:
            cur.execute(
                "INSERT INTO versions(name, created_at, source_lsn, row_count, status)"
                " VALUES('half', 't', '0/10', 1, 'building')"
            )
            vid = cur.lastrowid
            cur.execute(
                "INSERT INTO version_events"
                "(version_id, event_id, origin_time, latitude, longitude, magnitude)"
                " VALUES(?, 'A', 't', 1.0, 2.0, 3.0)", (vid,)
            )
        # 重新打开同一文件，模拟进程重启
        self.store = Store(os.path.join(self.tmp.name, "mirror.db"))
        self.assertEqual(self.store.list_versions(), [])
        self.assertEqual(self.store.read("SELECT count(*) FROM version_events")[0][0], 0)

    def test_versions_survive_rebuild(self):
        """重建镜像只清当前目录，已存版本仍可对照；重建期间不得留存。"""
        self.make_ready()
        vid, _n, _lsn = self.store.save_version("重建前留存")
        # 模拟 replicator._reset_for_build：清镜像、状态切回 loading
        with self.store.write_txn() as cur:
            cur.execute("DELETE FROM events")
            Store.set_meta(cur, "load_state", "loading")
            Store.set_meta(cur, "last_applied_lsn", "")
        self.assertEqual(len(self.store.list_versions()), 1)
        self.assertEqual([r[0] for r in self.version_rows(vid)], ["A", "B"])
        with self.assertRaises(MirrorNotReady):
            self.store.save_version("重建期间留存")

    def test_version_read_txn_isolated_from_concurrent_save(self):
        """对照期间另有人留存版本：同一读事务里的本次结果不变。"""
        self.make_ready()
        txn = self.store.read_txn()
        try:
            n1 = txn.execute("SELECT count(*) FROM versions").fetchone()[0]
            self.store.save_version("v1")                  # 他人此时留存了新版本
            n2 = txn.execute("SELECT count(*) FROM versions").fetchone()[0]
        finally:
            txn.close()
        self.assertEqual((n1, n2), (0, 0))                 # 本次对照所见不变
        self.assertEqual(len(self.store.list_versions()), 1)


class DiffTest(unittest.TestCase):
    """目录版本对照：新增 / 删除 / 字段更正的旧新值。"""

    A = ("EQ-1", "2026-11-14T08:00:00.000000Z", 30.0, 100.0, 4.5)
    B = ("EQ-2", "2026-11-14T09:00:00.000000Z", 31.0, 101.0, 5.0)
    C = ("EQ-3", "2026-11-14T10:00:00.000000Z", 32.0, 102.0, 6.0)

    def test_added_deleted_changed(self):
        rows_a = [self.A, self.B, self.C]
        b2 = ("EQ-2", "2026-11-14T09:00:00.000000Z", 31.5, 101.0, 5.5)   # 纬度+震级被重新测定
        d4 = ("EQ-4", "2026-11-14T11:00:00.000000Z", 33.0, 103.0, 7.0)
        rows_b = [b2, self.C, d4]
        diff = diff_catalogs(rows_a, rows_b)
        self.assertEqual([r[0] for r in diff["added"]], ["EQ-4"])
        self.assertEqual([r[0] for r in diff["deleted"]], ["EQ-1"])
        self.assertEqual(
            diff["changed"],
            [("EQ-2", [("latitude", 31.0, 31.5), ("magnitude", 5.0, 5.5)])],
        )

    def test_identical_catalogs_have_empty_diff(self):
        diff = diff_catalogs([self.A, self.B], [self.B, self.A])   # 顺序无关
        self.assertEqual(diff, {"added": [], "deleted": [], "changed": []})

    def test_results_sorted_by_event_id(self):
        rows_a = [("EQ-9", "t", 1.0, 1.0, 1.0), ("EQ-1", "t", 1.0, 1.0, 1.0)]
        rows_b = [("EQ-8", "t", 2.0, 2.0, 2.0), ("EQ-2", "t", 2.0, 2.0, 2.0)]
        diff = diff_catalogs(rows_a, rows_b)
        self.assertEqual([r[0] for r in diff["added"]], ["EQ-2", "EQ-8"])
        self.assertEqual([r[0] for r in diff["deleted"]], ["EQ-1", "EQ-9"])

    def test_origin_time_change_reported(self):
        a = ("EQ-1", "2026-11-14T08:00:00.000000Z", 30.0, 100.0, 4.5)
        b = ("EQ-1", "2026-11-14T08:01:23.456789Z", 30.0, 100.0, 4.5)
        diff = diff_catalogs([a], [b])
        self.assertEqual(
            diff["changed"],
            [("EQ-1", [("origin_time", a[1], b[1])])],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
