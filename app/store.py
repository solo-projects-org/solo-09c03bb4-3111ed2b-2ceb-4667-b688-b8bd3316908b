"""SQLite 镜像存储层。

要点：
- WAL 模式 + synchronous=FULL：写事务提交即持久（进程被杀、容器重启不丢已提交数据）；
- 镜像数据与续采位置（meta.last_applied_lsn）在同一个事务里落库，二者永远一致；
- 网页读请求各自使用独立只读事务 => 每次取数只读同一已提交状态，
  绝不会看到某笔源事务应用了一半的中间态。
"""
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    origin_time TEXT NOT NULL,      -- ISO-8601 UTC，如 2026-11-14T08:00:00.123456Z
    latitude    REAL NOT NULL,
    longitude   REAL NOT NULL,
    magnitude   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_origin_time_idx ON events(origin_time);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT NOT NULL,
    msg TEXT NOT NULL
);

-- 留存的目录版本：与当前镜像 events 表相互独立，重建镜像不影响已存版本
CREATE TABLE IF NOT EXISTS versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    source_lsn TEXT NOT NULL,        -- 留存时镜像的续采位置（与震例行同一已提交状态）
    row_count  INTEGER NOT NULL,
    status     TEXT NOT NULL         -- 事务内直接落 'ready'；提交前崩溃不会留下半成品
);
CREATE TABLE IF NOT EXISTS version_events (
    version_id  INTEGER NOT NULL,
    event_id    TEXT NOT NULL,
    origin_time TEXT NOT NULL,
    latitude    REAL NOT NULL,
    longitude   REAL NOT NULL,
    magnitude   REAL NOT NULL,
    PRIMARY KEY (version_id, event_id)
);
"""

LOG_KEEP = 100


class MirrorNotReady(Exception):
    """留存时镜像未就绪（load_state 不是 done）。"""

    def __init__(self, state):
        super().__init__(f"镜像未就绪：{state}")
        self.state = state


class VersionNameTaken(Exception):
    """版本名已被占用。"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Store:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()          # 单写者（采集线程），日志等偶发写也走同一把锁
        self._w = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._w.execute("PRAGMA journal_mode=WAL")
        self._w.execute("PRAGMA synchronous=FULL")
        self._w.execute("PRAGMA busy_timeout=10000")
        self._w.executescript(SCHEMA)
        self._cleanup_incomplete_versions()

    def _cleanup_incomplete_versions(self):
        """启动兜底清理：保证「可选的」版本都是完整版本。

        留存本身是单事务（见 save_version），提交前崩溃会整体回滚，正常不会
        留下半成品；这里再防御性清除任何非 ready 的残留及其震例行。
        """
        with self.write_txn() as cur:
            cur.execute("DELETE FROM versions WHERE status <> 'ready'")
            cur.execute(
                "DELETE FROM version_events WHERE version_id NOT IN "
                "(SELECT version_id FROM versions)"
            )

    # ---------- 写 ----------
    @contextmanager
    def write_txn(self):
        """一个写事务：镜像变更与位置推进必须放在同一个事务里提交。"""
        with self._lock:
            cur = self._w.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    # ---------- 读 ----------
    def read(self, sql, params=()):
        rows = self.read_txn()
        try:
            return rows.execute(sql, params).fetchall()
        finally:
            rows.close()

    def read_txn(self):
        """返回一个已开启只读事务的连接：多次查询看到同一已提交快照。用毕 close()。"""
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("BEGIN")
        return conn

    # ---------- meta ----------
    def get_meta(self, key, default=None):
        rows = self.read("SELECT value FROM meta WHERE key=?", (key,))
        return rows[0][0] if rows else default

    def get_all_meta(self):
        return dict(self.read("SELECT key, value FROM meta"))

    @staticmethod
    def set_meta(cur, key, value):
        cur.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    # ---------- 事件日志 ----------
    def log(self, msg, cur=None):
        """cur 为空时自成一个小事务；否则并入调用方的事务（与数据同生共死）。"""
        if cur is None:
            with self.write_txn() as c:
                self.log(msg, c)
            return
        cur.execute("INSERT INTO event_log(ts, msg) VALUES(?, ?)", (utcnow(), msg))
        cur.execute(
            "DELETE FROM event_log WHERE id NOT IN "
            "(SELECT id FROM event_log ORDER BY id DESC LIMIT ?)",
            (LOG_KEEP,),
        )

    def recent_log(self, limit=20):
        return self.read(
            "SELECT ts, msg FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
        )

    # ---------- 目录版本 ----------
    def save_version(self, name):
        """把当前目录整体留存为命名版本，返回 (version_id, 行数, 续采位置)。

        就绪状态、续采位置、全部震例行在同一个写事务里读取并复制：
        - 与采集线程的写事务互斥 => 续采恰逢留存时，不会存下两笔源事务
          之间的混杂目录；复制的一定是某一已提交状态的完整目录；
        - 若重建已先把状态切离 done，本事务看到的就是未就绪，直接拒绝，
          不会存下未就绪的目录；
        - 事务整体提交或整体回滚 => 留存中失败或进程重启不会留下可选的
          半成品（启动时另有 _cleanup_incomplete_versions 兜底）。
        """
        with self.write_txn() as cur:
            row = cur.execute("SELECT value FROM meta WHERE key='load_state'").fetchone()
            state = row[0] if row else "idle"
            if state != "done":
                raise MirrorNotReady(state)
            row = cur.execute("SELECT value FROM meta WHERE key='last_applied_lsn'").fetchone()
            lsn = row[0] if row and row[0] else ""
            if cur.execute("SELECT 1 FROM versions WHERE name=?", (name,)).fetchone():
                raise VersionNameTaken(name)
            n = cur.execute("SELECT count(*) FROM events").fetchone()[0]
            cur.execute(
                "INSERT INTO versions(name, created_at, source_lsn, row_count, status)"
                " VALUES(?, ?, ?, ?, 'ready')",
                (name, utcnow(), lsn, n),
            )
            vid = cur.lastrowid
            cur.execute(
                "INSERT INTO version_events"
                "(version_id, event_id, origin_time, latitude, longitude, magnitude)"
                " SELECT ?, event_id, origin_time, latitude, longitude, magnitude FROM events",
                (vid,),
            )
            self.log(f"已留存目录版本「{name}」：{n} 行，续采位置 {lsn or '—'}", cur)
            return vid, n, lsn

    def list_versions(self):
        """已留存（且完整）的版本清单；半成品永远不会出现在这里。"""
        return self.read(
            "SELECT version_id, name, created_at, source_lsn, row_count"
            " FROM versions WHERE status='ready' ORDER BY version_id"
        )

    def delete_version(self, version_id):
        with self.write_txn() as cur:
            cur.execute("DELETE FROM version_events WHERE version_id=?", (version_id,))
            cur.execute("DELETE FROM versions WHERE version_id=?", (version_id,))
            gone = cur.rowcount > 0
            if gone:
                self.log(f"已删除目录版本 #{version_id}", cur)
            return gone
