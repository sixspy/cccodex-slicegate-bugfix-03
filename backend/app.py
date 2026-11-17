
import hashlib, secrets, sqlite3, threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# —— 拒绝副作用口径（代码级钉死，当日禁止再换）——
# 甲写法(POLICY_JIA)：拒绝只追加拒绝流水与原因，slices 一行不动，完整度/未放行态即拒绝前快照
# 乙写法(POLICY_YI) ：拒绝时在 slices 写拒绝快照列与最近拒绝原因（详情只读），完整度字段本身保留不动
# 本仓钉死乙写法：任何路径都不得把完整度改成“待检”，拒绝路径不得推进消费水位
POLICY_JIA = "甲"
POLICY_YI = "乙"
REJECTION_POLICY = POLICY_YI
# 同一切片两次拒绝到达时间差小于该窗口，后手判定为并发争用，只记争用审计、不重复落拒绝流水
REJECT_CONTENTION_WINDOW_SEC = 2.0
EVENT_REJECT = "拒绝"
EVENT_CONTENTION = "拒绝争用"
EVENT_POLICY_DRIFT = "拒绝口径偏了"

_slice_locks: dict[int, threading.Lock] = {}
_locks_guard = threading.Lock()

def slice_lock(sid: int) -> threading.Lock:
    with _locks_guard:
        lk = _slice_locks.get(sid)
        if lk is None:
            lk = threading.Lock()
            _slice_locks[sid] = lk
        return lk

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

@contextmanager
def immediate_tx(conn):
    # 手动事务：BEGIN IMMEDIATE 立即拿库级写锁，配合切片锁保证裁决串行
    conn.isolation_level = None
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except HTTPException:
        # 409 拒绝/争用/口径偏离都是已完成的业务裁决，其副作用必须落库而非回滚
        conn.commit()
        raise
    except Exception:
        conn.rollback()
        raise

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from typing import Any
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        -- 拒绝副作用簿：拒绝/争用/口径偏离的唯一审计出口，与拒绝流水同库可核
        CREATE TABLE IF NOT EXISTS rejection_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          biz_date TEXT NOT NULL,
          slice_id INTEGER NOT NULL,
          slice_no TEXT NOT NULL,
          consumer TEXT NOT NULL,
          event_type TEXT NOT NULL,
          policy TEXT NOT NULL,
          reason TEXT NOT NULL,
          completeness_snapshot TEXT NOT NULL,
          release_status_snapshot TEXT NOT NULL,
          operator TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        -- 当日拒绝口径锁：每日首次拒绝落锁后，当日禁止再换
        CREATE TABLE IF NOT EXISTS rejection_policy(
          biz_date TEXT PRIMARY KEY,
          policy TEXT NOT NULL,
          locked_by TEXT NOT NULL,
          locked_at TEXT NOT NULL
        );
        """)
        # 乙写法所需的拒绝快照列（仅写这些列，completeness/release_status 保持拒绝前原值）
        for col, ddl in (
            ("reject_completeness_snapshot", "ALTER TABLE slices ADD COLUMN reject_completeness_snapshot TEXT"),
            ("reject_release_status_snapshot", "ALTER TABLE slices ADD COLUMN reject_release_status_snapshot TEXT"),
            ("last_reject_reason", "ALTER TABLE slices ADD COLUMN last_reject_reason TEXT"),
            ("last_reject_at", "ALTER TABLE slices ADD COLUMN last_reject_at TEXT"),
        ):
            exists = conn.execute(
                "SELECT 1 FROM pragma_table_info('slices') WHERE name=?", (col,)
            ).fetchone()
            if not exists:
                conn.execute(ddl)
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')")

@app.on_event("startup")
def _start():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}

class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int

@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = "完整" if body.row_count >= body.expected_count and body.expected_count > 0 else ("部分完整" if body.row_count > 0 else "待检")
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")

@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = "完整" if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0 else ("部分完整" if row["row_count"] > 0 else "待检")
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}

class ReleaseIn(BaseModel):
    consumer: str
    reason: str | None = None

def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def _today() -> str:
    return datetime.now().date().isoformat()

def _append_ledger(c, *, biz_date, sid, slice_no, consumer, event_type, policy,
                   reason, comp_snapshot, status_snapshot, operator, now_str):
    c.execute("""INSERT INTO rejection_ledger(
        biz_date,slice_id,slice_no,consumer,event_type,policy,reason,
        completeness_snapshot,release_status_snapshot,operator,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (biz_date, sid, slice_no, consumer, event_type, policy, reason,
         comp_snapshot, status_snapshot, operator, now_str))

def _lock_daily_policy(c, today: str, operator: str) -> str:
    """钉死当日拒绝口径：首次拒绝落锁；已锁则必须与代码常量一致，否则记偏离。"""
    row = c.execute("SELECT policy FROM rejection_policy WHERE biz_date=?", (today,)).fetchone()
    if row is None:
        c.execute("INSERT INTO rejection_policy(biz_date,policy,locked_by,locked_at) VALUES (?,?,?,?)",
                  (today, REJECTION_POLICY, operator, _now()))
        return REJECTION_POLICY
    if row["policy"] != REJECTION_POLICY:
        return ""  # 口径偏了，由调用方记副作用簿并拒绝
    return row["policy"]

@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    consumer = (body.consumer or "").strip()
    if not consumer:
        raise HTTPException(422, "下游消费方不能为空")
    reason = (body.reason or "完整度未达标").strip() or "完整度未达标"
    today = _today()

    # 同一切片的裁决互斥串行；不同切片仍可并行
    now_dt = datetime.now()
    now_str = now_dt.isoformat(timespec="seconds")
    with slice_lock(sid):
        conn = connect()
        try:
            with immediate_tx(conn) as c:
                row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
                if not row:
                    raise HTTPException(404, "切片不存在")
                slice_no, biz_date = row["slice_no"], row["biz_date"]
                comp_before, status_before = row["completeness"], row["release_status"]

                if row["completeness"] != "完整":
                    # —— 拒绝路径（乙写法）——
                    policy = _lock_daily_policy(c, today, operator)
                    if policy == "":
                        locked = c.execute(
                            "SELECT policy FROM rejection_policy WHERE biz_date=?", (today,)
                        ).fetchone()["policy"]
                        # 口径偏离：拒绝本次操作并留痕，绝不沿用偏离口径写副作用
                        _append_ledger(c, biz_date=biz_date, sid=sid, slice_no=slice_no,
                                       consumer=consumer, event_type=EVENT_POLICY_DRIFT,
                                       policy=locked, reason=f"拒绝口径偏了：库内={locked} 代码={REJECTION_POLICY}",
                                       comp_snapshot=comp_before, status_snapshot=status_before,
                                       operator=operator, now_str=now_str)
                        raise HTTPException(409, "拒绝口径偏了，当日口径已锁且与现行写法冲突，本次拒绝已拦截")

                    # 并发争用判定：后手与该切片最近一次拒绝/争用落在同一窗口内
                    last = c.execute("""SELECT created_at FROM rejection_ledger
                                        WHERE slice_id=? AND event_type IN (?,?)
                                        ORDER BY id DESC LIMIT 1""",
                                     (sid, EVENT_REJECT, EVENT_CONTENTION)).fetchone()
                    if last is not None:
                        try:
                            gap = (now_dt - datetime.fromisoformat(last["created_at"])).total_seconds()
                        except ValueError:
                            gap = 999999.0
                        if 0 <= gap < REJECT_CONTENTION_WINDOW_SEC:
                            _append_ledger(c, biz_date=biz_date, sid=sid, slice_no=slice_no,
                                           consumer=consumer, event_type=EVENT_CONTENTION,
                                           policy=policy,
                                           reason=f"拒绝争用：后手 {operator} 与最近拒绝间隔 {gap:.1f}s",
                                           comp_snapshot=comp_before, status_snapshot=status_before,
                                           operator=operator, now_str=now_str)
                            raise HTTPException(409, "拒绝争用：该切片刚被拒绝，后手操作已串行拦截，请刷新后重试")

                    # 1) 只追加拒绝流水与原因，完整度/放行态保持拒绝前快照，不推进水位
                    c.execute("""INSERT INTO releases(slice_id,consumer,status,note,created_at)
                                 VALUES (?,?,?,?,?)""",
                              (sid, consumer, "拒绝", reason, now_str))
                    # 2) 拒绝副作用簿（乙写法，含拒绝前快照）
                    _append_ledger(c, biz_date=biz_date, sid=sid, slice_no=slice_no,
                                   consumer=consumer, event_type=EVENT_REJECT,
                                   policy=policy, reason=reason,
                                   comp_snapshot=comp_before, status_snapshot=status_before,
                                   operator=operator, now_str=now_str)
                    # 3) 乙写法快照列：只写快照/原因列，严禁触碰 completeness 与 release_status
                    c.execute("""UPDATE slices SET reject_completeness_snapshot=?,
                                 reject_release_status_snapshot=?, last_reject_reason=?,
                                 last_reject_at=? WHERE id=? AND completeness=? AND release_status=?""",
                              (comp_before, status_before, reason, now_str, sid,
                               comp_before, status_before))
                    if c.execute("SELECT changes()").fetchone()[0] == 0:
                        # 快照守卫未命中说明行被并发改写，拒绝静默通过
                        raise HTTPException(409, "拒绝争用：切片状态已被并发改写，本次拒绝未入账")
                    raise HTTPException(409, "未完整禁止放行")

                # —— 放行路径：仅完整切片可入，不经过拒绝副作用簿 ——
                c.execute("UPDATE slices SET release_status='已放行' WHERE id=? AND completeness='完整'",
                          (sid,))
                c.execute("""INSERT INTO releases(slice_id,consumer,status,note,created_at)
                             VALUES (?,?,?,?,?)""",
                          (sid, consumer, "成功", "完整切片放行", now_str))
                c.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice)
                             VALUES (?,?,?,?)""",
                          (consumer, biz_date, now_str, slice_no))
                return {"ok": True, "slice_no": slice_no, "consumer": consumer}
        finally:
            conn.close()

@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/api/rejection-ledger")
def list_rejection_ledger(authorization: str | None = Header(default=None)):
    """拒绝副作用簿：拒绝/争用/口径偏离统一出口，与拒绝流水同库可核。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM rejection_ledger ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows], "locked_policy": REJECTION_POLICY}

@app.get("/api/rejection-policy")
def get_rejection_policy(authorization: str | None = Header(default=None)):
    """当日拒绝口径锁（只读）。"""
    require_user(authorization)
    today = _today()
    with connect() as c:
        row = c.execute("SELECT * FROM rejection_policy WHERE biz_date=?", (today,)).fetchone()
    return {
        "code_policy": REJECTION_POLICY,
        "today": today,
        "locked": dict(row) if row else None,
        "drift": bool(row and row["policy"] != REJECTION_POLICY),
    }

@app.get("/api/slices/{sid}")
def slice_detail(sid: int, authorization: str | None = Header(default=None)):
    """切片详情：完整度以主字段为准，拒绝快照与最近拒绝原因只读展示。"""
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        rels = c.execute(
            "SELECT id,consumer,status,note,created_at FROM releases WHERE slice_id=? ORDER BY id DESC",
            (sid,)).fetchall()
        ledger = c.execute(
            "SELECT id,event_type,policy,reason,completeness_snapshot,release_status_snapshot,operator,created_at "
            "FROM rejection_ledger WHERE slice_id=? ORDER BY id DESC", (sid,)).fetchall()
    item = dict(row)
    item["releases"] = [dict(r) for r in rels]
    item["rejection_ledger"] = [dict(r) for r in ledger]
    item["completeness_readonly"] = row["completeness"]
    return item

@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")

app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
