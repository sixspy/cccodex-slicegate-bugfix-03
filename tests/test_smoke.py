
import threading

import pytest
from fastapi.testclient import TestClient

from app import app, init_db, REJECTION_POLICY


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _make_partial(h, comp="部分完整"):
    payload = {"slice_no": "SL-T-P", "biz_date": "2026-11-04", "partition_key": "mid",
               "row_count": 5, "expected_count": 10}
    if comp == "待检":
        payload["slice_no"] = "SL-T-Z"
        payload["row_count"] = 0
    r = TestClient(app).post("/api/slices", headers=h, json=payload)
    assert r.status_code == 200
    return r.json()["id"]


def test_reject_appends_flow_but_keeps_completeness_snapshot():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h)

    before = c.get(f"/api/slices/{sid}", headers=h).json()
    assert before["completeness"] == "部分完整"
    assert before["release_status"] == "未放行"

    r = c.post(f"/api/slices/{sid}/release", headers=h,
               json={"consumer": "ads_x", "reason": "缺口 5 行"})
    assert r.status_code == 409
    assert "未完整" in r.json()["detail"]

    after = c.get(f"/api/slices/{sid}", headers=h).json()
    # 核心口径：拒绝不得动完整度主字段，也不得动放行态
    assert after["completeness"] == "部分完整"
    assert after["release_status"] == "未放行"
    # 乙写法快照列留证，且与拒绝前一致
    assert after["reject_completeness_snapshot"] == "部分完整"
    assert after["reject_release_status_snapshot"] == "未放行"
    assert after["last_reject_reason"] == "缺口 5 行"
    assert after["last_reject_at"]

    # 拒绝流水只追加一条拒绝
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"]
            if x["slice_id"] == sid]
    assert len(rels) == 1 and rels[0]["status"] == "拒绝" and rels[0]["note"] == "缺口 5 行"

    # 拒绝副作用簿同库可核，快照与主字段一致
    ledger = [x for x in c.get("/api/rejection-ledger", headers=h).json()["items"]
              if x["slice_id"] == sid]
    assert len(ledger) == 1
    ev = ledger[0]
    assert ev["event_type"] == "拒绝"
    assert ev["policy"] == REJECTION_POLICY == "乙"
    assert ev["completeness_snapshot"] == "部分完整"
    assert ev["release_status_snapshot"] == "未放行"
    assert ev["operator"] == "admin"


def test_reject_never_resets_completeness_to_pending():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h, comp="部分完整")
    c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    row = c.get(f"/api/slices/{sid}", headers=h).json()
    assert row["completeness"] != "待检"
    assert row["completeness"] == "部分完整"

    # 待检切片被拒绝后仍是待检（自身快照），不会被额外改写
    sid0 = _make_partial(h, comp="待检")
    r = c.post(f"/api/slices/{sid0}/release", headers=h, json={"consumer": "ads_x"})
    assert r.status_code == 409
    row0 = c.get(f"/api/slices/{sid0}", headers=h).json()
    assert row0["completeness"] == "待检"
    assert row0["reject_completeness_snapshot"] == "待检"


def test_reject_does_not_advance_watermark():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h)
    wm_before = len(c.get("/api/watermarks", headers=h).json()["items"])
    r = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert r.status_code == 409
    wm_after = c.get("/api/watermarks", headers=h).json()["items"]
    assert len(wm_after) == wm_before
    assert all(w["source_slice"] != "SL-T-P" for w in wm_after)


def test_concurrent_reject_is_serialized_with_contention_audit():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h)
    n = 4
    barrier = threading.Barrier(n)
    results = [None] * n

    def worker(i):
        cli = TestClient(app)
        barrier.wait()
        results[i] = cli.post(f"/api/slices/{sid}/release",
                              headers=h, json={"consumer": "ads_x"}).status_code

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 所有请求都被拒绝（409），无静默成功
    assert results == [409] * n

    rels = [x for x in c.get("/api/releases", headers=h).json()["items"]
            if x["slice_id"] == sid and x["status"] == "拒绝"]
    ledger_items = c.get("/api/rejection-ledger", headers=h).json()["items"]
    rejects = [x for x in ledger_items if x["slice_id"] == sid and x["event_type"] == "拒绝"]
    contentions = [x for x in ledger_items if x["slice_id"] == sid and x["event_type"] == "拒绝争用"]

    # 串行化：只有先手写一条拒绝流水，其余后手全部记争用，不允许静默通过
    assert len(rels) == 1
    assert len(rejects) == 1
    assert len(contentions) == n - 1
    assert all(x["reason"].startswith("拒绝争用") for x in contentions)

    row = c.get(f"/api/slices/{sid}", headers=h).json()
    assert row["completeness"] == "部分完整"
    assert row["release_status"] == "未放行"


def test_daily_policy_locks_and_drift_is_rejected_and_audited():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h)

    # 首次拒绝落当日口径锁
    assert c.post(f"/api/slices/{sid}/release", headers=h,
                  json={"consumer": "ads_x"}).status_code == 409
    pol = c.get("/api/rejection-policy", headers=h).json()
    assert pol["code_policy"] == "乙"
    assert pol["locked"]["policy"] == "乙"
    assert pol["drift"] is False

    # 模拟库内口径被外部改成甲（偏离代码钉死的乙写法）
    from app import connect
    today = pol["today"]
    raw = connect()
    try:
        raw.execute("UPDATE rejection_policy SET policy=? WHERE biz_date=?", ("甲", today))
        raw.commit()
    finally:
        raw.close()

    sid2 = _make_partial(h, comp="待检")
    r = c.post(f"/api/slices/{sid2}/release", headers=h, json={"consumer": "ads_x"})
    assert r.status_code == 409
    assert "拒绝口径偏了" in r.json()["detail"]

    pol2 = c.get("/api/rejection-policy", headers=h).json()
    assert pol2["drift"] is True

    ledger = c.get("/api/rejection-ledger", headers=h).json()["items"]
    drifts = [x for x in ledger if x["event_type"] == "拒绝口径偏了"]
    assert len(drifts) == 1 and drifts[0]["policy"] == "甲"
    # 偏离时不得追加拒绝流水、不得改切片
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"]
            if x["slice_id"] == sid2]
    assert rels == []
    row = c.get(f"/api/slices/{sid2}", headers=h).json()
    assert row["completeness"] == "待检" and row["last_reject_reason"] is None


def test_complete_slice_releases_and_advances_watermark():
    c = TestClient(app)
    h = _login(c)
    ok = c.post("/api/slices", headers=h,
                json={"slice_no": "SL-T-1", "biz_date": "2026-11-04", "partition_key": "mid",
                      "row_count": 10, "expected_count": 10})
    sid = ok.json()["id"]
    assert c.post(f"/api/slices/{sid}/release", headers=h,
                  json={"consumer": "ads_x"}).status_code == 200
    row = c.get(f"/api/slices/{sid}", headers=h).json()
    assert row["release_status"] == "已放行"
    assert any(w["source_slice"] == "SL-T-1"
               for w in c.get("/api/watermarks", headers=h).json()["items"])
    # 放行成功不产生拒绝副作用
    assert c.get("/api/rejection-ledger", headers=h).json()["items"] == []


def test_reject_flow_and_ledger_are_mutually_verifiable():
    c = TestClient(app)
    h = _login(c)
    sid = _make_partial(h)
    c.post(f"/api/slices/{sid}/release", headers=h,
           json={"consumer": "ads_x", "reason": "对账缺口"})
    rels = [x for x in c.get("/api/releases", headers=h).json()["items"]
            if x["slice_id"] == sid and x["status"] == "拒绝"]
    ledger = [x for x in c.get("/api/rejection-ledger", headers=h).json()["items"]
              if x["slice_id"] == sid and x["event_type"] == "拒绝"]
    # 互斥账本：一条拒绝流水必对应一条拒绝副作用，时间/原因可核
    assert len(rels) == len(ledger) == 1
    assert rels[0]["created_at"] == ledger[0]["created_at"]
    assert rels[0]["note"] == ledger[0]["reason"] == "对账缺口"
