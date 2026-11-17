# 日切放行

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 日切放行 |
| 目录 | `04-slicegate` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10628 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

迟到切片、重切、跨分区依赖、部分完整冻结、消费水位回拨

## 避开

别做成作业拓扑编排换皮；别做成纯水位大盘

## 拒绝副作用口径（钉死，乙写法，当日禁止再换）

- 拒绝只追加 `releases` 拒绝流水与原因；`slices.completeness` 与 `release_status` 保持拒绝前原值，**禁止拒绝后改成“待检”，拒绝路径禁止写 `watermarks` 推进水位**
- 乙写法快照列 `reject_completeness_snapshot` / `reject_release_status_snapshot` / `last_reject_reason` / `last_reject_at` 只供切片详情只读留证
- `rejection_ledger` 为拒绝副作用簿，事件分 `拒绝` / `拒绝争用` / `拒绝口径偏了`，与拒绝流水同库（SQLite `data/app.db`）逐笔可核
- `rejection_policy` 按日落锁当日口径；代码常量 `REJECTION_POLICY=乙` 与库内锁冲突时拒绝请求并记“拒绝口径偏了”，不沿用偏离口径
- 同一切片拒绝经进程内切片锁 + `BEGIN IMMEDIATE` 互斥串行；2 秒窗口内后手一律记“拒绝争用”并返回 409，不静默通过、不重复落拒绝流水
- 只读核查接口：`GET /api/rejection-ledger`、`GET /api/rejection-policy`、`GET /api/slices/{id}`

## 互斥切口候选

- 迟到切片整批拒收 vs 进待补齐冻结区
- 重切失败整笔回滚 vs 保留已完整分区
- 放行与水位同步闸 vs 异步放行后纠偏

## 启动（一键）

`powershell
cd d:\document\cccodex\workspace\slicegate\slicegate-bugfix-03
docker compose up -d --build
`

浏览器：http://127.0.0.1:10628/  
健康检查：GET http://127.0.0.1:10628/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10628
python -m pytest -q
`
