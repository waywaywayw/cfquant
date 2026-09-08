# CFQUANT 信用 Trade 模型离线配置说明

## 架构

信用账户只新增一个 QMT Trade 模型：`CFQUANT_CREDIT_TRADE`。它使用专属
`bridge_id=zs_qmt2_credit` 和 Trade request channel，承接信用查询、下单/撤单
入口以及回调。回调通过已经修好的
`NormalQmtBridge.for_callback_publisher(trade_bridge, ...)` 复用 Trade 的
transport；publisher 不启动第二个 tx、不启动 Normal 定时器/线程，也不拥有
transport 生命周期。现有普通模型 `zs_qmt2`、账号 `28100046850` 不改绑、不重启，
不新增信用 Normal 模型。

信用 QMT 模型源码中的配置必须明确写成：

```python
USER_BRIDGE_ID = "zs_qmt2_credit"
DEFAULT_ACCOUNT_ID = "28160000447"
ACCOUNT_TYPE = "CREDIT"
ACCOUNT_LOCKED = True
```

离线部署包的入口是 artifact 中的 `payload/python/CFQUANT_CREDIT_TRADE.py`，其
三处桥导入使用独立前缀 `cfquant_credit.cfquant`。部署时同时新增
`C:/QMT_ZS_2/python/cfquant_credit/`，只把该目录作为信用模型的 Python 库目录；
不要把信用副本复制到既有 `python/cfquant`，也不要通过覆盖共享库后 reload 来改变
普通模型已经缓存的类。通信仍复用 LTtx `127.0.0.1:2049`，publisher 复用信用
Trade transport，不增加 Normal 模型、第二个 transport 或后台服务。

锁定模式忽略进程级 `CFQUANT_BRIDGE_ID`，并在创建 Trade transport 前拒绝空账号、
默认 bridge 或非法账号类型。锁定 Trade 的所有账号相关请求必须声明同一账号和
类型；订阅只有在 QMT `set_account` 成功后才更新本地及共享路由。`unsubscribe`
不会清除锁定身份。

## CREDIT 连接门禁

trader gateway 在 CREDIT 的 `xt_client.connect()` 之前调用
`xt_client._trade_request("cfquant.status")`。必须同时满足：

- `bridge_id == "zs_qmt2_credit"`；
- `account_locked == true`、`account_id == "28160000447"`、`account_type` 为
  `CREDIT`；
- `context_ready == true`、`tx_ready == true`；
- `callback_account_bound == true`、`callback_account_id == "28160000447"`；
- `callback_source == "trade_model"`。

任一条件不满足都会在账号订阅前失败，因此不会继续调用 `connect()` 的自动
`subscribe`。STOCK 连接仍走原来的路径，不依赖信用 Normal 状态。

## 只读观察

离线读取状态时，可请求 Trade 的 `cfquant.status`，只检查上述状态字段；也可用
已有资产、持仓、委托、成交查询入口做 mock/本地回归。`trading_enabled` 继续保持
`false`，本包不做真实下单、撤单或账号订阅探测；真实订单必须由用户另行确认后
再执行。

QMT 现有模型是 GUI 保存的编码格式，库文件可以复制。部署信用入口时应新建一个
Python 模型，把入口源码粘贴后保存，不能明文覆盖普通模型，也不能修改运行中的
模型索引，也不重启 QMT；本包不包含覆盖共享普通 QMT 文件的步骤。

## 验收边界

Trade 模型不能在 `init` 中调用长期 `run_forever()`：QMT 的多个 Python 模型共享
同一策略调度线程，阻塞其中一个模型会导致后续模型不再完成初始化。当前入口在
`init` 中启动桥、绑定回调并注册 50 ms 的 `ContextInfo.run_time` 定时器，然后立即
返回；定时回调每次只执行一次非阻塞 `poll(max_messages=100, timeout=0)`。

2026-09-08 已在 QMT2 中同时启动普通 Normal、普通 Trade 和信用 Trade 三个模型。
三个 request channel 均独立响应，普通账户和信用账户的资金、持仓、委托、成交只读
查询均返回各自账号数据，信用状态保持 `account_locked=true`、`account_type=3`，
且两个账户均无活动委托。本次验收未做下单或撤单；真实交易仍需在对应执行器独立
启用后按生产门禁验证。完整过程见
[QMT2 双账户 CFQuant 桥排障复盘](qmt2_dual_account_bridge_postmortem.md)。
