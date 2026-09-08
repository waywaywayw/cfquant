# QMT2 双账户 CFQuant 桥排障复盘

日期：2026-09-08

## 1. 结论

本次目标是在同一个中山证券 QMT2 客户端中，同时运行普通股票账户和信用账户的
CFQuant 桥，并保持两条交易链路隔离。最终实机验证通过：

| QMT 模型 | bridge_id | 职责 | 账户约束 |
| --- | --- | --- | --- |
| `CFQUANT` | `zs_qmt2` | 行情、普通回调 | 普通账户 |
| `CFQUANT_TRADE_LOWLAT` | `zs_qmt2` | 普通交易请求 | 请求显式携带普通账户 |
| `CFQUANT_CREDIT_TRADE` | `zs_qmt2_credit` | 信用查询、交易请求和回调 | 锁定信用账户、`CREDIT(3)` |

三行模型可以同时运行，不存在“QMT2 最多只能运行两个 Python 模型”的限制。本次
先后遇到的是两个独立问题：

1. Trade 模型在 `init()` 中调用永久 `run_forever()`，阻塞了 QMT 的共享策略调度
   线程；
2. 改成协作式轮询后，普通 Trade 入口又向旧版普通 `cfquant` 工厂传入了新版才有
   的 `account_locked`、`account_type` 参数，导致 `init()` 立即抛出 `TypeError`。

最终方案不是增加线程、进程或重试，而是让每个 Trade 模型通过 QMT
`ContextInfo.run_time` 定时执行一次非阻塞 `poll(max_messages=100, timeout=0)`；
普通入口只传旧工厂支持的通用参数，锁定信用入口仍强制传递账号锁定参数并保持
fail-closed。

## 2. 最终状态

2026-09-08 21:49 至 21:59 的实机验收得到：

- `cfquant.zs_qmt2.normal.request`：在线，`context_ready=true`、`tx_ready=true`；
- `cfquant.zs_qmt2.trade.request`：在线，普通账户只读查询正常；
- `cfquant.zs_qmt2_credit.trade.request`：在线，`account_locked=true`、
  `account_type=3`，回调账户与锁定账户一致；
- 普通账户：资金 1 条、持仓 5 条、历史委托 6 条、成交 5 条；6 条委托均为终态，
  其中 3 条已撤（54）、3 条已成（56），活动委托为 0；
- 信用账户：资金查询正常，持仓、委托、成交均为 0；
- CFQuant Web：HTTP 200；
- 模拟执行器：`ready=true`、`md_inited=true`、`trading=false`；
- 实盘和信用执行器未启动，本次没有发单或撤单。

一次信用 `cfquant.status` 在 4 秒超时内未返回，但同一通道随后使用 8 秒超时立即
成功，三条桥状态和身份均未变化。该现象按瞬时 LTtx 延迟处理，不作为掉线证据。

## 3. 故障演进

### 3.1 界面“运行中”不等于桥已上线

QMT 模型列表曾同时显示三行“运行中”，但 LTtx 日志只有行情桥和其中一个 Trade
桥的握手。停止模型后，旧 transport 连接也可能短暂残留。因此排障不能只看界面
颜色或 TCP 连接数，必须同时检查：

1. 对应 request channel 是否产生新握手；
2. `cfquant.status` 是否能在超时内返回；
3. `bridge_id`、账户身份、`context_ready`、`tx_ready` 和回调绑定是否一致；
4. 显式账户的资金、持仓、委托、成交查询是否只返回该账户的数据。

### 3.2 `importlib.reload` 竞争不是最终根因

普通行情入口和普通 Trade 入口曾在 QMT 冷启动时并发 reload 共享模块。这会增加
共享解释器状态的不确定性，因此入口已改为只 import、不 reload。该修改消除了一个
真实风险，但普通 Trade 仍不能与信用 Trade 同时工作，说明它不是完整根因。

经验：一个改动降低了风险，不代表已经解释所有观测；必须继续用独立通道状态验证。

### 3.3 永久循环阻塞 QMT 策略线程

旧 Trade 入口在 `init(ContextInfo)` 中执行：

```python
_trade_bridge.run_forever(sleep_seconds=0.001)
```

QMT 的 Python 策略共享调度线程。即使循环内部每次只 sleep 1 ms，`init()` 仍永远
不返回，其他模型的 `init()` 和回调就可能无法被调度。迅投文档也明确要求策略中
不要用阻塞循环，并提供 `run_time` 作为定时执行入口：

- [QMT 常见问题：策略运行与同线程约束](https://dict.thinktrader.net/innerApi/question_answer.html)
- [QMT Python 用户注意事项](https://dict.thinktrader.net/innerApi/user_attention.html)
- [QMT 系统函数：run_time](https://dict.thinktrader.net/innerApi/system_function.html)

修复后的核心路径是：

```python
def init(ContextInfo):
    _trade_bridge.set_context(ContextInfo)
    _trade_bridge.start()
    ContextInfo.run_time(
        "_cfquant_trade_lowlat_pump",
        "50nMilliSecond",
        "2019-01-01 00:00:00",
    )


def _cfquant_trade_lowlat_pump(ContextInfo):
    _trade_bridge.poll(max_messages=100, timeout=0)
```

普通与信用入口使用不同的回调函数名，轮询函数每次有明确消息上限且 timeout 为 0。
`init()` 能立即返回，因此三行模型可以共享 QMT 调度线程。

### 3.4 普通库与信用隔离库发生工厂签名漂移

协作式调度上线后，普通 Trade 模型截图给出了决定性错误：

```text
TypeError: start_tx_trade_bridge() got an unexpected keyword argument
'account_locked'
```

对 QMT2 普通库做只读签名检查，结果为：

```text
(context, ip='127.0.0.1', port=2049, token='LTtx',
 request_channel='cfquant.request', bridge_id='default',
 account_id='', show=True)
```

普通入口是未锁定模型，不需要向旧工厂传递信用专用参数。最终实现先构造通用参数，
仅在 `ACCOUNT_LOCKED` 为真时追加锁定参数：

```python
bridge_kwargs = {
    "ip": "127.0.0.1",
    "port": 2049,
    "token": "LTtx",
    "request_channel": BRIDGE_CHANNELS["trade"],
    "bridge_id": BRIDGE_ID,
    "account_id": DEFAULT_ACCOUNT_ID,
    "show": True,
}
if ACCOUNT_LOCKED:
    bridge_kwargs.update(
        account_locked=ACCOUNT_LOCKED,
        account_type=ACCOUNT_TYPE,
    )
return start_tx_trade_bridge(None, **bridge_kwargs)
```

这不是把信用安全门禁降级为兼容模式。信用模型使用隔离的
`cfquant_credit.cfquant` 新库，仍必须传递 `account_locked=True` 和
`account_type=CREDIT`；如果锁定入口遇到不支持这些参数的旧工厂，它应直接失败，
不能静默变成未锁定账户。

## 4. 决定结果的验证方法

### 4.1 检查 QMT 内实际加载库的工厂签名

在 Windows QMT2 主机上使用安装环境执行：

```python
import inspect
import sys

sys.path.insert(0, r"C:\QMT_ZS_2\python")
from cfquant.cfquant.tx_trade_bridge import start_tx_trade_bridge

print(inspect.signature(start_tx_trade_bridge))
print(inspect.getsourcefile(start_tx_trade_bridge))
```

这个检查比根据本地 Git 工作树推断生产签名可靠。此次本地源码已经支持锁定参数，
但 QMT2 普通库仍是旧签名；信用模型又使用另一套隔离库，三者不能混为同一版本。

### 4.2 分通道探测状态

可从 Windows 主机使用普通 CFQuant 客户端探测三个 request channel：

```python
from cfquant.cfquant.client import LTtxRpcClient

channels = [
    "cfquant.zs_qmt2.normal.request",
    "cfquant.zs_qmt2.trade.request",
    "cfquant.zs_qmt2_credit.trade.request",
]

for channel in channels:
    client = LTtxRpcClient(
        host="127.0.0.1",
        port=2049,
        token="LTtx",
        request_channel=channel,
        timeout=8,
    )
    try:
        print(channel, client.request("cfquant.status", timeout=8))
    finally:
        client.close()
```

输出中至少检查：`bridge_id`、`account_id`、`account_type`、
`account_locked`、`context_ready`、`tx_ready`、`callback_account_id` 和
`callback_source`。普通 Trade 桥允许 status 中的默认账户为空，但业务查询必须显式
携带普通账户；信用 Trade 桥的身份字段则必须全部锁定且一致。

### 4.3 账户数据对账

对两个 Trade channel 分别调用以下只读 action：

```text
xttrader.query_stock_asset
xttrader.query_stock_positions
xttrader.query_stock_orders
xttrader.query_stock_trades
```

每次都显式传入 `account_id` 和 `account_type`，并核对返回记录的账户 ID。不要把旧桥
对 `cancelable_only=True` 返回的列表直接解释为活动委托；此次旧桥忽略了该筛选，
最终是读取原始委托状态码后确认所有记录均为终态。

### 4.4 本地回归

最终入口兼容修复使用的命令为：

```bash
cd /data/user/weijiawei/projects/cfquant
/data/user/weijiawei/projects/WQuant_v1_runtime/envs/wquant/bin/python \
  -m pytest -q tests/test_normal_bridge.py
```

结果：`30 passed`。其中新增回归同时证明：

- 未锁定普通入口可以调用只到 `show=True` 的旧工厂；
- 锁定信用入口仍传递 `account_locked` 和 `account_type`；
- 两个 Trade 模型都使用非阻塞协作式 pump；
- 入口符合 Python 3.6 语法。

相关改动汇总后又执行了仓库全量测试，结果为 `126 passed`。

## 5. 两个账户能否都使用新版 CFQuant

可以，但需要区分“同一套协议”与“同一个物理 Python 包”。

当前两个账户都已经通过 QMT2 + CFQuant/LTtx + XtQuantTrader 兼容层完成只读实机验证：

- 普通账户使用 `zs_qmt2`；
- 信用账户使用 `zs_qmt2_credit`，并且必须锁定账号和 `CREDIT(3)`；
- 两个账户不能共用同一个 Trade request channel，也不能把信用账户接到未锁定普通
  Trade 模型上。

当前信用模型仍使用隔离包 `cfquant_credit`，普通模型使用 `cfquant`。这是为了避免
覆盖共享库或 reload 影响已经运行的普通链路。因此，业务上两个账户都能使用新版
CFQuant 接口；部署上暂时不是单目录、单包版本。

如果以后要统一为一个物理 `cfquant` 包，应先把支持锁定参数的新版库部署到 QMT2，
再做一次完整冷启动验收，确认三行模型、两个账户、回调隔离和模拟执行器全部通过；
在此之前不要删除 `cfquant_credit` 隔离目录。

另外，桥可用不等于生产自动交易已经启用。当前实盘普通执行器和信用执行器均未
启动。要让两个账户都进入 WQuant 自动化执行，还需要分别配置执行器 profile、
保持不同 bridge ID，并按 PAPER/只读、受控首单、正式启用的顺序完成验证。

## 6. 排障与部署准则

1. 不以 QMT UI 的“运行中”作为成功标准，必须做 channel status 和账户对账。
2. QMT 策略入口不得在 `init()`、`handlebar()` 或定时回调中永久循环、阻塞等待。
3. QMT 入口不对共享桥模块做 `importlib.reload`。
4. 普通与信用 Trade 使用不同 bridge ID、request channel 和回调身份。
5. 信用桥必须 fail-closed：空账户、默认 bridge、错误账号类型或回调身份冲突均拒绝。
6. 更新入口脚本前先检查 QMT 内实际库签名，不能只看开发机源码。
7. 停止 QMT GUI 后仍要核对 `XtItClient.exe` 是否真正退出；残留进程会保留旧模块。
8. 只读验证不发单、不撤单；真实首单需要单独授权和业务门禁。

## 7. 原始施工与验收记录

- 协作式 pump：
  `/home/weijiawei/.local/state/codex-agent/tasks/qmt-coop-pump-20260908-9xjuus/`
- v05 旧工厂兼容：
  `/home/weijiawei/.local/state/codex-agent/tasks/qmt-unlocked-factory-compat-20260908-tzs3r9/`
- v05 最终部署副本：
  `/home/weijiawei/.local/state/codex-agent/tasks/qmt-unlocked-factory-compat-final-20260908-RRyy9g/`
- Windows 桌面入口：
  `C:\Users\Administrator\Desktop\CFQUANT_TRADE_LOWLAT_zs_qmt2_v05_unlocked_compat.py`

这些路径记录了实际施工和验收边界；仓库中的长期真源仍是
`qmt_scripts/CFQUANT.py`、`qmt_scripts/CFQUANT_TRADE_LOWLAT.py`、信用隔离包和对应测试。
