# xtquant.xttrader 平替追踪

更新时间：2026-09-08

## 总体结论

- 原版 `xtquant.xttrader.XtQuantTrader` 当前检测到 75 个公开方法。
- `cfquant.cfquant.xttrader.XtQuantTrader` 已补齐这 75 个同名方法，签名已对齐。
- 原版 `XtQuantTraderCallback` 当前检测到 14 个公开回调方法，`cfquant` 已全部补齐。
- `cfquant` 额外保留了 `disconnect()`，方便外部程序主动关闭本地桥接连接。
- 股票交易、撤单、资产、委托、成交、持仓和交易回调已走现有 QMT 桥接实装。
- 银行、信用、期权/期货划转、SMT、数据导出等接口已提供兼容入口，但实际可用性依赖 QMT 策略上下文中是否存在对应 callable；缺失时会返回明确的未实现错误。

## 信用操作码与委托回查

`cfquant.xtconstant` 使用 Windows SDK 的原生数值：`CREDIT_BUY=23`、`CREDIT_SELL=24`、`CREDIT_FIN_BUY=27`、`CREDIT_SLO_SELL=28`。其中 23/24 只在账号类型明确为 `CREDIT` 时表达信用账户的担保品买卖；不能仅凭数值把它们解释成融资/融券。

`TxTradeBridge.order_stock` 在调用 QMT `passorder` 前按账号类型处理业务码：

| 请求账号类型 | 请求业务码 | 实际 `passorder` 业务码 | 语义 |
| --- | --- | --- | --- |
| `STOCK` 或未声明 | 23/24 | 23/24 | 普通股票买/卖 |
| 明确 `CREDIT` | 23/24 | 33/34 | 担保品买/卖 |
| 明确 `CREDIT` | 27/28/33/34 | 原码 | 融资买/融券卖/担保品买/担保品卖 |

27–34（包括还券/还款 29–32）没有明确 `CREDIT` 账号时会在 `passorder` 前拒绝；信用委托必须携带非空账号 ID，不会回退到桥的普通默认账号。缺失业务码或显式 `None` 也会在 `passorder` 前拒绝。`buy`/`sell` 和严格整数文本可作为业务码输入，布尔值、小数、空字符串和未知字符串会被拒绝。`passorder` 不返回可用订单号时，桥会按原 `order_remark`，并在回查记录提供时匹配 `strategy_name`，再从信用账户的 `credit/order` 明细中选择正数或非空稳定券商 ID；0、负数和非有限数不会被当作订单号。

信用查询入口要求显式的 `StockAccount(account_id, "CREDIT")`（或账号类型 3）。标准 `query_credit_detail` 传给 QMT 的原生参数保持为 `(account_id, "credit", "account")`；直接兼容 `get_trade_detail_data` 时统一规范为 `(account_id, "CREDIT", "ACCOUNT")`，合约类查询统一使用 `(account_id, "CREDIT")`。返回值保留可读取的原生 `m_*` 字段并增加稳定别名；可选字段不可读时跳过，但身份字段不可读、账号冲突或整条记录无有效字段会失败。

gateway 的 `OrderData.extra` / `TradeData.extra` 在能由原生业务码确定时增加 `credit_operation`（`collateral_buy`、`collateral_sell`、`financing_buy`、`short_sell`）和 `broker_operation_code`，并保留已有的 `broker_trade_amount`。`m_nOffsetFlag` 是另一套枚举，只用于既有方向解析，不会单独制造信用业务标记；普通方向码 48/49 也不会被臆造为信用动作。

通用 vn.py `send_order` 路径在信用账号上只选择 SDK 的 `CREDIT_BUY/CREDIT_SELL`（23/24），因此当前表示担保品买卖，不默认表示融资买入或融券卖出。融资/融券需要从 CFQuant SDK 层显式传入 27/28；本包没有把它扩展成策略级融资配置或完整融资业务链路。

## 状态定义

| 状态 | 含义 |
| --- | --- |
| 已平替 | 已通过当前桥接能力完成主要调用链，返回值会做必要的对象化或字段适配。 |
| 部分平替 | 入口、参数和桥接调用已完成，但返回结构或底层 QMT 行为仍需实盘/仿真验证。 |
| 兼容入口 | 已补齐同名方法并转发到 QMT 侧同名或候选 callable，是否可用取决于本机 QMT 环境。 |
| 未完全平替 | 暂未做到原生接口的完整语义，或仍依赖后续补充 QMT 侧能力。 |

## 已平替接口

| 接口 | 当前实现 |
| --- | --- |
| `start` / `stop` / `connect` / `run_forever` | 管理 LTtx RPC 客户端生命周期，并注册交易回调事件。 |
| `register_callback` | 注册回调对象，并把 QMT 桥接事件映射到 `XtQuantTraderCallback`。 |
| `XtQuantTraderCallback` | 已补齐原版 14 个公开回调方法；股票、下单、撤单、银行划转、CTP 内转和 SMT async 兼容入口会触发对应回调。 |
| `subscribe` / `unsubscribe` | 订阅或取消账号级交易回调转发。 |
| `order_stock` | 通过 QMT `passorder` 下单，返回 `order_id`。 |
| `order_stock_async` | 通过 QMT `passorder` 下单，并向客户端推送异步下单响应事件。 |
| `cancel_order_stock` | 通过 QMT `cancel` 撤单，返回 `cancel_result`。 |
| `cancel_order_stock_async` | 执行撤单，并向客户端推送异步撤单响应事件。 |
| `query_stock_asset` | 读取 QMT `account` 交易明细并转为 `XtAsset`。 |
| `query_stock_orders` / `query_stock_order` | 读取 QMT `order` 交易明细并转为 `XtOrder`，单笔查询在列表中匹配。 |
| `query_stock_trades` | 读取 QMT `deal` 交易明细并转为 `XtTrade`。 |
| `query_stock_positions` / `query_stock_position` | 读取 QMT `position` 交易明细并转为 `XtPosition`，单持仓查询在列表中匹配。 |
| `query_stock_asset_async` / `query_stock_orders_async` / `query_stock_trades_async` / `query_stock_positions_async` | 复用同步查询并调用传入 callback，返回本地 seq。 |
| `set_timeout` | 设置兼容对象超时时间，并同步到当前 RPC 客户端。 |
| `set_relaxed_response_order_enabled` | 保存兼容标志，当前桥接无需额外排序处理。 |
| `sleep` | 本地 sleep 兼容。 |
| `common_op_sync_with_seq` / `common_op_async_with_seq` | 提供本地 callable 调用兼容。 |

## 部分平替接口

| 接口 | 当前实现 | 待验证点 |
| --- | --- | --- |
| `cancel_order_stock_sysid` / `cancel_order_stock_sysid_async` | 已补齐入口，并把 `sysid` 作为撤单编号转到当前 QMT `cancel` 调用。 | 不同 QMT 版本对系统编号撤单的入参可能不同，需要用真实系统编号验证。 |
| `query_com_fund` | 映射到 QMT `account` 明细，返回第一条资金记录。 | 字段命名和原生 `query_com_fund` 可能不完全一致。 |
| `query_com_position` | 映射到 QMT `position` 明细。 | 字段命名和原生 `query_com_position` 可能不完全一致。 |

## 已补齐兼容入口

这些方法已在 `cfquant.cfquant.xttrader.XtQuantTrader` 中补齐，并通过桥接端按候选 callable 调用 QMT 环境。如果 QMT 策略上下文未暴露对应函数，会返回类似 `xttrader.xxx requires QMT callable: ...` 的错误，便于定位缺口。

| 类别 | 接口 |
| --- | --- |
| 账号信息 | `query_account_info`, `query_account_infos`, `query_account_infos_async`, `query_account_status`, `query_account_status_async` |
| 综合查询 | `query_position_statistics`, `query_secu_account` |
| 信用业务 | `query_credit_detail`, `query_credit_detail_async`, `query_credit_subjects`, `query_credit_subjects_async`, `query_credit_slo_code`, `query_credit_slo_code_async`, `query_credit_assure`, `query_credit_assure_async`, `query_stk_compacts`, `query_stk_compacts_async` |
| 新股申购 | `query_ipo_data`, `query_ipo_data_async`, `query_new_purchase_limit`, `query_new_purchase_limit_async` |
| 银证业务 | `query_bank_info`, `query_bank_amount`, `query_bank_transfer_stream`, `bank_transfer_in`, `bank_transfer_in_async`, `bank_transfer_out`, `bank_transfer_out_async` |
| 资金/证券划转 | `fund_transfer`, `secu_transfer`, `ctp_transfer_future_to_option`, `ctp_transfer_future_to_option_async`, `ctp_transfer_option_to_future`, `ctp_transfer_option_to_future_async` |
| 数据同步/导出 | `query_data`, `export_data`, `sync_transaction_from_external` |
| SMT | `smt_query_compact`, `smt_query_order`, `smt_query_quoter`, `smt_appointment_order_async`, `smt_appointment_cancel_async`, `smt_negotiate_order_async`, `smt_compact_return_async`, `smt_compact_renewal_async` |

## 未完全平替项

| 项目 | 说明 |
| --- | --- |
| 非股票接口的返回模型 | 信用、银行、SMT、划转等接口当前返回 QMT 原始结果，尚未逐项包装成原版 `xtquant` 的专用对象结构。 |
| 部分 async 语义 | 交易下单和撤单 async 已有事件推送；其他 async 方法目前是同步请求完成后立即执行 callback 并返回 seq，不等价于原生异步队列。 |
| QMT callable 覆盖 | 兼容入口会尝试同名和常见候选函数，但是否存在取决于普通 QMT 策略运行环境。 |
| 系统编号撤单 | `cancel_order_stock_sysid` 已打通入口，但底层仍复用当前 `cancel` 能力，需要真实 QMT 环境确认系统编号撤单格式。 |

## 当前验证记录

```powershell
python -m py_compile cfquant\cfquant\xttrader.py cfquant\cfquant\tx_trade_bridge.py
```

结果：通过。

```powershell
@'
import inspect
from xtquant.xttrader import XtQuantTrader as Native
from cfquant.cfquant.xttrader import XtQuantTrader as Cf
native = {name: str(inspect.signature(obj)) for name, obj in inspect.getmembers(Native, inspect.isfunction) if not name.startswith('_')}
cf = {name: str(inspect.signature(obj)) for name, obj in inspect.getmembers(Cf, inspect.isfunction) if not name.startswith('_')}
print(len(native), len(cf), sorted(native.keys() - cf.keys()))
'@ | python -
```

结果：原版 75 个公开方法，`cfquant` 76 个公开方法，原版方法无缺失；额外方法为 `disconnect`。

```powershell
@'
import inspect
from xtquant.xttrader import XtQuantTraderCallback as Native
from cfquant.cfquant.xttrader import XtQuantTraderCallback as Cf
native = {name: str(inspect.signature(obj)) for name, obj in inspect.getmembers(Native, inspect.isfunction) if not name.startswith('_')}
cf = {name: str(inspect.signature(obj)) for name, obj in inspect.getmembers(Cf, inspect.isfunction) if not name.startswith('_')}
print(len(native), len(cf), sorted(native.keys() - cf.keys()))
'@ | python -
```

结果：原版 14 个公开回调方法，`cfquant` 14 个公开回调方法，无缺失。
