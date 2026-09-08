from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.protocol import decode_value
from cfquant.protocol import loads_message
from cfquant.protocol import pack_response
from cfquant.protocol import dumps_message
from cfquant import account_routing
from cfquant.normal_bridge import NormalQmtBridge
from cfquant.tx_trade_bridge import TxTradeBridge


class FakeTx:
    def __init__(self) -> None:
        self.pushed = []
        self.closed = False

    def push(self, kind, payload, client_id) -> None:  # noqa: ANN001
        self.pushed.append((kind, payload, client_id))

    def close(self) -> None:
        self.closed = True


class FakeQuoteContext:
    def __init__(self) -> None:
        self.callback = None
        self.unsubscribe_calls = []

    def subscribe_quote(self, stock_code, period, start_time="", end_time="", count=0, callback=None):  # noqa: ANN001
        self.callback = callback
        return 17

    def unsubscribe_quote(self, subscribe_id):  # noqa: ANN001
        self.unsubscribe_calls.append(subscribe_id)
        return True


class FakeOrderLookupContext:
    def __init__(self, *, passorder_result=None, orders=None) -> None:
        self.passorder_result = passorder_result
        self.orders = list(orders or [])
        self.passorder_calls = []
        self.query_calls = []

    def passorder(self, *args):  # noqa: ANN002
        self.passorder_calls.append(args)
        return self.passorder_result

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.query_calls.append((account_id, account_type, detail_type))
        return list(self.orders)


class FakeCancelContext:
    def __init__(self, *, order_status=50, cancel_result=True) -> None:
        self.order_status = order_status
        self.cancel_result = cancel_result
        self.query_calls = []
        self.cancel_calls = []

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.query_calls.append((account_id, account_type, detail_type))
        return [
            SimpleNamespace(
                m_strOrderSysID="3",
                m_nOrderStatus=self.order_status,
            )
        ]

    def cancel(self, order_id, account_id, account_type, context):  # noqa: ANN001
        self.cancel_calls.append((order_id, account_id, account_type, context))
        return self.cancel_result


class LockedTradeContext:
    def __init__(self, set_account_error=None, orders=None, passorder_result=12345) -> None:
        self.set_account_error = set_account_error
        self.orders = list(orders or [])
        self.passorder_result = passorder_result
        self.calls = []

    def set_account(self, account_id):  # noqa: ANN001
        self.calls.append(("set_account", account_id))
        if self.set_account_error is not None:
            raise self.set_account_error

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.calls.append(("get_trade_detail_data", account_id, account_type, detail_type))
        return list(self.orders)

    def passorder(self, *args):  # noqa: ANN002
        self.calls.append(("passorder", args))
        return self.passorder_result

    def cancel(self, *args):  # noqa: ANN002
        self.calls.append(("cancel", args))
        return True


class FakeCreditContext:
    def __init__(self, detail=None, subjects=None, slo_code=None, unclosed=None, closed=None) -> None:
        self.detail = [] if detail is None else detail
        self.subjects = [] if subjects is None else subjects
        self.slo_code = [] if slo_code is None else slo_code
        self.unclosed = [] if unclosed is None else unclosed
        self.closed = [] if closed is None else closed
        self.calls = []

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.calls.append(("get_trade_detail_data", (account_id, account_type, detail_type)))
        return self.detail

    def get_assure_contract(self, account_id):  # noqa: ANN001
        self.calls.append(("get_assure_contract", (account_id,)))
        return self.subjects

    def get_enable_short_contract(self, account_id):  # noqa: ANN001
        self.calls.append(("get_enable_short_contract", (account_id,)))
        return self.slo_code

    def get_unclosed_compacts(self, account_id, account_type):  # noqa: ANN001
        self.calls.append(("get_unclosed_compacts", (account_id, account_type)))
        return self.unclosed

    def get_closed_compacts(self, account_id, account_type):  # noqa: ANN001
        self.calls.append(("get_closed_compacts", (account_id, account_type)))
        return self.closed


def _property_only_object(values):
    class NativeObject(object):
        pass

    for name, value in values.items():
        setattr(NativeObject, name, property(lambda self, value=value: value))
    result = NativeObject()
    assert vars(result) == {}
    return result


def _credit_account(account_type=3):
    return {"account_id": "28160000447", "account_type": account_type}


def test_credit_standard_queries_use_explicit_native_signatures_and_preserve_fields() -> None:
    account_id = "28160000447"
    account = _credit_account("3")
    detail = _property_only_object(
        {
            "m_strAccountID": account_id,
            "m_dBalance": 5000.0,
            "m_dAvailable": 4500.0,
            "m_dAssureAsset": 5000.0,
        }
    )
    subjects = [
        _property_only_object(
            {
                "m_strInstrumentID": "600000",
                "m_strExchangeID": "SH",
                "m_dAssureRatio": 0.8,
                "m_dFinRatio": 0.7,
                "m_dSloRatio": 0.6,
                "m_eAssureStatus": 11,
                "m_eFinStatus": 12,
                "m_eSloStatus": 13,
            }
        ),
        _property_only_object(
            {
                "m_strInstrumentID": "000001",
                "m_strExchangeID": "SZ",
                "m_dAssureRatio": 0.5,
                "m_dFinRatio": 0.4,
                "m_dSloRatio": 0.3,
                "m_eAssureStatus": 21,
                "m_eFinStatus": 22,
                "m_eSloStatus": 23,
            }
        ),
    ]
    source = _property_only_object(
        {
            "m_strInstrumentID": "600000",
            "m_strExchangeID": "SH",
            "m_nEnableAmount": 12500,
            "m_eQuerySloType": 2,
        }
    )
    unclosed = _property_only_object(
        {
            "m_strCompactId": "CMP-1",
            "m_strEntrustNo": "ORD-1",
            "m_nCompactType": 7,
        }
    )
    closed = _property_only_object({"m_strCompactId": "CLOSED-1"})
    context = FakeCreditContext(
        detail=[detail],
        subjects=subjects,
        slo_code=[source],
        unclosed=[unclosed],
        closed=[closed],
    )
    bridge = TxTradeBridge(context=context, account_id="ordinary-account", globals_dict={}, show=False)
    params = {"account": account, "args": [], "kwargs": {}}

    detail_result = bridge._dispatch("xttrader.query_credit_detail", params, {})
    subject_result = bridge._dispatch("xttrader.query_credit_subjects", params, {})
    assure_result = bridge._dispatch("xttrader.query_credit_assure", params, {})
    source_result = bridge._dispatch("xttrader.query_credit_slo_code", params, {})
    compact_result = bridge._dispatch("xttrader.query_stk_compacts", params, {})

    assert detail_result[0]["m_dBalance"] == 5000.0
    assert detail_result[0]["account_id"] == account_id
    assert len(subject_result) == 2
    assert subject_result[0]["m_dAssureRatio"] == 0.8
    assert subject_result[0]["m_eFinStatus"] == 12
    assert subject_result[0]["stock_code"] == "600000.SH"
    assert subject_result[0]["market"] == "SH"
    assert "m_strAccountID" not in subject_result[0]
    assert assure_result[1]["m_eSloStatus"] == 23
    assert source_result[0]["enable_amount"] == 12500
    assert source_result[0]["query_slo_type"] == 2
    assert source_result[0]["m_nEnableAmount"] == 12500
    assert compact_result[0]["compact_id"] == "CMP-1"
    assert compact_result[0]["broker_order_id"] == "ORD-1"
    assert context.calls == [
        ("get_trade_detail_data", (account_id, "credit", "account")),
        ("get_assure_contract", (account_id,)),
        ("get_assure_contract", (account_id,)),
        ("get_enable_short_contract", (account_id,)),
        ("get_unclosed_compacts", (account_id, "CREDIT")),
    ]


def test_credit_native_args_are_strict_and_direct_results_round_trip() -> None:
    account_id = "28160000447"
    detail = _property_only_object(
        {"m_strAccountID": account_id, "m_dBalance": 5000.0, "m_dAvailable": 5000.0}
    )
    source = _property_only_object(
        {
            "m_strInstrumentID": "600000",
            "m_strExchangeID": "SH",
            "m_nEnableAmount": 0,
            "m_eQuerySloType": 0,
        }
    )
    compact = _property_only_object(
        {"m_strCompactId": "CMP-2", "m_strEntrustNo": "ORD-2", "m_dCompactAmount": 10.5}
    )
    context = FakeCreditContext(detail=[detail], slo_code=[source], unclosed=[compact], closed=[compact])
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)

    detail_result = bridge._dispatch(
        "xttrader.get_trade_detail_data",
        {"args": [account_id, "CREDIT", "ACCOUNT"], "kwargs": {}},
        {},
    )
    source_result = bridge._dispatch(
        "xttrader.get_enable_short_contract",
        {"args": [account_id], "kwargs": {}},
        {},
    )
    unclosed_result = bridge._dispatch(
        "xttrader.get_unclosed_compacts",
        {"args": [account_id, "CREDIT"], "kwargs": {}},
        {},
    )
    closed_result = bridge._dispatch(
        "xttrader.get_closed_compacts",
        {"args": [account_id, "CREDIT"], "kwargs": {}},
        {},
    )

    assert detail_result[0]["m_dBalance"] == 5000.0
    assert source_result[0]["enable_amount"] == 0
    assert source_result[0]["query_slo_type"] == 0
    assert unclosed_result[0]["compact_id"] == "CMP-2"
    assert closed_result[0]["broker_order_id"] == "ORD-2"
    assert context.calls == [
        ("get_trade_detail_data", (account_id, "CREDIT", "ACCOUNT")),
        ("get_enable_short_contract", (account_id,)),
        ("get_unclosed_compacts", (account_id, "CREDIT")),
        ("get_closed_compacts", (account_id, "CREDIT")),
    ]

    wire = pack_response("credit-response", result=source_result)
    decoded = decode_value(loads_message(wire)["result"])
    assert decoded == source_result
    assert decoded[0]["m_nEnableAmount"] == 0
    assert decoded[0]["enable_amount"] == 0


@pytest.mark.parametrize(
    ("action", "args", "expected_call"),
    [
        (
            "xttrader.get_trade_detail_data",
            ["28160000447", 3, "ACCOUNT"],
            ("28160000447", "CREDIT", "ACCOUNT"),
        ),
        (
            "xttrader.get_trade_detail_data",
            ["28160000447", " 3 ", " account "],
            ("28160000447", "CREDIT", "ACCOUNT"),
        ),
        (
            "xttrader.get_unclosed_compacts",
            ["28160000447", "3"],
            ("28160000447", "CREDIT"),
        ),
        (
            "xttrader.get_closed_compacts",
            ["28160000447", " cReDiT "],
            ("28160000447", "CREDIT"),
        ),
    ],
)
def test_credit_native_args_reach_callable_as_canonical_strings(action, args, expected_call) -> None:
    context = FakeCreditContext(
        detail=[{"m_dBalance": 5000.0}],
        unclosed=[{"m_strCompactId": "CMP-3"}],
        closed=[{"m_strCompactId": "CMP-4"}],
    )
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)

    bridge._dispatch(action, {"args": args, "kwargs": {}}, {})

    assert context.calls == [(action.split(".", 1)[1], expected_call)]


def test_credit_response_survives_bridge_wire_cycle() -> None:
    context = FakeCreditContext(
        subjects=[
            _property_only_object(
                {
                    "m_strInstrumentID": "000001",
                    "m_strExchangeID": "SZ",
                    "m_dAssureRatio": 0.75,
                    "m_eAssureStatus": 1,
                }
            )
        ]
    )
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    bridge.tx = FakeTx()

    bridge._handle_raw(
        dumps_message(
            {
                "type": "request",
                "id": "credit-wire-1",
                "action": "xttrader.query_credit_subjects",
                "params": {"account": _credit_account(), "args": [], "kwargs": {}},
                "client_id": "credit-client",
            }
        )
    )

    kind, payload, client_id = bridge.tx.pushed[-1]
    event = loads_message(payload)
    decoded = decode_value(event["result"])
    assert kind == "response"
    assert client_id == "credit-client"
    assert event["ok"] is True
    assert decoded[0]["stock_code"] == "000001.SZ"
    assert decoded[0]["m_dAssureRatio"] == 0.75


def test_credit_standard_query_requires_explicit_nonempty_credit_account_without_native_call() -> None:
    context = FakeCreditContext(detail=[{"m_dBalance": 1.0}])
    bridge = TxTradeBridge(context=context, account_id="ordinary-account", globals_dict={}, show=False)
    invalid_params = [
        {},
        {"account": {"account_type": 3}},
        {"account": {"account_id": "", "account_type": 3}},
        {"account": {"account_id": "28160000447", "account_type": "STOCK"}},
        {"account": {"account_id": "28160000447", "account_type": 3}, "args": ["extra"]},
        {"account": {"account_id": "28160000447", "account_type": 3}, "kwargs": {"x": 1}},
    ]

    for params in invalid_params:
        with pytest.raises(ValueError):
            bridge._dispatch("xttrader.query_credit_detail", params, {})

    assert context.calls == []


@pytest.mark.parametrize(
    "action, params",
    [
        ("xttrader.get_assure_contract", {"args": ["28160000447", "extra"]}),
        ("xttrader.get_enable_short_contract", {"args": ["28160000447"], "kwargs": {"x": 1}}),
        ("xttrader.get_unclosed_compacts", {"args": ["28160000447", "STOCK"]}),
        ("xttrader.get_closed_compacts", {"args": ["28160000447", "CREDIT", "extra"]}),
        (
            "xttrader.get_trade_detail_data",
            {"args": ["28160000447", "CREDIT", "ACCOUNT", "extra"]},
        ),
    ],
)
def test_credit_native_query_rejects_bad_args_before_call(action, params) -> None:
    context = FakeCreditContext()
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)

    with pytest.raises(ValueError):
        bridge._dispatch(action, params, {})

    assert context.calls == []


def test_credit_direct_stock_position_detail_keeps_generic_behavior() -> None:
    raw = SimpleNamespace(m_strInstrumentID="600000", m_strExchangeID="SH")
    context = FakeCreditContext(detail=[raw])
    bridge = TxTradeBridge(context=context, account_id="ordinary-account", globals_dict={}, show=False)

    result = bridge._dispatch(
        "xttrader.get_trade_detail_data",
        {"args": ["2070001669", "STOCK", "POSITION"], "kwargs": {}},
        {},
    )

    assert result == [raw]
    assert context.calls == [
        ("get_trade_detail_data", ("2070001669", "STOCK", "POSITION")),
    ]


@pytest.mark.parametrize("method, result_attr", [("query_credit_detail", "detail"), ("query_credit_subjects", "subjects")])
def test_credit_query_distinguishes_none_from_empty_list(method, result_attr) -> None:
    context = FakeCreditContext()
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    params = {"account": _credit_account(), "args": [], "kwargs": {}}

    setattr(context, result_attr, [])
    assert bridge._dispatch("xttrader.%s" % method, params, {}) == []

    setattr(context, result_attr, None)
    with pytest.raises(RuntimeError, match="returned None"):
        bridge._dispatch("xttrader.%s" % method, params, {})


def test_credit_record_conversion_fails_closed_for_unreadable_or_empty_objects() -> None:
    class UnreadableObject(object):
        @property
        def m_strInstrumentID(self):
            raise RuntimeError("native field unavailable")

    context = FakeCreditContext(subjects=[_property_only_object({})])
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    params = {"account": _credit_account(), "args": [], "kwargs": {}}

    with pytest.raises(RuntimeError, match="conversion failed"):
        bridge._dispatch("xttrader.query_credit_subjects", params, {})

    context.subjects = [UnreadableObject()]
    with pytest.raises(RuntimeError, match="not readable"):
        bridge._dispatch("xttrader.query_credit_subjects", params, {})


def test_credit_record_skips_unreadable_optional_property_but_rejects_unreadable_identity() -> None:
    class OptionalFieldObject(object):
        @property
        def m_strInstrumentID(self):
            return "600000"

        @property
        def m_strExchangeID(self):
            return "SH"

        @property
        def m_dAssureRatio(self):
            return 0.8

        @property
        def m_dUnsupportedOptional(self):
            raise RuntimeError("optional field unavailable")

    context = FakeCreditContext(subjects=[OptionalFieldObject()])
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    params = {"account": _credit_account(), "args": [], "kwargs": {}}

    result = bridge._dispatch("xttrader.query_credit_subjects", params, {})

    assert result[0]["m_strInstrumentID"] == "600000"
    assert result[0]["m_dAssureRatio"] == 0.8
    assert "m_dUnsupportedOptional" not in result[0]

    class UnreadableIdentityObject(object):
        @property
        def m_strAccountID(self):
            raise RuntimeError("account identity unavailable")

        @property
        def m_dBalance(self):
            return 5000.0

    context.detail = [UnreadableIdentityObject()]
    with pytest.raises(RuntimeError, match="account identity field is not readable"):
        bridge._dispatch("xttrader.query_credit_detail", params, {})


def test_credit_record_rejects_mismatched_account_and_does_not_number_can_not_convert() -> None:
    context = FakeCreditContext(
        detail=[
            _property_only_object(
                {
                    "m_strAccountID": "other-account",
                    "m_dBalance": 1.0,
                }
            )
        ]
    )
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    params = {"account": _credit_account(), "args": [], "kwargs": {}}

    with pytest.raises(RuntimeError, match="account mismatch"):
        bridge._dispatch("xttrader.query_credit_detail", params, {})

    context.subjects = [
        _property_only_object(
            {
                "m_strInstrumentID": "600000",
                "m_strExchangeID": "SH",
                "m_dAssureRatio": "<CanNotConvert>",
                "m_eAssureStatus": 4,
            }
        )
    ]
    result = bridge._dispatch("xttrader.query_credit_subjects", params, {})
    assert result[0]["m_eAssureStatus"] == 4
    assert "m_dAssureRatio" not in result[0]
    assert "<CanNotConvert>" not in repr(result[0])


def _bridge() -> TxTradeBridge:
    return TxTradeBridge(context=None, globals_dict={})


def test_normal_and_lowlat_bridges_share_order_and_cancel_implementation() -> None:
    assert NormalQmtBridge._order_stock is TxTradeBridge._order_stock
    assert NormalQmtBridge._cancel_order_stock is TxTradeBridge._cancel_order_stock


def test_status_probe_does_not_write_per_request_bridge_logs() -> None:
    bridge = _bridge()
    bridge.tx = FakeTx()
    logs = []
    bridge._log = logs.append

    bridge._handle_raw(
        dumps_message(
            {
                "type": "request",
                "id": "status-1",
                "action": "cfquant.status",
                "params": {},
                "client_id": "client-1",
            }
        )
    )

    assert logs == []
    assert len(bridge.tx.pushed) == 1


def test_format_trade_detail_order_exposes_standard_alias_fields() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="600000",
        m_strExchangeID="SH",
        m_strInstrumentName="PF Bank",
        m_strRemark="cid-1",
        m_nOrderID=12345,
        m_strOrderSysID="SYS-1",
        m_nOrderType=23,
        m_nPriceType=42,
        m_dOrderPrice=10.25,
        m_nOrderStatus=56,
        m_strOrderDate="20260715",
        m_strOrderTime="09:35:01",
    )

    payload = bridge._format_trade_detail(raw, "order")

    assert payload["stock_code"] == "600000.SH"
    assert payload["account_id"] == ""
    assert payload["order_remark"] == "cid-1"
    assert payload["order_id"] == 12345
    assert payload["order_sysid"] == "SYS-1"
    assert payload["order_type"] == 23
    assert payload["direction"] == 23
    assert payload["price_type"] == 42
    assert payload["price"] == 10.25
    assert payload["order_status"] == 56
    assert payload["order_date"] == "20260715"
    assert payload["order_time"] == "09:35:01"
    assert payload["m_strOrderSysID"] == "SYS-1"


def test_query_trade_detail_order_maps_qmt_native_aliases() -> None:
    raw = SimpleNamespace(
        m_strInstrumentID="301559",
        m_strExchangeID="SZ",
        m_strRemark="WQd4c2cce977aa284c017d41",
        m_nOrderID=34944,
        m_strOrderSysID="34944",
        m_nOrderType=23,
        m_nOrderPriceType=50,
        m_dLimitPrice=10.25,
        m_dTradedPrice=0,
        m_nOrderStatus=54,
        m_nVolumeTotalOriginal=200,
        m_nVolumeTraded=0,
        m_strOrderTime="09:35:01",
        m_strInsertDate="20260907",
    )
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: [raw]},
    )

    payload = bridge._query_trade_detail({"account_id": "2070001669"}, "order")[0]

    assert payload["price_type"] == 50
    assert payload["price"] == 10.25
    assert payload["traded_price"] == 0
    assert payload["order_date"] == "20260907"
    assert payload["m_nOrderPriceType"] == 50
    assert payload["m_dLimitPrice"] == 10.25
    assert payload["m_strInsertDate"] == "20260907"


def test_query_trade_detail_order_preserves_old_alias_priority_and_zero() -> None:
    raw = SimpleNamespace(
        m_strInstrumentID="600000",
        m_strExchangeID="SH",
        price_type=5,
        m_nPriceType=5,
        m_nOrderPriceType=50,
        price=0,
        m_dLimitPrice=10.25,
        order_date="20260801",
        m_strInsertDate="20260907",
    )
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: [raw]},
    )

    payload = bridge._query_trade_detail({"account_id": "2070001669"}, "order")[0]

    assert payload["price_type"] == 5
    assert payload["price"] == 0
    assert payload["order_date"] == "20260801"


def test_format_trade_detail_deal_exposes_standard_alias_fields() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="920833",
        m_strExchangeID="BJ",
        m_strInstrumentName="BSE Name",
        m_strRemark="cid-bj",
        m_nOrderID=456,
        m_strOrderSysID="SYS-BJ",
        m_nOrderType=24,
        m_nPriceType=11,
        m_strTradeID="TR-1",
        m_strDealID="DL-1",
        m_dPrice=9.88,
        m_nVolume=300,
        m_nOrderStatus=55,
        m_strTradeDate="20260715",
        m_strTradeTime="10:01:05",
    )

    payload = bridge._format_trade_detail(raw, "deal")

    assert payload["stock_code"] == "920833.BJ"
    assert payload["account_id"] == ""
    assert payload["order_remark"] == "cid-bj"
    assert payload["order_id"] == 456
    assert payload["order_sysid"] == "SYS-BJ"
    assert payload["trade_id"] == "TR-1"
    assert payload["deal_id"] == "DL-1"
    assert payload["order_type"] == 24
    assert payload["direction"] == 24
    assert payload["price_type"] == 11
    assert payload["price"] == 9.88
    assert payload["order_status"] == 55
    assert payload["trade_date"] == "20260715"
    assert payload["trade_time"] == "10:01:05"


def test_format_trade_detail_prefers_stock_offset_flag_for_direction() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="513300",
        m_strExchangeID="SH",
        m_nDirection=48,
        m_nOffsetFlag=49,
    )

    order = bridge._format_trade_detail(raw, "order")
    deal = bridge._format_trade_detail(raw, "deal")

    assert order["direction"] == 49
    assert deal["direction"] == 49


def test_dispatch_subscribe_quote_pushes_quote_event_and_unsubscribe_cleans_up() -> None:
    context = FakeQuoteContext()
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()

    result = bridge._dispatch(
        "xtdata.subscribe_quote",
        {
            "stock_code": "600000.SH",
            "period": "tick",
            "start_time": "",
            "end_time": "",
            "count": 0,
        },
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    assert result == {"subscribe_id": 17}
    assert 17 in bridge.subscriptions
    assert bridge.client_subscriptions["cli-1"] == {17}

    assert context.callback is not None
    context.callback({"600000.SH": {"lastPrice": 10.2}})

    kind, payload, client_id = bridge.tx.pushed[-1]
    event = loads_message(payload)
    assert kind == "event"
    assert client_id == "cli-1"
    assert event["type"] == "event"
    assert event["event"] == "quote:17"
    assert event["subscription_id"] == 17
    assert event["data"] == {"600000.SH": {"lastPrice": 10.2}}

    unsubscribe = bridge._dispatch(
        "xtdata.unsubscribe_quote",
        {"subscribe_id": 17},
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    assert unsubscribe is True
    assert context.unsubscribe_calls == [17]
    assert bridge.subscriptions == {}
    assert bridge.client_subscriptions == {}


def test_query_trade_detail_propagates_request_account_id_to_all_payload_types() -> None:
    row = SimpleNamespace(m_strInstrumentID="600000", m_strExchangeID="SH", m_dBalance=100.0, m_nVolume=200)
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, detail_type: [row]},
    )

    order_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "order")
    deal_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "deal")
    position_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "position")
    account_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "account")

    assert order_rows[0]["account_id"] == "2070001669"
    assert deal_rows[0]["account_id"] == "2070001669"
    assert position_rows[0]["account_id"] == "2070001669"
    assert account_rows[0]["account_id"] == "2070001669"


def test_format_trade_detail_account_preserves_asset_classification() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strAccountID="28100046850",
        m_dBalance=470096.99,
        m_dAvailable=6.5,
        m_dFrozenCash=170000.0,
        m_dInstrumentValue=0.0,
        m_dStockValue=0.0,
        m_dFundValue=0.0,
        m_dLoanValue=0.0,
        m_dRepurchaseValue=300000.0,
        m_dFetchBalance=6.5,
        m_dBuyWaitMoney=0.0,
        m_dSellWaitMoney=0.0,
        m_dCommission=0.0,
        m_strTradingDate="20260731",
    )

    payload = bridge._format_trade_detail(raw, "account")

    assert payload["account_id"] == "28100046850"
    assert payload["total_asset"] == 470096.99
    assert payload["cash"] == 6.5
    assert payload["frozen"] == 170000.0
    assert payload["frozen_cash"] == 170000.0
    assert payload["repurchase_value"] == 300000.0
    assert payload["m_dRepurchaseValue"] == 300000.0
    assert payload["withdrawable"] == 6.5
    assert payload["trading_date"] == "20260731"


@pytest.mark.parametrize("detail_type", ["position", "order", "deal"])
def test_query_trade_detail_preserves_empty_list_as_success(detail_type) -> None:
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: []},
    )

    assert bridge._query_trade_detail({"account_id": "2070001669"}, detail_type) == []


@pytest.mark.parametrize("detail_type", ["position", "order", "deal"])
def test_query_trade_detail_rejects_none_snapshot(detail_type) -> None:
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: None},
    )

    with pytest.raises(RuntimeError, match="trade detail query returned None"):
        bridge._query_trade_detail({"account_id": "2070001669"}, detail_type)


def test_close_unsubscribes_remaining_quote_subscriptions() -> None:
    context = FakeQuoteContext()
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()
    bridge._dispatch(
        "xtdata.subscribe_quote",
        {"stock_code": "600000.SH", "period": "tick"},
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    bridge.close()

    assert context.unsubscribe_calls == [17]
    assert bridge.subscriptions == {}
    assert bridge.client_subscriptions == {}


def test_order_stock_finds_stable_order_id_by_remark_when_passorder_returns_none() -> None:
    context = FakeOrderLookupContext(
        passorder_result=None,
        orders=[
            SimpleNamespace(
                m_strRemark="cid-remark",
                m_nOrderID=12345,
                m_strOrderSysID="SYS-12345",
            )
        ],
    )
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
                "account_id": "2070001669",
                "stock_code": "600000.SH",
                "order_type": 23,
                "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "strategy_name": "alpha",
            "order_remark": "cid-remark",
            "find_order_wait": 0,
        },
        {"id": "req-1"},
    )

    assert result["request_result"] is None
    assert result["order_id"] == "SYS-12345"
    assert result["order_remark"] == "cid-remark"
    assert context.query_calls == [("2070001669", "stock", "order")]


@pytest.mark.parametrize("passorder_result", [0, -1, "0", "-1", False])
def test_order_stock_finds_stable_order_id_when_passorder_returns_non_order_status(
    passorder_result,
) -> None:
    context = FakeOrderLookupContext(
        passorder_result=passorder_result,
        orders=[
            SimpleNamespace(
                m_strRemark="cid-zero",
                m_nOrderID=None,
                m_strOrderSysID="SYS-ZERO",
            )
        ],
    )
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
                "account_id": "2070001669",
                "stock_code": "513300.SH",
                "order_type": 23,
                "price_type": 11,
            "price": 2.5,
            "order_volume": 100,
            "strategy_name": "limit-probe",
            "order_remark": "cid-zero",
            "find_order_wait": 0,
        },
        {"id": "req-zero"},
    )

    assert result["request_result"] == passorder_result
    assert result["order_id"] == "SYS-ZERO"
    assert context.query_calls == [("2070001669", "stock", "order")]


def test_order_stock_keeps_positive_passorder_order_id_without_lookup() -> None:
    context = FakeOrderLookupContext(passorder_result=12345, orders=[])
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
                "account_id": "2070001669",
                "stock_code": "513300.SH",
                "order_type": 23,
                "price_type": 11,
            "price": 2.5,
            "order_volume": 100,
            "order_remark": "cid-positive",
        },
        {"id": "req-positive"},
    )

    assert result["order_id"] == 12345
    assert context.query_calls == []


@pytest.mark.parametrize(
    ("account_type", "order_type", "expected_passorder_type"),
    [
        (2, 23, 23),
        ("STOCK", "sell", 24),
        (3, 23, 33),
        (" credit ", "sell", 34),
        ("CREDIT", "27", 27),
        ("3", 28, 28),
        (3, 33, 33),
        (3, 34, 34),
    ],
)
def test_order_stock_maps_credit_operation_codes_only_for_explicit_account_type(
    account_type, order_type, expected_passorder_type
) -> None:
    context = FakeOrderLookupContext(passorder_result=12345)
    bridge = TxTradeBridge(context=context, account_id="ordinary-default", globals_dict={})

    result = bridge._order_stock(
        {
            "account": {"account_id": "28160000447", "account_type": account_type},
            "stock_code": "600000.SH",
            "order_type": order_type,
            "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "order_remark": "credit-op-%s" % expected_passorder_type,
        },
        {"id": "req-credit-op"},
    )

    assert result["order_id"] == 12345
    assert context.passorder_calls[0][0] == expected_passorder_type
    assert context.passorder_calls[0][2] == "28160000447"


@pytest.mark.parametrize(
    "params",
    [
        {"account_id": "2070001669", "order_type": 27},
        {
            "account": {"account_id": "2070001669", "account_type": "STOCK"},
            "order_type": 28,
        },
        {
            "account": {"account_id": "2070001669", "account_type": "UNKNOWN"},
            "order_type": 33,
        },
    ],
)
def test_order_stock_rejects_credit_operation_without_credit_account_type(params) -> None:
    context = FakeOrderLookupContext(passorder_result=12345)
    bridge = TxTradeBridge(context=context, account_id="ordinary-default", globals_dict={})

    with pytest.raises(ValueError, match="CREDIT account type"):
        bridge._order_stock(dict(params, stock_code="600000.SH"), {"id": "req-invalid-credit-op"})

    assert context.passorder_calls == []


def test_order_stock_credit_request_never_falls_back_to_bridge_default_account() -> None:
    context = FakeOrderLookupContext(passorder_result=12345)
    bridge = TxTradeBridge(context=context, account_id="ordinary-default", globals_dict={})

    with pytest.raises(ValueError, match="explicit account_id"):
        bridge._order_stock(
            {
                "account": {"account_type": "CREDIT"},
                "stock_code": "600000.SH",
                "order_type": 23,
            },
            {"id": "req-credit-no-account"},
        )

    assert context.passorder_calls == []


@pytest.mark.parametrize("order_type", ["", "buy-ish", "23.0", True, 23.5, None])
def test_order_stock_rejects_non_integer_operation_values_before_passorder(order_type) -> None:
    context = FakeOrderLookupContext(passorder_result=12345)
    bridge = TxTradeBridge(context=context, globals_dict={})

    with pytest.raises(ValueError, match="order_type"):
        bridge._order_stock(
            {
                "account": {"account_id": "2070001669", "account_type": "STOCK"},
                "stock_code": "600000.SH",
                "order_type": order_type,
            },
            {"id": "req-invalid-order-type"},
        )

    assert context.passorder_calls == []


def test_order_stock_credit_id_lookup_uses_credit_detail_and_stable_strategy_match() -> None:
    context = FakeOrderLookupContext(
        passorder_result=None,
        orders=[
            SimpleNamespace(
                m_strRemark="credit-lookup",
                m_strStrategyName="other-strategy",
                m_nOrderID=111,
                m_strOrderSysID="WRONG-STRATEGY",
            ),
            SimpleNamespace(
                m_strRemark="credit-lookup",
                m_strStrategyName="credit-strategy",
                m_nOrderID=0,
                m_strOrderSysID="CREDIT-SYS-1",
            ),
        ],
    )
    bridge = TxTradeBridge(context=context, account_id="ordinary-default", globals_dict={})

    result = bridge._order_stock(
        {
            "account": {"account_id": "28160000447", "account_type": 3},
            "stock_code": "600000.SH",
            "order_type": "23",
            "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "strategy_name": "credit-strategy",
            "order_remark": "credit-lookup",
            "find_order_wait": 0,
        },
        {"id": "req-credit-lookup"},
    )

    assert context.passorder_calls[0][0] == 33
    assert context.query_calls == [("28160000447", "credit", "order")]
    assert result["order_id"] == "CREDIT-SYS-1"


def test_order_stock_lookup_does_not_treat_nonpositive_ids_as_known() -> None:
    context = FakeOrderLookupContext(
        passorder_result=None,
        orders=[
            SimpleNamespace(
                m_strRemark="invalid-lookup",
                m_nOrderID=0,
                m_strOrderSysID="-1",
                m_strOrderID="",
            )
        ],
    )
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
                "account_id": "2070001669",
                "stock_code": "600000.SH",
                "order_type": 23,
                "order_remark": "invalid-lookup",
            "find_order_wait": 0,
        },
        {"id": "req-invalid-lookup"},
    )

    assert result["order_id"] is None


def test_order_stock_async_keeps_missing_order_id_none_when_lookup_fails() -> None:
    context = FakeOrderLookupContext(passorder_result=None, orders=[])
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()

    result = bridge._order_stock_async(
        {
            "seq": 9,
            "account_id": "2070001669",
            "stock_code": "600000.SH",
            "order_type": 23,
            "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "order_remark": "cid-missing",
            "find_order_wait": 0,
        },
        {"id": "req-2", "client_id": "cli-2"},
    )

    assert result["order_id"] is None
    kind, payload, client_id = bridge.tx.pushed[-1]
    event = loads_message(payload)
    assert kind == "event"
    assert client_id == "cli-2"
    assert event["event"] == "trader:on_order_stock_async_response"
    assert event["data"]["order_id"] is None
    assert event["data"]["order_remark"] == "cid-missing"
    assert context.query_calls == [("2070001669", "stock", "order")]


def test_cancel_queries_active_status_before_native_cancel() -> None:
    context = FakeCancelContext(order_status=50)
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "3"}
    )

    assert result == {"cancel_result": 0, "request_result": True, "order_id": "3"}
    assert context.query_calls == [("2070001669", "stock", "order")]
    assert context.cancel_calls == [("3", "2070001669", "STOCK", context)]


def test_cancel_refuses_terminal_order_without_native_cancel() -> None:
    context = FakeCancelContext(order_status=56)
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "3"}
    )

    assert result["cancel_result"] == -1
    assert result["reason"] == "order_not_active_or_status_unknown"
    assert result["order_status"] == 56
    assert context.cancel_calls == []


def test_cancel_refuses_missing_order_without_native_cancel() -> None:
    context = FakeCancelContext(order_status=50)
    context.get_trade_detail_data = lambda *args: []
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "missing"}
    )

    assert result["cancel_result"] == -1
    assert result["reason"] == "order_not_active_or_status_unknown"
    assert result["order_status"] is None
    assert context.cancel_calls == []


def test_cancel_credit_queries_credit_detail_before_native_cancel() -> None:
    context = FakeCancelContext(order_status=50)
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {
            "account": {"account_id": "28160000447", "account_type": 3},
            "order_id": "3",
        }
    )

    assert result == {"cancel_result": 0, "request_result": True, "order_id": "3"}
    assert context.query_calls == [("28160000447", "credit", "order")]
    assert context.cancel_calls == [("3", "28160000447", "credit", context)]


def _locked_trade_bridge(context, bridge_id="locked-credit-tests"):
    return TxTradeBridge(
        context=context,
        bridge_id=bridge_id,
        account_id="28160000447",
        account_locked=True,
        account_type="CREDIT",
        globals_dict={},
        show=False,
    )


def test_locked_subscribe_requires_context_before_writing_any_route() -> None:
    bridge = _locked_trade_bridge(None, bridge_id="locked-no-context")

    with pytest.raises(RuntimeError, match="requires a QMT context"):
        bridge._dispatch(
            "xttrader.subscribe",
            {"account": _credit_account("CREDIT")},
            {"client_id": "credit-client"},
        )

    assert bridge.account_subscribers == {}
    assert bridge.client_accounts == {}
    assert account_routing.client_ids("locked-no-context", "28160000447") == []


def test_locked_subscribe_set_account_failure_leaves_local_and_shared_routes_empty() -> None:
    context = LockedTradeContext(set_account_error=RuntimeError("bind failed"))
    bridge = _locked_trade_bridge(context, bridge_id="locked-bind-failure")

    with pytest.raises(RuntimeError, match="bind failed"):
        bridge._dispatch(
            "xttrader.subscribe",
            {"account": _credit_account("CREDIT")},
            {"client_id": "credit-client"},
        )

    assert context.calls == [("set_account", "28160000447")]
    assert bridge.account_subscribers == {}
    assert bridge.client_accounts == {}
    assert account_routing.client_ids("locked-bind-failure", "28160000447") == []


def test_locked_unsubscribe_keeps_configured_identity_but_clears_routes() -> None:
    bridge_id = "locked-unsubscribe"
    context = LockedTradeContext()
    bridge = _locked_trade_bridge(context, bridge_id=bridge_id)
    account = _credit_account("3")

    assert bridge._dispatch("xttrader.subscribe", {"account": account}, {"client_id": "credit-client"}) == 0
    assert account_routing.client_ids(bridge_id, "28160000447") == ["credit-client"]

    assert bridge._dispatch("xttrader.unsubscribe", {"account": account}, {"client_id": "credit-client"}) == 0

    assert bridge.account_id == "28160000447"
    assert bridge.account_type == 3
    assert bridge.account_subscribers == {}
    assert bridge.client_accounts == {}
    assert account_routing.client_ids(bridge_id, "28160000447") == []


@pytest.mark.parametrize(
    "action, params",
    [
        (
            "xttrader.query_stock_positions",
            {"account": {"account_id": "other-account", "account_type": "CREDIT"}},
        ),
        (
            "xttrader.query_stock_orders",
            {"account": {"account_id": "28160000447"}},
        ),
        (
            "xttrader.query_stock_trades",
            {
                "account": {"account_id": "28160000447", "account_type": "CREDIT"},
                "account_id": "other-account",
            },
        ),
        (
            "xttrader.query_stock_asset",
            {
                "account": {"account_id": "28160000447", "account_type": "STOCK"},
            },
        ),
        (
            "cfquant.query_info",
            {"account": {"account_id": "28160000447", "account_type": "CREDIT"}, "account_type": "STOCK"},
        ),
        (
            "xttrader.order_stock",
            {
                "account": {"account_id": "28160000447", "account_type": "CREDIT"},
                "account_id": "other-account",
                "stock_code": "600000.SH",
                "order_type": 23,
            },
        ),
        (
            "xttrader.cancel_order_stock",
            {
                "account": {"account_id": "28160000447", "account_type": "CREDIT"},
                "account_type": "STOCK",
                "order_id": "3",
            },
        ),
    ],
)
def test_locked_business_call_rejects_missing_or_conflicting_identity_before_native_call(action, params) -> None:
    context = LockedTradeContext()
    bridge = _locked_trade_bridge(context, bridge_id="locked-identity-guards")

    with pytest.raises(ValueError):
        bridge._dispatch(action, params, {"id": "guarded-request"})

    assert context.calls == []


@pytest.mark.parametrize(
    "params",
    [
        {"args": ["other-account", "CREDIT"]},
        {"args": ["28160000447"]},
        {"args": ["28160000447", "STOCK"]},
    ],
)
def test_locked_credit_native_args_are_checked_before_native_call(params) -> None:
    context = FakeCreditContext()
    bridge = _locked_trade_bridge(context, bridge_id="locked-native-guards")

    with pytest.raises(ValueError):
        bridge._dispatch("xttrader.get_unclosed_compacts", params, {})

    assert context.calls == []


def test_locked_unknown_generic_request_is_rejected_before_callable_lookup_call() -> None:
    calls = []
    bridge = _locked_trade_bridge(
        SimpleNamespace(unknown=lambda *args: calls.append(args)),
        bridge_id="locked-generic-guard",
    )

    with pytest.raises(ValueError, match="unknown xttrader generic request"):
        bridge._dispatch("xttrader.unknown", {"account": _credit_account()}, {})

    assert calls == []


def test_locked_batch_validates_every_row_before_first_passorder() -> None:
    context = LockedTradeContext()
    bridge = _locked_trade_bridge(context, bridge_id="locked-batch-guards")
    params = {
        "account": _credit_account(),
        "orders": [
            {"stock_code": "600000.SH", "order_type": 23},
            {"stock_code": "000001.SZ"},
        ],
    }

    with pytest.raises(ValueError, match="order_type"):
        bridge._dispatch("xttrader.order_stock_batch", params, {"id": "batch-guard"})

    assert context.calls == []


def test_unlocked_batch_rejects_missing_operation_before_any_passorder() -> None:
    context = LockedTradeContext()
    bridge = TxTradeBridge(context=context, globals_dict={}, show=False)
    params = {
        "account_id": "2070001669",
        "orders": [
            {"stock_code": "600000.SH", "order_type": 23},
            {"stock_code": "000001.SZ"},
        ],
    }

    with pytest.raises(ValueError, match="order_type"):
        bridge._dispatch("xttrader.order_stock_batch", params, {"id": "batch-guard"})

    assert [call[0] for call in context.calls] == []
