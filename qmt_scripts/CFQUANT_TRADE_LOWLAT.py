#coding:gbk
#! /usr/bin/python

import os
import sys


_trade_bridge = None
_callback_bridge = None
_ENTRY_VERSION = "trade_lowlat_20260908_05_unlocked_compat"
_RUN_TIME_CALLBACK_NAME = "_cfquant_trade_lowlat_pump"
_RUN_TIME_PERIOD = "50nMilliSecond"
_RUN_TIME_START_TIME = "2019-01-01 00:00:00"
DEFAULT_ACCOUNT_ID = ""
ACCOUNT_TYPE = "STOCK"
ACCOUNT_LOCKED = False
USER_BRIDGE_ID = "default"


def _entry_bridge_id():
    if ACCOUNT_LOCKED:
        return USER_BRIDGE_ID
    return os.environ.get("CFQUANT_BRIDGE_ID", USER_BRIDGE_ID)


def _validate_entry_configuration():
    if not ACCOUNT_LOCKED:
        return
    if not str(DEFAULT_ACCOUNT_ID or "").strip():
        raise ValueError("locked CFQUANT trade entry requires DEFAULT_ACCOUNT_ID")
    if not str(USER_BRIDGE_ID or "").strip() or str(USER_BRIDGE_ID).strip() == "default":
        raise ValueError("locked CFQUANT trade entry requires a non-default USER_BRIDGE_ID")
    if ACCOUNT_TYPE in (None, ""):
        raise ValueError("locked CFQUANT trade entry requires ACCOUNT_TYPE")
    account_type_text = str(ACCOUNT_TYPE).strip().upper()
    if account_type_text not in {
        "1",
        "2",
        "3",
        "5",
        "6",
        "7",
        "8",
        "10",
        "11",
        "FUTURE",
        "STOCK",
        "CREDIT",
        "FUTURE_OPTION",
        "STOCK_OPTION",
        "HUGANGTONG",
        "INCOME_SWAP",
        "NEW3BOARD",
        "SHENGANGTONG",
    }:
        raise ValueError("locked CFQUANT trade entry requires a valid ACCOUNT_TYPE")


def _ensure_path():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        if base_dir and base_dir not in sys.path:
            sys.path.insert(0, base_dir)
    except Exception:
        pass


_ensure_path()


def _load_bridge_starter():
    import cfquant.cfquant.tx_trade_bridge as tx_trade_bridge
    return tx_trade_bridge.start_tx_trade_bridge


_validate_entry_configuration()
start_tx_trade_bridge = _load_bridge_starter()

from cfquant.cfquant.channels import channels_for_bridge, normalize_bridge_id
from cfquant.cfquant.normal_bridge import NormalQmtBridge

BRIDGE_ID = normalize_bridge_id(_entry_bridge_id())
if ACCOUNT_LOCKED and BRIDGE_ID == "default":
    raise ValueError("locked CFQUANT trade entry cannot use the default bridge id")
BRIDGE_CHANNELS = channels_for_bridge(BRIDGE_ID)


def _create_trade_bridge():
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


_trade_bridge = _create_trade_bridge()
print("cfquant lowlat trade bridge module loaded")
print("cfquant lowlat entry version:%s" % _ENTRY_VERSION)
print("cfquant bridge id:%s trade_channel:%s" % (BRIDGE_ID, BRIDGE_CHANNELS["trade"]))


def init(ContextInfo):
    global _trade_bridge, _callback_bridge

    if _trade_bridge is None:
        _trade_bridge = _create_trade_bridge()

    _trade_bridge.set_context(ContextInfo)
    _trade_bridge.start()
    _callback_bridge = NormalQmtBridge.for_callback_publisher(
        _trade_bridge,
        callback_event_channel=BRIDGE_CHANNELS["callback"],
        bridge_id=BRIDGE_ID,
        account_id=DEFAULT_ACCOUNT_ID,
        globals_dict=globals(),
    )
    _callback_bridge.bind_callback_account(ContextInfo)
    print("cfquant lowlat trade context ready version:%s" % _ENTRY_VERSION)
    try:
        _register_run_time(ContextInfo)
    except Exception:
        _close_entry_bridge()
        raise


def handlebar(ContextInfo):
    _poll_trade_bridge()


def _poll_trade_bridge():
    if _trade_bridge is not None:
        _trade_bridge.poll(max_messages=100, timeout=0)


def _cfquant_trade_lowlat_pump(ContextInfo):
    _poll_trade_bridge()


def _register_run_time(ContextInfo):
    run_time = getattr(ContextInfo, "run_time", None)
    if not callable(run_time):
        raise RuntimeError(
            "cfquant lowlat trade requires ContextInfo.run_time for cooperative pump"
        )
    try:
        run_time(
            _RUN_TIME_CALLBACK_NAME,
            _RUN_TIME_PERIOD,
            _RUN_TIME_START_TIME,
        )
    except Exception as e:
        raise RuntimeError(
            "cfquant lowlat trade run_time registration failed: %s" % e
        )


def _close_entry_bridge():
    global _trade_bridge, _callback_bridge

    trade_bridge = _trade_bridge
    callback_bridge = _callback_bridge
    _trade_bridge = None
    _callback_bridge = None
    if trade_bridge:
        try:
            trade_bridge.close()
        finally:
            if getattr(trade_bridge, "callback_publisher", None) is callback_bridge:
                trade_bridge.callback_publisher = None
        return True
    return False


def stop(ContextInfo):
    if _close_entry_bridge():
        print("cfquant lowlat trade bridge stopped")


def _publish_callback(event_name, obj):
    try:
        if _callback_bridge:
            _callback_bridge.publish_callback_event(event_name, obj)
    except Exception as e:
        print("cfquant lowlat callback publish failed event=%s error=%s" % (event_name, e))


def account_callback(ContextInfo, accountInfo):
    _publish_callback("trader:on_stock_asset", accountInfo)


def order_callback(ContextInfo, orderInfo):
    _publish_callback("trader:on_stock_order", orderInfo)


def deal_callback(ContextInfo, dealInfo):
    _publish_callback("trader:on_stock_trade", dealInfo)


def position_callback(ContextInfo, positionInfo):
    _publish_callback("trader:on_stock_position", positionInfo)
