# -*- coding: utf-8 -*-
import math
import os
import sys
import threading
import time

from .protocol import loads_message, pack_event, pack_response
from . import account_routing


XTTRADER_COMPAT_CANDIDATES = {
    "query_account_info": ("query_account_info", "get_account_info"),
    "query_account_infos": ("query_account_infos", "get_account_infos", "query_account_info", "get_account_info"),
    "query_account_status": ("query_account_status", "get_account_status"),
    "query_position_statistics": ("query_position_statistics", "get_position_statistics"),
    "query_secu_account": ("query_secu_account", "get_secu_account"),
    "query_credit_detail": ("query_credit_detail", "get_credit_detail"),
    "query_credit_subjects": ("query_credit_subjects", "get_credit_subjects"),
    "query_credit_slo_code": ("query_credit_slo_code", "get_credit_slo_code"),
    "query_credit_assure": ("query_credit_assure", "get_credit_assure"),
    "query_stk_compacts": ("query_stk_compacts", "get_stk_compacts"),
    "query_ipo_data": ("query_ipo_data", "get_ipo_data"),
    "query_new_purchase_limit": ("query_new_purchase_limit", "get_new_purchase_limit"),
    "query_bank_info": ("query_bank_info", "get_bank_info"),
    "query_bank_amount": ("query_bank_amount", "get_bank_amount"),
    "query_bank_transfer_stream": ("query_bank_transfer_stream", "get_bank_transfer_stream"),
    "bank_transfer_in": ("bank_transfer_in", "transfer_bank_to_security"),
    "bank_transfer_out": ("bank_transfer_out", "transfer_security_to_bank"),
    "fund_transfer": ("fund_transfer",),
    "secu_transfer": ("secu_transfer",),
    "ctp_transfer_future_to_option": ("ctp_transfer_future_to_option",),
    "ctp_transfer_option_to_future": ("ctp_transfer_option_to_future",),
    "query_data": ("query_data",),
    "export_data": ("export_data",),
    "sync_transaction_from_external": ("sync_transaction_from_external",),
    "smt_query_compact": ("smt_query_compact",),
    "smt_query_order": ("smt_query_order",),
    "smt_query_quoter": ("smt_query_quoter",),
    "smt_appointment_order": ("smt_appointment_order",),
    "smt_appointment_cancel": ("smt_appointment_cancel",),
    "smt_negotiate_order": ("smt_negotiate_order",),
    "smt_compact_return": ("smt_compact_return",),
    "smt_compact_renewal": ("smt_compact_renewal",),
}


CREDIT_QUERY_NATIVE_METHODS = {
    "query_credit_detail": "get_trade_detail_data",
    "query_credit_subjects": "get_assure_contract",
    "query_credit_assure": "get_assure_contract",
    "query_credit_slo_code": "get_enable_short_contract",
    "query_stk_compacts": "get_unclosed_compacts",
}
CREDIT_NATIVE_METHODS = {
    "get_trade_detail_data",
    "get_assure_contract",
    "get_enable_short_contract",
    "get_unclosed_compacts",
    "get_closed_compacts",
}
_CREDIT_UNAVAILABLE = object()

ACCOUNT_TYPE_CODES = {
    1: "FUTURE",
    2: "STOCK",
    3: "CREDIT",
    5: "FUTURE_OPTION",
    6: "STOCK_OPTION",
    7: "HUGANGTONG",
    8: "INCOME_SWAP",
    10: "NEW3BOARD",
    11: "SHENGANGTONG",
}
ACCOUNT_TYPE_NAMES = dict((name, code) for code, name in ACCOUNT_TYPE_CODES.items())
ACCOUNT_TYPE_NAMES.update({
    "SECURITY": 2,
    "STOCK_ACCOUNT": 2,
    "CREDIT_ACCOUNT": 3,
})
CREDIT_OPERATION_CODES = frozenset(range(27, 35))
ACCOUNT_ID_FIELDS = (
    "account_id",
    "m_strAccountID",
    "m_strAccountId",
    "m_strAccount",
    "m_accountID",
)
ACCOUNT_TYPE_FIELDS = ("account_type", "m_nAccountType")
BRIDGE_ID_FIELDS = ("bridge_id", "qmt_bridge_id")

# QMT's embedded account object contains a wider asset breakdown than the
# six fields historically forwarded by cfquant. Keep the raw names alongside
# stable aliases so broker-specific assets such as reverse repos are not lost.
ACCOUNT_DETAIL_FIELDS = (
    ("frozen_cash", "m_dFrozenCash"),
    ("frozen_commission", "m_dFrozenCommission"),
    ("commission", "m_dCommission"),
    ("pre_balance", "m_dPreBalance"),
    ("asset_balance", "m_dAssetBalance"),
    ("withdrawable", "m_dFetchBalance"),
    ("stock_value", "m_dStockValue"),
    ("bond_value", "m_dLoanValue"),
    ("fund_value", "m_dFundValue"),
    ("repurchase_value", "m_dRepurchaseValue"),
    ("buy_wait_money", "m_dBuyWaitMoney"),
    ("sell_wait_money", "m_dSellWaitMoney"),
    ("entrust_asset", "m_dEntrustAsset"),
    ("cash_in", "m_dCashIn"),
    ("deposit", "m_dDeposit"),
    ("withdraw", "m_dWithdraw"),
    ("used_margin", "m_dUsedMargin"),
    ("current_margin", "m_dCurrMargin"),
    ("margin", "m_dMargin"),
    ("raw_margin", "m_dRawMargin"),
    ("real_used_margin", "m_dRealUsedMargin"),
    ("frozen_margin", "m_dFrozenMargin"),
    ("close_profit", "m_dCloseProfit"),
    ("risk", "m_dRisk"),
    ("real_risk_degree", "m_dRealRiskDegree"),
    ("nav", "m_dNav"),
    ("net_value", "m_dNetValue"),
    ("royalty", "m_dRoyalty"),
    ("frozen_royalty", "m_dFrozenRoyalty"),
    ("trading_date", "m_strTradingDate"),
    ("account_status", "m_strStatus"),
)
ACCOUNT_DETAIL_RAW_FIELDS = tuple(raw_name for _, raw_name in ACCOUNT_DETAIL_FIELDS)
_BRIDGE_LOG_LOCK = threading.RLock()


class TxTradeBridge(object):
    def __init__(
        self,
        context,
        ip="127.0.0.1",
        port=2049,
        token="LTtx",
        request_channel="cfquant.request",
        bridge_id="default",
        account_id="",
        show=True,
        globals_dict=None,
        account_locked=False,
        account_type=2,
        callback_publisher=None,
    ):
        self.context = context
        self.ip = ip
        self.port = int(port)
        self.token = token
        self.request_channel = request_channel
        self.bridge_id = bridge_id or "default"
        self.account_locked = bool(account_locked)
        if self.account_locked:
            self.account_id = self._normalize_account_id(account_id, "locked bridge account_id")
            self.account_type = self._normalize_account_type_code(account_type)
            self._locked_account_id = self.account_id
            self._locked_account_type = self.account_type
        else:
            self.account_id = account_id
            self.account_type = account_type
            self._locked_account_id = ""
            self._locked_account_type = None
        self.show = show
        self.globals_dict = globals_dict or {}
        self.running = False
        self.tx = None
        explicit_log_file = str(os.environ.get("CFQUANT_BRIDGE_LOG_FILE") or "").strip()
        explicit_log_dir = str(os.environ.get("CFQUANT_LOG_DIR") or "").strip()
        if explicit_log_file:
            self.log_file = os.path.abspath(os.path.expanduser(explicit_log_file))
        elif explicit_log_dir:
            self.log_file = os.path.join(
                os.path.abspath(os.path.expanduser(explicit_log_dir)),
                "cfquant_qmt_bridge.log",
            )
        else:
            self.log_file = os.path.join(os.getcwd(), "cfquant_qmt_bridge.log")
        log_parent = os.path.dirname(os.path.abspath(self.log_file))
        if log_parent:
            try:
                os.makedirs(log_parent, exist_ok=True)
            except Exception:
                pass
        self.log_max_bytes = int(os.environ.get("CFQUANT_BRIDGE_LOG_MAX_BYTES", str(10 * 1024 * 1024)))
        self.log_backup_count = int(os.environ.get("CFQUANT_BRIDGE_LOG_BACKUP_COUNT", "2"))
        self.account_subscribers = {}
        self.client_accounts = {}
        self.subscriptions = {}
        self.client_subscriptions = {}
        self.subscriber_lock = threading.RLock()
        self.callback_publisher = callback_publisher
        self._transport_owner = True

    def set_context(self, context):
        self.context = context
        self._log("tx trade bridge context ready")

    def start(self):
        if self.running:
            return self
        self.running = True
        txl = self._load_txl()
        self.tx = txl(self.ip, self.port, self.token)
        self.tx.start_tx()
        self.tx.start_txg(self.request_channel)
        self._log(
            "tx trade bridge started LTtx=%s:%s request_channel=%s"
            % (self.ip, self.port, self.request_channel)
        )
        return self

    def close(self):
        self.running = False
        publisher = self.callback_publisher
        self.callback_publisher = None
        if publisher is not None and publisher is not self:
            on_close = getattr(publisher, "_on_trade_bridge_closed", None)
            if callable(on_close):
                on_close()
        if not self._transport_owner:
            self.tx = None
            return
        unsubscribe_quote = self._get_callable("unsubscribe_quote")
        for subscribe_id in list(self.subscriptions):
            try:
                if callable(unsubscribe_quote):
                    unsubscribe_quote(subscribe_id)
            except Exception:
                pass
        self.subscriptions.clear()
        self.client_subscriptions.clear()
        tx = self.tx
        self.tx = None
        if tx is not None:
            try:
                tx.close()
            except Exception:
                pass
        self._log("tx trade bridge stopped")

    def run_forever(self, sleep_seconds=0.05):
        self.start()
        while self.running:
            self.poll(max_messages=100, timeout=sleep_seconds)

    def poll(self, max_messages=100, timeout=0):
        self.start()
        count = 0
        while self.running and count < max_messages:
            try:
                raw = self.tx.Q.get(timeout=timeout if count == 0 else 0)
            except Exception:
                break
            if raw is None:
                break
            self._handle_raw(raw)
            count += 1
        return count

    def _handle_raw(self, raw):
        received_at = time.time()
        msg = loads_message(raw)
        if not msg or msg.get("type") != "request":
            return
        request_id = msg.get("id")
        action = msg.get("action")
        client_id = msg.get("client_id") or msg.get("reply_channel")
        try:
            result = self._dispatch(action, msg.get("params") or {}, msg)
            response = pack_response(request_id, ok=True, result=result)
            if action not in ("cfquant.status", "cfquant.ping"):
                self._log("tx trade response_ready action=%s id=%s" % (action, request_id))
        except Exception as e:
            response = pack_response(request_id, ok=False, error=e)
            self._log("tx trade request_error action=%s id=%s error=%s" % (action, request_id, e))
        if client_id:
            self.tx.push("response", response, client_id)
            if action not in ("cfquant.status", "cfquant.ping"):
                self._log(
                    "tx trade response_sent action=%s id=%s client_id=%s total_ms=%.2f"
                    % (action, request_id, client_id, (time.time() - received_at) * 1000)
                )

    def _dispatch(self, action, params, msg):
        if action == "cfquant.ping":
            return {
                "pong": True,
                "ts": time.time(),
                "request_channel": self.request_channel,
                "bridge_id": self.bridge_id,
            }
        if action == "cfquant.status":
            return self._status()
        if action == "cfquant.cleanup_qmt_logs":
            return self._cleanup_qmt_userdata_logs(params)
        if action == "cfquant.query_info":
            return self._query_info(params)
        if action == "xttrader.subscribe":
            return self._subscribe_account(params, msg)
        if action == "xttrader.unsubscribe":
            return self._unsubscribe_account(params, msg)
        if action == "xttrader.query_stock_positions":
            return self._query_trade_detail(params, "position")
        if action == "xttrader.query_stock_orders":
            return self._query_trade_detail(params, "order")
        if action == "xttrader.query_stock_trades":
            return self._query_trade_detail(params, "deal")
        if action == "xttrader.query_stock_asset":
            return self._query_trade_detail(params, "account")
        if action == "xttrader.order_stock":
            return self._order_stock(params, msg)
        if action == "xttrader.order_stock_batch":
            return self._order_stock_batch(params, msg)
        if action == "xttrader.order_stock_async":
            return self._order_stock_async(params, msg)
        if action == "xttrader.cancel_order_stock":
            return self._cancel_order_stock(params)
        if action == "xttrader.cancel_order_stock_async":
            return self._cancel_order_stock_async(params, msg)
        if action == "xttrader.cancel_order_stock_sysid":
            return self._cancel_order_stock_sysid(params)
        if action == "xttrader.cancel_order_stock_sysid_async":
            return self._cancel_order_stock_sysid_async(params, msg)
        if action == "xtdata.get_market_data":
            return self._get_market_data(params)
        if action == "xtdata.get_market_data_ex":
            return self._get_market_data_ex(params)
        if action == "xtdata.get_full_tick":
            return self.context.get_full_tick(params.get("code_list", []))
        if action == "xtdata.subscribe_quote":
            return self._subscribe_quote(params, msg)
        if action == "xtdata.unsubscribe_quote":
            return self._unsubscribe_quote(params)
        if action == "xtdata.download_history_data":
            return self._download_history_data(params)
        if action == "xtdata.download_history_data2":
            return self._download_history_data2(params, msg)
        if action == "xtdata.get_financial_data":
            return self._get_financial_data(params)
        if action == "xtdata.get_raw_financial_data":
            return self._get_raw_financial_data(params)
        if action == "xtdata.download_financial_data":
            return self._download_financial_data(params)
        if action == "xtdata.get_instrument_detail":
            return self._get_instrument_detail(params)
        if action == "xtdata.get_stock_list_in_sector":
            return self.context.get_stock_list_in_sector(params.get("sector_name", ""))
        if action.startswith("xttrader."):
            return self._dispatch_xttrader_compat(action, params, msg)
        raise ValueError("unsupported action: %s" % action)

    def _status(self):
        status = {
            "bridge": type(self).__name__,
            "bridge_id": self.bridge_id,
            "running": self.running,
            "request_channel": self.request_channel,
            "account_id": self.account_id,
            "account_locked": self.account_locked,
            "account_type": self.account_type,
            "account_type_name": self._account_type_name(self.account_type).upper(),
            "account_subscribers": self._account_subscriber_status(),
            "context_ready": self.context is not None,
            "tx_ready": self.tx is not None,
            "subscriptions": len(self.subscriptions),
            "ts": time.time(),
        }
        callback_status = self._callback_publisher_status()
        status.update(callback_status)
        try:
            extra = self._status_extra()
            if extra:
                status.update(extra)
        except Exception as e:
            status["status_extra_error"] = str(e)
        return status

    def _status_extra(self):
        return {}

    def _callback_publisher_status(self):
        publisher = self.callback_publisher
        if publisher is None:
            return {
                "callback_account_bound": False,
                "callback_account_id": "",
                "callback_source": "",
            }
        valid = (
            publisher is not self
            and getattr(publisher, "tx", None) is self.tx
            and getattr(publisher, "bridge_id", None) == self.bridge_id
            and getattr(publisher, "account_locked", False) == self.account_locked
            and getattr(publisher, "account_type", None) == self.account_type
            and str(getattr(publisher, "account_id", "") or "").strip()
            == str(self.account_id or "").strip()
            and bool(getattr(publisher, "callback_account_bound", False))
            and str(getattr(publisher, "callback_account_id", "") or "").strip()
            == str(self.account_id or "").strip()
        )
        if not valid:
            return {
                "callback_account_bound": False,
                "callback_account_id": "",
                "callback_source": "",
            }
        return {
            "callback_account_bound": True,
            "callback_account_id": str(publisher.callback_account_id).strip(),
            "callback_source": "trade_model",
        }

    def _on_trade_bridge_closed(self):
        self.running = False
        self.tx = None
        self.callback_account_bound = False
        self.callback_account_id = ""

    def _normalize_account_id(self, value, label="account_id"):
        if value is None or isinstance(value, bool) or isinstance(value, (dict, list, tuple, set)):
            raise ValueError("%s must be non-empty" % label)
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("%s must be non-empty" % label)
        return normalized

    def _normalize_account_type_code(self, value):
        if isinstance(value, bool):
            raise ValueError("account_type must be a valid QMT account type")
        if isinstance(value, int):
            code = value
        elif isinstance(value, str):
            text = value.strip().upper()
            if text.lstrip("+-").isdigit():
                code = int(text)
            else:
                code = ACCOUNT_TYPE_NAMES.get(text)
        else:
            code = None
        if code not in ACCOUNT_TYPE_CODES:
            raise ValueError("account_type must be a valid QMT account type")
        return code

    def _identity_values(self, params, args=None, args_type_index=None, require_type=False):
        if not isinstance(params, dict):
            raise ValueError("%s params must be a dict" % (type(self).__name__,))
        containers = []
        account = params.get("account")
        if account is not None:
            if not isinstance(account, dict):
                raise ValueError("account must be a dict")
            containers.append(account)
        containers.append(params)
        account_ids = []
        account_types = []
        bridge_ids = []
        for container in containers:
            for name in ACCOUNT_ID_FIELDS:
                value = container.get(name)
                if value not in (None, ""):
                    account_ids.append((name, self._normalize_account_id(value)))
            for name in ACCOUNT_TYPE_FIELDS:
                value = container.get(name)
                if value not in (None, ""):
                    account_types.append((name, value))
            for name in BRIDGE_ID_FIELDS:
                value = container.get(name)
                if value not in (None, ""):
                    bridge_ids.append((name, str(value).strip()))

        if args is not None:
            if not isinstance(args, (list, tuple)) or not args:
                raise ValueError("native args must include account_id")
            account_ids.append(("args[0]", self._normalize_account_id(args[0])))
            if args_type_index is not None and len(args) > args_type_index:
                value = args[args_type_index]
                if value not in (None, ""):
                    account_types.append(("args[%s]" % args_type_index, value))

        if self.account_locked:
            if not account_ids:
                raise ValueError("locked bridge requires explicit account_id")
            normalized_ids = set(value for _, value in account_ids)
            if len(normalized_ids) > 1:
                raise ValueError("conflicting account_id declarations")
            if normalized_ids != {self._locked_account_id}:
                raise ValueError("locked bridge account_id mismatch")
            if require_type and not account_types:
                raise ValueError("locked bridge requires explicit account_type")
            try:
                type_codes = set(self._normalize_account_type_code(value) for _, value in account_types)
            except ValueError as exc:
                raise ValueError("locked bridge requires a valid account_type") from exc
            if len(type_codes) > 1:
                raise ValueError("conflicting account_type declarations")
            if type_codes and type_codes != {self._locked_account_type}:
                raise ValueError("locked bridge account_type mismatch")
            for _, value in bridge_ids:
                if str(value).strip() != str(self.bridge_id).strip():
                    raise ValueError("locked bridge bridge_id mismatch")
            return (
                self._locked_account_id,
                ACCOUNT_TYPE_CODES[self._locked_account_type],
                self._locked_account_type,
                True,
                bool(account_types),
            )

        account_id = account_ids[0][1] if account_ids else self.account_id
        if not account_id:
            raise ValueError("account_id is required")
        declared_type = account_types[0][1] if account_types else self.account_type
        try:
            type_code = self._normalize_account_type_code(declared_type)
        except ValueError:
            type_code = None
        type_name = (
            ACCOUNT_TYPE_CODES[type_code]
            if type_code is not None
            else self._account_type_name(declared_type).upper()
        )
        return account_id, type_name, type_code, bool(account_ids), bool(account_types)

    def _validate_result_account(self, row, account_id):
        returned = []
        for name in ACCOUNT_ID_FIELDS:
            value = self._get_value(row, name)
            if value not in (None, ""):
                returned.append(self._normalize_account_id(value, "returned account_id"))
        if returned and any(value != account_id for value in returned):
            raise ValueError(
                "result account mismatch requested=%s returned=%s"
                % (account_id, sorted(set(returned)))
            )

    def _cleanup_qmt_userdata_logs(self, params):
        params = params or {}
        retention_days = self._retention_days(params.get("retention_days"), default=5)
        dry_run = str(params.get("dry_run") or "").strip().lower() in ("1", "true", "yes", "on")
        log_dir, candidate_dirs, python_dir, entry_file = self._qmt_userdata_log_dir()
        result = {
            "bridge_id": self.bridge_id,
            "request_channel": self.request_channel,
            "retention_days": retention_days,
            "dry_run": dry_run,
            "entry_file": entry_file,
            "python_dir": python_dir,
            "log_dir": log_dir,
            "candidate_dirs": candidate_dirs,
            "exists": bool(log_dir and os.path.isdir(log_dir)),
            "scanned_files": 0,
            "kept_files": 0,
            "deleted_files": 0,
            "would_delete_files": 0,
            "failed_files": 0,
            "deleted_bytes": 0,
            "errors": [],
            "ts": time.time(),
        }
        if not result["exists"]:
            return result

        cutoff = time.time() - retention_days * 86400
        for current_root, dirs, files in os.walk(log_dir):
            for name in files:
                path = os.path.join(current_root, name)
                result["scanned_files"] += 1
                try:
                    stat_result = os.stat(path)
                    if stat_result.st_mtime >= cutoff:
                        result["kept_files"] += 1
                        continue
                    if dry_run:
                        result["would_delete_files"] += 1
                        result["deleted_bytes"] += stat_result.st_size
                    else:
                        os.remove(path)
                        result["deleted_files"] += 1
                        result["deleted_bytes"] += stat_result.st_size
                except Exception as e:
                    result["failed_files"] += 1
                    result["errors"].append("%s: %s" % (path, e))
        self._log(
            "qmt userdata log cleanup log_dir=%s retention_days=%s deleted=%s failed=%s dry_run=%s"
            % (log_dir, retention_days, result["deleted_files"], result["failed_files"], dry_run)
        )
        return result

    def _qmt_userdata_log_dir(self):
        entry_file = self.globals_dict.get("__file__") or ""
        if entry_file:
            entry_file = os.path.abspath(entry_file)
            python_dir = os.path.dirname(entry_file)
        else:
            python_dir = os.path.abspath(os.getcwd())
        candidate_dirs = []
        if os.path.basename(python_dir).lower() == "python":
            candidate_dirs.append(os.path.join(os.path.dirname(python_dir), "userdata", "log"))
        candidate_dirs.append(os.path.join(python_dir, "userdata", "log"))

        normalized = []
        seen = set()
        for path in candidate_dirs:
            path = os.path.abspath(path)
            key = path.lower()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(path)
        for path in normalized:
            if os.path.isdir(path):
                return path, normalized, python_dir, entry_file
        return normalized[0] if normalized else "", normalized, python_dir, entry_file

    def _retention_days(self, value, default=5):
        try:
            days = int(value)
        except Exception:
            days = int(default)
        if days < 1:
            days = 1
        if days > 3650:
            days = 3650
        return days

    def _query_info(self, params):
        return {
            "orders": self._query_trade_detail(params, "order"),
            "deals": self._query_trade_detail(params, "deal"),
            "positions": self._query_trade_detail(params, "position"),
            "accounts": self._query_trade_detail(params, "account"),
        }

    def _query_trade_detail(self, params, detail_type):
        account_id, account_type, account_type_code, _, _ = self._identity_values(
            params,
            require_type=self.account_locked,
        )
        func = self._get_callable("get_trade_detail_data")
        if not func:
            raise NotImplementedError("get_trade_detail_data not found")
        self._log(
            "query_trade_detail start account=%s account_type=%s detail_type=%s"
            % (account_id, account_type.lower(), detail_type.lower())
        )
        try:
            rows = func(account_id, account_type.lower(), detail_type.lower())
            if rows is None:
                raise RuntimeError(
                    "trade detail query returned None account=%s detail_type=%s"
                    % (account_id, detail_type)
                )
        except Exception as e:
            self._log(
                "query_trade_detail call failed account=%s detail_type=%s error=%s"
                % (account_id, detail_type, e)
            )
            raise

        result = []
        for index, row in enumerate(rows):
            try:
                if self.account_locked:
                    self._validate_result_account(row, account_id)
                result.append(self._format_trade_detail(row, detail_type, account_id=account_id))
            except Exception as e:
                self._log(
                    "query_trade_detail format failed detail_type=%s index=%s type=%s error=%s"
                    % (detail_type, index, type(row).__name__, e)
                )
                result.append({
                    "format_error": str(e),
                    "raw_type": type(row).__name__,
                })
        self._log(
            "query_trade_detail done detail_type=%s count=%s"
            % (detail_type, len(result))
        )
        return result

    def _subscribe_quote(self, params, msg):
        func = self._get_callable("subscribe_quote")
        if not func:
            raise NotImplementedError("subscribe_quote not found")
        stock_code = params.get("stock_code", "")
        period = params.get("period", "1d")
        dividend_type = params.get("dividend_type") or "none"
        client_id = msg.get("client_id") or msg.get("reply_channel") or ""
        holder = {"id": None}

        def callback(data):
            subscribe_id = holder.get("id")
            if subscribe_id is None:
                return
            self._send_event(client_id, "quote:%s" % subscribe_id, data, subscription_id=subscribe_id)

        subscribe_id = self._call_variants(
            func,
            [
                ((stock_code, period, params.get("start_time", ""), params.get("end_time", ""), params.get("count", 0)), {"callback": callback}),
                ((stock_code, period, dividend_type, "", callback), {}),
                ((stock_code, period, params.get("start_time", ""), params.get("end_time", ""), params.get("count", 0), callback), {}),
                ((stock_code, period, callback), {}),
            ],
        )
        holder["id"] = subscribe_id
        self._remember_subscription(subscribe_id, client_id, "quote", params)
        return {"subscribe_id": subscribe_id}

    def _remember_subscription(self, subscribe_id, client_id, kind, params):
        self.subscriptions[subscribe_id] = {
            "client_id": client_id,
            "kind": kind,
            "params": dict(params or {}),
        }
        if client_id:
            self.client_subscriptions.setdefault(client_id, set()).add(subscribe_id)

    def _unsubscribe_quote(self, params):
        subscribe_id = params.get("subscribe_id")
        func = self._get_callable("unsubscribe_quote")
        result = None
        try:
            if callable(func):
                result = func(subscribe_id)
        finally:
            info = self.subscriptions.pop(subscribe_id, None)
            if info:
                client_id = info.get("client_id")
                if client_id in self.client_subscriptions:
                    subscriptions = self.client_subscriptions.get(client_id, set())
                    subscriptions.discard(subscribe_id)
                    if not subscriptions:
                        self.client_subscriptions.pop(client_id, None)
        return result

    def _order_stock(self, params, msg):
        account_id, account_type, account_type_code, has_account_id, _ = self._identity_values(
            params,
            require_type=self.account_locked,
        )
        is_credit_account = account_type_code == 3 or account_type == "CREDIT"
        if is_credit_account and not has_account_id:
            raise ValueError("credit order requires an explicit account_id")
        if "optype" in params and "order_type" in params:
            first_order_type = self._normalize_order_type(params.get("optype"))
            second_order_type = self._normalize_order_type(params.get("order_type"))
            if first_order_type != second_order_type:
                raise ValueError("conflicting order_type declarations")
            order_type = first_order_type
        elif "optype" in params:
            order_type = self._normalize_order_type(params.get("optype"))
        elif "order_type" in params:
            order_type = self._normalize_order_type(params.get("order_type"))
        else:
            raise ValueError("order_type is required before passorder")
        if is_credit_account:
            account_id = self._normalize_credit_account_id(account_id)

        if order_type in CREDIT_OPERATION_CODES and not is_credit_account:
            raise ValueError(
                "credit operation code %s requires explicit CREDIT account type" % order_type
            )
        if is_credit_account:
            if order_type == 23:
                order_type = 33
            elif order_type == 24:
                order_type = 34

        passorder = self._get_callable("passorder")
        if not passorder:
            raise NotImplementedError("passorder not found")
        price_type = params.get("price_type", 11)
        order_remark = params.get("order_remark", msg.get("id", "tx_order"))
        qmt_order_type = params.get("qmt_order_type", 1101)
        stock_code = params.get("stock_code", params.get("code", ""))
        price = params.get("price", 0)
        order_volume = params.get("order_volume", params.get("num", 0))
        strategy_name = params.get("strategy_name", "1")
        quick_trade = params.get("quick_trade", 2)
        self._log(
            "order_stock submit account=%s stock=%s order_type=%s qmt_order_type=%s "
            "price_type=%s price=%s volume=%s quick_trade=%s remark=%s"
            % (
                account_id,
                stock_code,
                order_type,
                qmt_order_type,
                price_type,
                price,
                order_volume,
                quick_trade,
                order_remark,
            )
        )
        try:
            result = passorder(
                order_type,
                qmt_order_type,
                account_id,
                stock_code,
                price_type,
                price,
                order_volume,
                strategy_name,
                quick_trade,
                order_remark,
                self.context,
            )
        except TypeError:
            result = passorder(
                order_type,
                qmt_order_type,
                account_id,
                stock_code,
                price_type,
                price,
                order_volume,
                strategy_name,
                quick_trade,
                order_remark,
            )
        order_id = result if self._is_usable_order_id(result) else None
        if order_id is None:
            order_id = self._find_order_id(
                account_id,
                order_remark,
                strategy_name,
                params.get("find_order_wait", 2.0),
                account_type,
            )
        return {"request_result": result, "order_id": order_id, "order_remark": order_remark}

    def _order_stock_async(self, params, msg):
        async_params = dict(params)
        async_params.setdefault("find_order_wait", 0)
        result = self._order_stock(async_params, msg)
        data = {
            "seq": params.get("seq"),
            "account_id": (params.get("account") or {}).get("account_id", params.get("account_id", "")),
            "order_id": result.get("order_id", -1) if isinstance(result, dict) else result,
            "order_remark": result.get("order_remark", params.get("order_remark", "")) if isinstance(result, dict) else params.get("order_remark", ""),
        }
        self._send_trader_event(msg.get("client_id"), "on_order_stock_async_response", data)
        return result

    def _validate_order_type_before_submit(self, params):
        if "optype" in params and "order_type" in params:
            first = self._normalize_order_type(params.get("optype"))
            second = self._normalize_order_type(params.get("order_type"))
            if first != second:
                raise ValueError("conflicting order_type declarations")
            return first
        if "optype" in params:
            return self._normalize_order_type(params.get("optype"))
        if "order_type" in params:
            return self._normalize_order_type(params.get("order_type"))
        raise ValueError("order_type is required before passorder")

    def _order_stock_batch(self, params, msg):
        orders = params.get("orders") or []
        if not isinstance(orders, list) or not orders:
            raise ValueError("orders must be a non-empty list")
        common_account = params.get("account") or {}
        stop_on_error = bool(params.get("stop_on_error"))
        prepared = []
        for index, order in enumerate(orders):
            row = dict(params)
            row.pop("orders", None)
            row.update(order or {})
            if common_account and not row.get("account"):
                row["account"] = common_account
            if not row.get("order_remark"):
                row["order_remark"] = "%s_%s" % (params.get("order_remark") or msg.get("id", "batch_order"), index + 1)
            order_type = self._validate_order_type_before_submit(row)
            if self.account_locked:
                self._identity_values(row, require_type=True)
                _, account_type, account_type_code, _, _ = self._identity_values(row, require_type=True)
                if order_type in CREDIT_OPERATION_CODES and account_type_code != 3:
                    raise ValueError(
                        "credit operation code %s requires explicit CREDIT account type" % order_type
                    )
            prepared.append(row)
        results = []
        for index, row in enumerate(prepared):
            try:
                result = self._order_stock(row, msg)
                results.append({
                    "index": index,
                    "ok": True,
                    "stock_code": row.get("stock_code", row.get("code", "")),
                    "result": result,
                })
            except Exception as e:
                results.append({
                    "index": index,
                    "ok": False,
                    "stock_code": row.get("stock_code", row.get("code", "")),
                    "error": str(e),
                })
                if stop_on_error:
                    break
        return {
            "total": len(orders),
            "submitted": len([item for item in results if item.get("ok")]),
            "failed": len([item for item in results if not item.get("ok")]),
            "results": results,
        }

    def _cancel_order_stock(self, params):
        account_id, account_type, _, _, _ = self._identity_values(
            params,
            require_type=self.account_locked,
        )
        cancel_func = self._get_callable("cancel")
        if not cancel_func:
            raise NotImplementedError("cancel not found")
        order_id = str(params.get("order_id", ""))
        if not order_id:
            raise ValueError("order_id is required")
        if self.account_locked:
            native_account_type = account_type
        else:
            # Keep the unlocked bridge's historical native-type spelling. The
            # identity validator normalizes only its internal comparison value;
            # QMT callers may still rely on an explicitly supplied ``STOCK``.
            account = params.get("account") or {}
            declared_account_type = account.get("account_type") or params.get("account_type")
            native_account_type = self._account_type_name(declared_account_type)
        order = self._find_order_for_cancel(account_id, native_account_type, order_id)
        status = self._get_value(order, "order_status") if order else None
        try:
            status_code = int(status)
        except (TypeError, ValueError):
            status_code = None
        if status_code not in {48, 49, 50, 55}:
            return {
                "cancel_result": -1,
                "request_result": False,
                "order_id": order_id,
                "reason": "order_not_active_or_status_unknown",
                "order_status": status,
            }
        result = cancel_func(order_id, account_id, native_account_type, self.context)
        return {"cancel_result": 0 if result else -1, "request_result": result, "order_id": order_id}

    def _find_order_for_cancel(self, account_id, account_type, order_id):
        orders = self._query_trade_detail(
            {
                "account_id": account_id,
                "account_type": account_type,
            },
            "order",
        )
        expected = str(order_id)
        aliases = (
            "order_sysid",
            "order_id",
            "m_strOrderSysID",
            "m_nOrderID",
            "m_strOrderID",
        )
        for order in orders or []:
            if any(str(self._get_value(order, name)) == expected for name in aliases):
                return order
        return None

    def _cancel_order_stock_async(self, params, msg):
        result = self._cancel_order_stock(params)
        data = {
            "seq": params.get("seq"),
            "account_id": (params.get("account") or {}).get("account_id", params.get("account_id", "")),
            "order_id": params.get("order_id"),
            "cancel_result": result.get("cancel_result", -1) if isinstance(result, dict) else result,
        }
        self._send_trader_event(msg.get("client_id"), "on_cancel_order_stock_async_response", data)
        return result

    def _cancel_order_stock_sysid(self, params):
        row = dict(params)
        row["order_id"] = params.get("sysid", params.get("order_id", ""))
        result = self._cancel_order_stock(row)
        result["market"] = params.get("market")
        result["sysid"] = params.get("sysid")
        return result

    def _cancel_order_stock_sysid_async(self, params, msg):
        result = self._cancel_order_stock_sysid(params)
        data = {
            "seq": params.get("seq"),
            "account_id": (params.get("account") or {}).get("account_id", params.get("account_id", "")),
            "order_id": params.get("sysid", params.get("order_id")),
            "cancel_result": result.get("cancel_result", -1) if isinstance(result, dict) else result,
        }
        self._send_trader_event(msg.get("client_id"), "on_cancel_order_stock_async_response", data)
        return result

    def _dispatch_xttrader_compat(self, action, params, msg):
        method = action.split(".", 1)[1]
        if method in CREDIT_QUERY_NATIVE_METHODS:
            return self._dispatch_credit_query(method, params)
        if method in CREDIT_NATIVE_METHODS:
            return self._dispatch_credit_native(method, params)
        if method == "query_com_fund":
            rows = self._query_trade_detail(params, "account")
            return rows[0] if rows else {}
        if method == "query_com_position":
            return self._query_trade_detail(params, "position")
        if method == "query_position_statistics":
            # Production FUTURE QMT builds do not universally expose
            # query_position_statistics/get_position_statistics. The native
            # account-scoped trade detail API is available and returns the
            # broker position rows; preserve its direction/today/yesterday
            # fields instead of fabricating an empty statistics result.
            _account_id, _account_type, account_type_code, _, _ = self._identity_values(
                params,
                require_type=self.account_locked,
            )
            if int(account_type_code) != 1:
                raise ValueError(
                    "query_position_statistics fallback requires FUTURE account"
                )
            return self._query_trade_detail(params, "position")
        if method == "query_stock_asset_async":
            return self._query_trade_detail(params, "account")
        if method == "query_stock_orders_async":
            return self._query_trade_detail(params, "order")
        if method == "query_stock_trades_async":
            return self._query_trade_detail(params, "deal")
        if method == "query_stock_positions_async":
            return self._query_trade_detail(params, "position")
        return self._generic_xttrader_call(method, params)

    def _dispatch_credit_query(self, method, params):
        self._validate_credit_query_params(params)
        account_id = self._require_credit_account(params)
        self._validate_optional_credit_account(params, account_id)
        native_name = CREDIT_QUERY_NATIVE_METHODS[method]
        func = self._get_callable(native_name)
        if not func:
            raise NotImplementedError(
                "xttrader.%s requires QMT callable: %s" % (method, native_name)
            )

        if method == "query_credit_detail":
            result = func(account_id, "credit", "account")
        elif method in ("query_credit_subjects", "query_credit_assure"):
            result = func(account_id)
        elif method == "query_credit_slo_code":
            result = func(account_id)
        else:
            result = func(account_id, "CREDIT")
        return self._format_credit_result(result, account_id, native_name)

    def _validate_credit_query_params(self, params):
        if not isinstance(params, dict):
            raise ValueError("credit query params must be a dict")
        args = params.get("args")
        if args not in (None, [], ()):
            raise ValueError("credit query does not accept positional args")
        kwargs = params.get("kwargs")
        if kwargs not in (None, {}):
            raise ValueError("credit query does not accept kwargs")

    def _dispatch_credit_native(self, method, params):
        if not isinstance(params, dict):
            raise ValueError("credit query params must be a dict")
        if method == "get_trade_detail_data":
            args = params.get("args")
            if self._is_future_trade_detail_request(args):
                return self._dispatch_future_account_detail(params, args)
            if not self._is_credit_account_detail_request(args, params):
                return self._generic_xttrader_call(method, params)
            account_id, call_args = self._validate_credit_native_args(method, args, params)
        else:
            account_id, call_args = self._validate_credit_native_args(
                method, params.get("args"), params
            )

        func = self._get_callable(method)
        if not func:
            raise NotImplementedError("xttrader.%s requires QMT callable: %s" % (method, method))
        result = func(*call_args)
        return self._format_credit_result(result, account_id, method)

    def _dispatch_future_account_detail(self, params, args):
        if not isinstance(args, (list, tuple)) or len(args) != 3:
            raise ValueError("get_trade_detail_data FUTURE requires exactly 3 positional args")
        if self._normalize_credit_detail_type(args[2]) != "account":
            raise ValueError("get_trade_detail_data FUTURE detail type must be ACCOUNT")
        kwargs = params.get("kwargs")
        if kwargs not in (None, {}):
            raise ValueError("get_trade_detail_data FUTURE does not accept kwargs")

        account_id, _account_type, account_type_code, _, _ = self._identity_values(
            params,
            args=args,
            args_type_index=1,
            require_type=self.account_locked,
        )
        if int(account_type_code) != 1:
            raise ValueError("get_trade_detail_data FUTURE requires account type 1/FUTURE")

        query_params = dict(params)
        query_params["account_id"] = account_id
        query_params["account_type"] = "FUTURE"
        return self._query_trade_detail(query_params, "account")

    def _require_credit_account(self, params):
        if not isinstance(params, dict):
            raise ValueError("credit query params must be a dict")
        if self.account_locked:
            self._identity_values(params, require_type=True)
        account = params.get("account")
        if not isinstance(account, dict):
            raise ValueError("credit query requires explicit account dict")
        account_id = self._normalize_credit_account_id(account.get("account_id"))
        if not self._is_credit_account_type(account.get("account_type")):
            raise ValueError("credit query requires account.account_type=3/CREDIT")
        return account_id

    def _validate_credit_native_args(self, method, args, params):
        if not isinstance(args, (list, tuple)):
            raise ValueError("xttrader.%s requires positional args" % method)
        expected_count = {
            "get_trade_detail_data": 3,
            "get_assure_contract": 1,
            "get_enable_short_contract": 1,
            "get_unclosed_compacts": 2,
            "get_closed_compacts": 2,
        }[method]
        if len(args) != expected_count:
            raise ValueError(
                "xttrader.%s requires exactly %s positional args" % (method, expected_count)
            )

        self._identity_values(
            params,
            args=args,
            args_type_index=(
                1
                if method in ("get_trade_detail_data", "get_unclosed_compacts", "get_closed_compacts")
                else None
            ),
            require_type=self.account_locked,
        )

        account_id = self._normalize_credit_account_id(args[0])
        if method == "get_trade_detail_data":
            if not self._is_credit_account_type(args[1]):
                raise ValueError("get_trade_detail_data credit account type must be 3/CREDIT")
            if self._normalize_credit_detail_type(args[2]) != "account":
                raise ValueError("get_trade_detail_data credit detail type must be ACCOUNT")
        elif method in ("get_unclosed_compacts", "get_closed_compacts"):
            if not self._is_credit_account_type(args[1]):
                raise ValueError("%s requires CREDIT account type" % method)

        kwargs = params.get("kwargs")
        if kwargs not in (None, {}):
            raise ValueError("xttrader.%s does not accept kwargs" % method)
        self._validate_optional_credit_account(params, account_id)
        return account_id, self._canonical_credit_native_args(method, args, account_id)

    def _canonical_credit_native_args(self, method, args, account_id):
        if method == "get_trade_detail_data":
            return (account_id, "CREDIT", "ACCOUNT")
        if method in ("get_assure_contract", "get_enable_short_contract"):
            return (account_id,)
        return (account_id, "CREDIT")

    def _normalize_order_type(self, value):
        if isinstance(value, bool):
            raise ValueError("order_type must be an integer, buy, or sell")
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            lowered = text.lower()
            if lowered == "buy":
                return 23
            if lowered == "sell":
                return 24
            if text and text.lstrip("+-").isdigit():
                return int(text)
        raise ValueError("order_type must be an integer, buy, or sell")

    def _order_account_type_name(self, account_type):
        if self._is_credit_account_type(account_type):
            return "CREDIT"
        if account_type in (None, ""):
            return "STOCK"
        if isinstance(account_type, str):
            text = account_type.strip()
            if text.lower() in ("2", "stock"):
                return "STOCK"
            return text.upper()
        return self._account_type_name(account_type).upper()

    def _validate_optional_credit_account(self, params, account_id):
        if "account" in params:
            declared_id = self._require_credit_account(params)
            if declared_id != account_id:
                raise ValueError(
                    "credit query account mismatch args=%s account=%s"
                    % (account_id, declared_id)
                )
        if "account_id" in params and params.get("account_id") not in (None, ""):
            declared_id = self._normalize_credit_account_id(params.get("account_id"))
            if declared_id != account_id:
                raise ValueError(
                    "credit query account mismatch args=%s account_id=%s"
                    % (account_id, declared_id)
                )
        if "account_type" in params and params.get("account_type") not in (None, ""):
            if not self._is_credit_account_type(params.get("account_type")):
                raise ValueError("credit query requires account_type=3/CREDIT")

    def _is_credit_account_detail_request(self, args, params):
        if isinstance(args, (list, tuple)) and len(args) >= 2:
            if not self._is_credit_account_type(args[1]):
                return False
            if len(args) == 3 and self._normalize_credit_detail_type(args[2]) in {
                "position",
                "order",
                "deal",
            }:
                return False
            return True
        account = (params or {}).get("account")
        return isinstance(account, dict) and self._is_credit_account_type(account.get("account_type"))

    def _is_future_trade_detail_request(self, args):
        if not isinstance(args, (list, tuple)) or len(args) < 2:
            return False
        try:
            return self._normalize_account_type_code(args[1]) == 1
        except ValueError:
            return False

    def _format_credit_result(self, result, account_id, native_name):
        if result is None:
            raise RuntimeError(
                "credit query %s returned None for account=%s" % (native_name, account_id)
            )
        if not isinstance(result, (list, tuple)):
            raise RuntimeError(
                "credit query %s must return a list, got %s"
                % (native_name, type(result).__name__)
            )
        if not result:
            return []
        formatted = []
        for index, row in enumerate(result):
            try:
                formatted.append(self._format_credit_record(row, account_id, native_name))
            except Exception as e:
                raise RuntimeError(
                    "credit query %s result[%s] conversion failed: %s"
                    % (native_name, index, e)
                )
        return formatted

    def _format_credit_record(self, row, account_id, native_name):
        if row is None:
            raise ValueError("result record is None")
        if isinstance(row, dict):
            fields = {}
            for name, value in row.items():
                converted = self._credit_plain_value(value)
                if converted is not _CREDIT_UNAVAILABLE:
                    fields[str(name)] = converted
                elif str(name) in ("account_id", "m_strAccountID"):
                    raise ValueError("account identity field is not serializable")
            if not fields:
                raise ValueError("result record has no serializable fields")
        else:
            fields = self._credit_object_fields(row)

        self._validate_credit_record_account(fields, account_id)
        self._add_credit_aliases(fields, account_id, native_name)
        return fields

    def _credit_object_fields(self, row):
        try:
            names = dir(row)
        except Exception as e:
            raise ValueError("result record attributes are not readable: %s" % e)

        selected = set(name for name in names if str(name).startswith("m_"))
        selected.update(name for name in ("account_id", "stock_code", "market") if name in names)
        fields = {}
        unreadable = []
        for name in sorted(selected):
            try:
                value = getattr(row, name)
            except Exception as e:
                if name in ("account_id", "m_strAccountID"):
                    raise ValueError("account identity field is not readable: %s" % e)
                unreadable.append("%s: %s" % (name, e))
                continue
            if callable(value):
                continue
            converted = self._credit_plain_value(value)
            if converted is _CREDIT_UNAVAILABLE:
                if name in ("account_id", "m_strAccountID"):
                    raise ValueError("account identity field is not serializable")
                continue
            fields[name] = converted
        if not fields:
            if unreadable:
                raise ValueError("result record fields are not readable: %s" % ", ".join(unreadable))
            raise ValueError("result record has no readable m_* fields")
        return fields

    def _validate_credit_record_account(self, fields, account_id):
        for name in ("account_id", "m_strAccountID"):
            if name not in fields:
                continue
            value = fields.get(name)
            if value in (None, ""):
                continue
            record_id = self._normalize_credit_account_id(value)
            if record_id != account_id:
                raise ValueError(
                    "result account mismatch requested=%s returned=%s"
                    % (account_id, record_id)
                )

    def _add_credit_aliases(self, fields, account_id, native_name):
        fields["account_id"] = account_id

        market = fields.get("market")
        if market in (None, ""):
            market = fields.get("m_strExchangeID")
        if market not in (None, ""):
            fields["market"] = market

        stock_code = fields.get("stock_code")
        if stock_code in (None, ""):
            instrument_id = fields.get("m_strInstrumentID")
            if instrument_id not in (None, ""):
                if market not in (None, ""):
                    stock_code = "%s.%s" % (instrument_id, market)
                else:
                    stock_code = instrument_id
        if stock_code not in (None, ""):
            fields["stock_code"] = stock_code

        if native_name == "get_enable_short_contract":
            if "m_nEnableAmount" in fields:
                fields["enable_amount"] = fields["m_nEnableAmount"]
            if "m_eQuerySloType" in fields:
                fields["query_slo_type"] = fields["m_eQuerySloType"]
        elif native_name in ("get_unclosed_compacts", "get_closed_compacts"):
            if "m_strCompactId" in fields:
                fields["compact_id"] = fields["m_strCompactId"]
            if "m_strEntrustNo" in fields:
                fields["broker_order_id"] = fields["m_strEntrustNo"]

    def _credit_plain_value(self, value):
        if isinstance(value, str) and value.strip() == "<CanNotConvert>":
            return _CREDIT_UNAVAILABLE
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return str(value)
        if isinstance(value, (list, tuple, set)):
            converted = []
            for item in value:
                plain = self._credit_plain_value(item)
                if plain is not _CREDIT_UNAVAILABLE:
                    converted.append(plain)
            return converted
        if isinstance(value, dict):
            converted = {}
            for key, item in value.items():
                plain = self._credit_plain_value(item)
                if plain is not _CREDIT_UNAVAILABLE:
                    converted[str(key)] = plain
            return converted
        try:
            item = getattr(value, "item", None)
            if callable(item):
                return self._credit_plain_value(item())
        except Exception:
            return _CREDIT_UNAVAILABLE
        try:
            plain = self._plain_value(value)
        except Exception:
            return _CREDIT_UNAVAILABLE
        if isinstance(plain, str) and plain.strip() == "<CanNotConvert>":
            return _CREDIT_UNAVAILABLE
        if isinstance(plain, (str, bool, int, float, list, dict)):
            return plain
        return _CREDIT_UNAVAILABLE

    def _normalize_credit_account_id(self, value):
        if value is None or isinstance(value, bool) or isinstance(value, (dict, list, tuple, set)):
            raise ValueError("credit account_id must be non-empty")
        account_id = str(value).strip()
        if not account_id:
            raise ValueError("credit account_id must be non-empty")
        return account_id

    def _is_credit_account_type(self, value):
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return value == 3
        if isinstance(value, str):
            return value.strip().lower() in ("3", "credit")
        return False

    def _normalize_credit_detail_type(self, value):
        if isinstance(value, str):
            return value.strip().lower()
        return str(value).strip().lower()

    def _generic_xttrader_call(self, method, params):
        if self.account_locked and method not in XTTRADER_COMPAT_CANDIDATES:
            raise ValueError("locked bridge rejects unknown xttrader generic request: %s" % method)
        candidates = XTTRADER_COMPAT_CANDIDATES.get(method, (method,))
        func = self._get_callable(*candidates)
        if not func:
            raise NotImplementedError(
                "xttrader.%s requires QMT callable: %s"
                % (method, ", ".join(candidates))
            )
        args = list(params.get("args") or [])
        kwargs = dict(params.get("kwargs") or {})
        if self.account_locked:
            self._identity_values(
                params,
                args=args if not params.get("account") and not params.get("account_id") else None,
                args_type_index=1 if method == "get_trade_detail_data" else None,
                require_type=True,
            )
        account = params.get("account") or {}
        account_id = account.get("account_id") or params.get("account_id") or self.account_id
        account_type_value = account.get("account_type") or params.get("account_type")
        account_type = self._account_type_name(account_type_value)
        variants = []
        if account:
            variants.extend([
                ((account,) + tuple(args), kwargs),
                ((account_id,) + tuple(args), kwargs),
                ((account_id, account_type.lower()) + tuple(args), kwargs),
                ((account_id, account_type) + tuple(args), kwargs),
                ((account_id, account_type_value) + tuple(args), kwargs),
            ])
        variants.extend([
            (tuple(args), kwargs),
            ((params,), {}),
        ])
        return self._call_variants(func, variants)

    def _get_market_data(self, params):
        func = self._get_callable("get_market_data")
        if not func:
            return self._get_market_data_ex(params)
        return func(
            params.get("field_list", []),
            params.get("stock_list", []),
            params.get("start_time", ""),
            params.get("end_time", ""),
            params.get("skip_paused", params.get("fill_data", True)),
            params.get("period", "1d"),
            params.get("dividend_type", "none"),
            params.get("count", -1),
        )

    def _get_market_data_ex(self, params):
        func = self._get_callable("get_market_data_ex")
        if not func:
            raise NotImplementedError("get_market_data_ex not found")
        return func(
            params.get("field_list", []),
            params.get("stock_list", []),
            params.get("period", "1d"),
            params.get("start_time", ""),
            params.get("end_time", ""),
            params.get("count", -1),
            params.get("dividend_type", "none"),
            params.get("fill_data", True),
        )

    def _download_history_data(self, params):
        func = self._get_callable("download_history_data", "down_history_data")
        if not func:
            raise NotImplementedError("download_history_data not found")
        variants = []
        incrementally = params.get("incrementally")
        if incrementally is not None:
            variants.append((
                (
                    params.get("stock_code", ""),
                    params.get("period", "1d"),
                    params.get("start_time", ""),
                    params.get("end_time", ""),
                    incrementally,
                ),
                {},
            ))
        variants.append((
            (
                params.get("stock_code", ""),
                params.get("period", "1d"),
                params.get("start_time", ""),
                params.get("end_time", ""),
            ),
            {},
        ))
        return self._call_variants(func, variants)

    def _download_history_data2(self, params, msg):
        func = self._get_callable("download_history_data2", "down_history_data2")
        if not func:
            raise NotImplementedError("download_history_data2 not found")
        client_id = msg.get("client_id")
        callback_event = params.get("callback_event")

        def callback(data):
            if callback_event and client_id:
                self._send_event(client_id, callback_event, data)

        callback_func = callback if callback_event else None
        variants = []
        incrementally = params.get("incrementally")
        if incrementally is not None:
            variants.append((
                (
                    params.get("stock_list", params.get("code_list", [])),
                    params.get("period", "1d"),
                    params.get("start_time", ""),
                    params.get("end_time", ""),
                    callback_func,
                    incrementally,
                ),
                {},
            ))
        variants.append((
            (
                params.get("stock_list", params.get("code_list", [])),
                params.get("period", "1d"),
                params.get("start_time", ""),
                params.get("end_time", ""),
                callback_func,
            ),
            {},
        ))
        return self._call_variants(func, variants)

    def _get_instrument_detail(self, params):
        func = self._get_callable("get_instrument_detail")
        if not func:
            raise NotImplementedError("get_instrument_detail not found")
        return func(params.get("stock_code", ""))

    def _get_financial_data(self, params):
        func = self._get_callable("get_financial_data")
        if not func:
            raise NotImplementedError("get_financial_data not found")
        fields = params.get("field_list") or []
        stock_list = params.get("stock_list", params.get("code_list", []))
        table_list = params.get("table_list") or []
        start_time = params.get("start_time", params.get("start_date", ""))
        end_time = params.get("end_time", params.get("end_date", ""))
        report_type = params.get("report_type") or ("announce_time" if fields else "report_time")
        variants = []
        if fields:
            variants.append(((fields, stock_list, start_time, end_time, report_type), {}))
            variants.append(((fields, stock_list, start_time, end_time), {}))
        if not variants:
            raise ValueError("field_list is required")
        return self._call_variants(func, variants)

    def _get_raw_financial_data(self, params):
        func = self._get_callable("get_raw_financial_data")
        if not func:
            raise NotImplementedError("get_raw_financial_data not found")
        fields = params.get("field_list") or []
        stock_list = params.get("stock_list", params.get("code_list", []))
        if not fields:
            raise ValueError("field_list is required for get_raw_financial_data")
        return self._call_variants(func, [
            ((
                fields,
                stock_list,
                params.get("start_time", params.get("start_date", "")),
                params.get("end_time", params.get("end_date", "")),
                params.get("report_type") or "announce_time",
            ), {}),
            ((
                fields,
                stock_list,
                params.get("start_time", params.get("start_date", "")),
                params.get("end_time", params.get("end_date", "")),
            ), {}),
        ])

    def _download_financial_data(self, params):
        stock_list = params.get("stock_list", params.get("code_list", []))
        table_list = params.get("table_list") or []
        start_time = params.get("start_time", params.get("start_date", ""))
        end_time = params.get("end_time", params.get("end_date", ""))
        callback_event = params.get("callback_event")
        func = self._get_callable("download_financial_data2", "down_financial_data2")
        if func:
            variants = [
                ((stock_list, table_list, start_time, end_time, None), {}),
                ((stock_list, table_list, start_time, end_time), {}),
            ]
            return self._call_variants(func, variants)
        func = self._get_callable("download_financial_data", "down_financial_data")
        if not func:
            raise NotImplementedError("download_financial_data not found")
        return self._call_variants(func, [
            ((stock_list, table_list), {}),
            ((stock_list,), {}),
        ])

    def _subscribe_account(self, params, msg=None):
        account_id, account_type, _, _, _ = self._identity_values(
            params,
            require_type=self.account_locked,
        )
        client_id = ""
        if msg:
            client_id = msg.get("client_id") or msg.get("reply_channel") or ""
        if self.account_locked and self.context is None:
            raise RuntimeError("locked bridge account binding requires a QMT context")
        if self.context is not None:
            try:
                self.context.set_account(account_id, account_type.upper())
            except Exception:
                # The local subscription maps are deliberately updated only
                # after QMT accepts the account binding.
                self.context.set_account(account_id)
        if not self.account_locked:
            self.account_id = account_id
        if client_id:
            with self.subscriber_lock:
                self.account_subscribers.setdefault(account_id, set()).add(client_id)
                self.client_accounts.setdefault(client_id, set()).add(account_id)
            account_routing.subscribe(self.bridge_id, account_id, client_id)
        self._log("account subscribed account=%s client_id=%s" % (account_id, client_id or "-"))
        return 0

    def _unsubscribe_account(self, params, msg=None):
        if self.account_locked:
            account_id, _, _, _, _ = self._identity_values(params, require_type=True)
        else:
            account = params.get("account") or {}
            account_id = account.get("account_id") or params.get("account_id")
        client_id = ""
        if msg:
            client_id = msg.get("client_id") or msg.get("reply_channel") or ""
        if account_id:
            account_id = str(account_id).strip()
        with self.subscriber_lock:
            if account_id and client_id:
                subscribers = self.account_subscribers.get(account_id)
                if subscribers:
                    subscribers.discard(client_id)
                    if not subscribers:
                        self.account_subscribers.pop(account_id, None)
                accounts = self.client_accounts.get(client_id)
                if accounts:
                    accounts.discard(account_id)
                    if not accounts:
                        self.client_accounts.pop(client_id, None)
            elif client_id:
                accounts = self.client_accounts.pop(client_id, set())
                for item in accounts:
                    subscribers = self.account_subscribers.get(item)
                    if subscribers:
                        subscribers.discard(client_id)
                        if not subscribers:
                            self.account_subscribers.pop(item, None)
        account_routing.unsubscribe(self.bridge_id, account_id=account_id, client_id=client_id)
        if not self.account_locked and account_id and account_id == self.account_id:
            self.account_id = ""
        self._log("account unsubscribed account=%s client_id=%s" % (account_id or "-", client_id or "-"))
        return 0

    def _format_trade_detail(self, obj, detail_type, account_id=""):
        detail_type = str(detail_type).lower()
        resolved_account_id = self._first_value(obj, ("account_id", "m_strAccountID")) or account_id
        if detail_type == "order":
            return {
                "account_id": resolved_account_id,
                "stock_code": self._stock_code(obj),
                "market": self._get_value(obj, "m_strExchangeID"),
                "instrument_name": self._get_value(obj, "m_strInstrumentName"),
                "order_remark": self._first_value(obj, (
                    "order_remark",
                    "remark",
                    "m_strRemark",
                )),
                "order_id": self._first_value(obj, (
                    "order_id",
                    "m_nOrderID",
                    "m_strOrderID",
                )),
                "order_sysid": self._first_value(obj, (
                    "order_sysid",
                    "sysid",
                    "m_strOrderSysID",
                )),
                "order_type": self._first_value(obj, (
                    "order_type",
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                    "direction",
                    "m_nOrderType",
                )),
                "broker_operation_code": self._first_value(obj, (
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                    "m_nOrderType",
                )),
                "direction": self._first_value(obj, (
                    "direction",
                    "order_type",
                    "m_nOffsetFlag",
                    "m_nDirection",
                    "m_nOrderType",
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                )),
                "strategy_name": self._first_value(obj, (
                    "strategy_name",
                    "m_strStrategyName",
                    "m_strStrategyID",
                    "m_strStrategy",
                )),
                "price_type": self._first_value(obj, (
                    "price_type",
                    "price_type_name",
                    "m_nPriceType",
                    "m_nOrderPriceType",
                )),
                "price": self._first_value(obj, (
                    "price",
                    "order_price",
                    "m_dPrice",
                    "m_dOrderPrice",
                    "m_dLimitPrice",
                )),
                "order_time": self._first_value(obj, (
                    "order_time",
                    "entrust_time",
                    "insert_time",
                    "m_strOrderTime",
                    "m_strEntrustTime",
                    "m_strInsertTime",
                    "m_nOrderTime",
                    "m_nEntrustTime",
                    "m_nInsertTime",
                )),
                "order_date": self._first_value(obj, (
                    "order_date",
                    "entrust_date",
                    "m_strOrderDate",
                    "m_strEntrustDate",
                    "m_strTradingDay",
                    "m_nOrderDate",
                    "m_nEntrustDate",
                    "m_strInsertDate",
                )),
                "offset_flag": self._get_value(obj, "m_nOffsetFlag"),
                "order_volume": self._get_value(obj, "m_nVolumeTotalOriginal"),
                "traded_price": self._get_value(obj, "m_dTradedPrice"),
                "traded_volume": self._get_value(obj, "m_nVolumeTraded"),
                "trade_amount": self._get_value(obj, "m_dTradeAmount"),
                "order_status": self._first_value(obj, (
                    "order_status",
                    "status",
                    "m_nOrderStatus",
                    "m_strOrderStatus",
                    "m_nOrderState",
                    "m_strStatus",
                )),
                "m_strInstrumentID": self._get_value(obj, "m_strInstrumentID"),
                "m_strExchangeID": self._get_value(obj, "m_strExchangeID"),
                "m_strInstrumentName": self._get_value(obj, "m_strInstrumentName"),
                "m_nOrderType": self._get_value(obj, "m_nOrderType"),
                "m_nDirection": self._get_value(obj, "m_nDirection"),
                "m_nPriceType": self._get_value(obj, "m_nPriceType"),
                "m_nOrderPriceType": self._get_value(obj, "m_nOrderPriceType"),
                "m_dPrice": self._get_value(obj, "m_dPrice"),
                "m_dOrderPrice": self._get_value(obj, "m_dOrderPrice"),
                "m_dLimitPrice": self._get_value(obj, "m_dLimitPrice"),
                "m_nOffsetFlag": self._get_value(obj, "m_nOffsetFlag"),
                "m_nVolumeTotalOriginal": self._get_value(obj, "m_nVolumeTotalOriginal"),
                "m_dTradedPrice": self._get_value(obj, "m_dTradedPrice"),
                "m_nVolumeTraded": self._get_value(obj, "m_nVolumeTraded"),
                "m_dTradeAmount": self._get_value(obj, "m_dTradeAmount"),
                "m_strRemark": self._get_value(obj, "m_strRemark"),
                "m_strOrderSysID": self._get_value(obj, "m_strOrderSysID"),
                "m_nOrderID": self._get_value(obj, "m_nOrderID"),
                "m_strOrderID": self._get_value(obj, "m_strOrderID"),
                "m_nOrderStatus": self._get_value(obj, "m_nOrderStatus"),
                "m_strOrderStatus": self._get_value(obj, "m_strOrderStatus"),
                "m_nOrderState": self._get_value(obj, "m_nOrderState"),
                "m_strStatus": self._get_value(obj, "m_strStatus"),
                "m_strOrderTime": self._get_value(obj, "m_strOrderTime"),
                "m_strEntrustTime": self._get_value(obj, "m_strEntrustTime"),
                "m_strInsertTime": self._get_value(obj, "m_strInsertTime"),
                "m_strAccountID": self._get_value(obj, "m_strAccountID"),
                "m_nOrderTime": self._get_value(obj, "m_nOrderTime"),
                "m_nEntrustTime": self._get_value(obj, "m_nEntrustTime"),
                "m_nInsertTime": self._get_value(obj, "m_nInsertTime"),
                "m_strOrderDate": self._get_value(obj, "m_strOrderDate"),
                "m_strEntrustDate": self._get_value(obj, "m_strEntrustDate"),
                "m_strTradingDay": self._get_value(obj, "m_strTradingDay"),
                "m_strInsertDate": self._get_value(obj, "m_strInsertDate"),
            }
        if detail_type == "deal":
            return {
                "account_id": resolved_account_id,
                "stock_code": self._stock_code(obj),
                "market": self._get_value(obj, "m_strExchangeID"),
                "instrument_name": self._get_value(obj, "m_strInstrumentName"),
                "order_remark": self._first_value(obj, (
                    "order_remark",
                    "remark",
                    "m_strRemark",
                )),
                "order_id": self._first_value(obj, (
                    "order_id",
                    "m_nOrderID",
                    "m_strOrderID",
                )),
                "order_sysid": self._first_value(obj, (
                    "order_sysid",
                    "sysid",
                    "m_strOrderSysID",
                )),
                "trade_id": self._first_value(obj, (
                    "trade_id",
                    "traded_id",
                    "m_strTradeID",
                    "m_nTradeID",
                )),
                "deal_id": self._first_value(obj, (
                    "deal_id",
                    "m_strDealID",
                    "m_nDealID",
                )),
                "order_type": self._first_value(obj, (
                    "order_type",
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                    "direction",
                    "m_nOrderType",
                )),
                "broker_operation_code": self._first_value(obj, (
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                    "m_nOrderType",
                )),
                "direction": self._first_value(obj, (
                    "direction",
                    "order_type",
                    "m_nOffsetFlag",
                    "m_nDirection",
                    "m_nOrderType",
                    "broker_operation_code",
                    "operation_code",
                    "optype",
                )),
                "strategy_name": self._first_value(obj, (
                    "strategy_name",
                    "m_strStrategyName",
                    "m_strStrategyID",
                    "m_strStrategy",
                )),
                "price_type": self._first_value(obj, (
                    "price_type",
                    "price_type_name",
                    "m_nPriceType",
                )),
                "trade_time": self._first_value(obj, (
                    "trade_time",
                    "deal_time",
                    "m_strTradeTime",
                    "m_strDealTime",
                    "m_nTradeTime",
                    "m_nDealTime",
                )),
                "trade_date": self._first_value(obj, (
                    "trade_date",
                    "deal_date",
                    "m_strTradeDate",
                    "m_strDealDate",
                    "m_strTradingDay",
                    "m_nTradeDate",
                    "m_nDealDate",
                )),
                "offset_flag": self._get_value(obj, "m_nOffsetFlag"),
                "price": self._first_value(obj, (
                    "price",
                    "m_dPrice",
                )),
                "volume": self._get_value(obj, "m_nVolume"),
                "trade_amount": self._get_value(obj, "m_dTradeAmount"),
                "order_status": self._first_value(obj, (
                    "order_status",
                    "status",
                    "m_nOrderStatus",
                    "m_strOrderStatus",
                    "m_nOrderState",
                    "m_strStatus",
                )),
                "m_strInstrumentID": self._get_value(obj, "m_strInstrumentID"),
                "m_strExchangeID": self._get_value(obj, "m_strExchangeID"),
                "m_strInstrumentName": self._get_value(obj, "m_strInstrumentName"),
                "m_strRemark": self._get_value(obj, "m_strRemark"),
                "m_nOrderID": self._get_value(obj, "m_nOrderID"),
                "m_strOrderID": self._get_value(obj, "m_strOrderID"),
                "m_strOrderSysID": self._get_value(obj, "m_strOrderSysID"),
                "m_nOrderType": self._get_value(obj, "m_nOrderType"),
                "m_nDirection": self._get_value(obj, "m_nDirection"),
                "m_nPriceType": self._get_value(obj, "m_nPriceType"),
                "m_strTradeID": self._get_value(obj, "m_strTradeID"),
                "m_nTradeID": self._get_value(obj, "m_nTradeID"),
                "m_strDealID": self._get_value(obj, "m_strDealID"),
                "m_nDealID": self._get_value(obj, "m_nDealID"),
                "m_nOffsetFlag": self._get_value(obj, "m_nOffsetFlag"),
                "m_dPrice": self._get_value(obj, "m_dPrice"),
                "m_nVolume": self._get_value(obj, "m_nVolume"),
                "m_dTradeAmount": self._get_value(obj, "m_dTradeAmount"),
                "m_strTradeTime": self._get_value(obj, "m_strTradeTime"),
                "m_strDealTime": self._get_value(obj, "m_strDealTime"),
                "m_strAccountID": self._get_value(obj, "m_strAccountID"),
                "m_nTradeTime": self._get_value(obj, "m_nTradeTime"),
                "m_nDealTime": self._get_value(obj, "m_nDealTime"),
                "m_strTradeDate": self._get_value(obj, "m_strTradeDate"),
                "m_strDealDate": self._get_value(obj, "m_strDealDate"),
                "m_strTradingDay": self._get_value(obj, "m_strTradingDay"),
            }
        if detail_type == "position":
            volume = self._first_value(
                obj,
                ("position", "volume", "m_nPosition", "m_nVolume"),
            )
            can_close = self._first_value(
                obj,
                (
                    "can_close_vol",
                    "can_use_volume",
                    "m_nCanCloseVolume",
                    "m_nCanUseVolume",
                ),
            )
            direction = self._first_value(
                obj,
                (
                    "direction",
                    "position_direction",
                    "m_nDirection",
                    "m_nPositionDirection",
                    "m_nPosDirection",
                ),
            )
            today = self._first_value(
                obj,
                (
                    "today_position",
                    "today_volume",
                    "m_nTodayPosition",
                    "m_nTodayVolume",
                ),
            )
            yesterday = self._first_value(
                obj,
                (
                    "yesterday_position",
                    "yesterday_volume",
                    "m_nYesterdayPosition",
                    "m_nYdPosition",
                    "m_nYesterdayVolume",
                ),
            )
            return {
                "account_id": resolved_account_id,
                "stock_code": self._stock_code(obj),
                "market": self._get_value(obj, "m_strExchangeID"),
                "instrument_name": self._get_value(obj, "m_strInstrumentName"),
                "position": volume,
                "volume": volume,
                "can_close_vol": can_close,
                "can_use_volume": can_close,
                "direction": direction,
                "position_direction": direction,
                "today_position": today,
                "today_volume": today,
                "yesterday_position": yesterday,
                "yesterday_volume": yesterday,
                "open_price": self._get_value(obj, "m_dOpenPrice"),
                "avg_price": self._first_value(obj, ("m_dAvgPrice", "m_dOpenPrice")),
                "market_value": self._get_value(obj, "m_dInstrumentValue"),
                "position_cost": self._get_value(obj, "m_dPositionCost"),
                "position_profit": self._get_value(obj, "m_dPositionProfit"),
                "m_strInstrumentID": self._get_value(obj, "m_strInstrumentID"),
                "m_strExchangeID": self._get_value(obj, "m_strExchangeID"),
                "m_strInstrumentName": self._get_value(obj, "m_strInstrumentName"),
                "m_nPosition": self._get_value(obj, "m_nPosition"),
                "m_nVolume": self._get_value(obj, "m_nVolume"),
                "m_nCanCloseVolume": self._get_value(obj, "m_nCanCloseVolume"),
                "m_nCanUseVolume": self._get_value(obj, "m_nCanUseVolume"),
                "m_nDirection": self._get_value(obj, "m_nDirection"),
                "m_nPositionDirection": self._get_value(obj, "m_nPositionDirection"),
                "m_nPosDirection": self._get_value(obj, "m_nPosDirection"),
                "m_nTodayPosition": self._get_value(obj, "m_nTodayPosition"),
                "m_nTodayVolume": self._get_value(obj, "m_nTodayVolume"),
                "m_nYesterdayPosition": self._get_value(obj, "m_nYesterdayPosition"),
                "m_nYdPosition": self._get_value(obj, "m_nYdPosition"),
                "m_nYesterdayVolume": self._get_value(obj, "m_nYesterdayVolume"),
                "m_dOpenPrice": self._get_value(obj, "m_dOpenPrice"),
                "m_dAvgPrice": self._get_value(obj, "m_dAvgPrice"),
                "m_dInstrumentValue": self._get_value(obj, "m_dInstrumentValue"),
                "m_strAccountID": self._get_value(obj, "m_strAccountID"),
                "m_dPositionCost": self._get_value(obj, "m_dPositionCost"),
                "m_dPositionProfit": self._get_value(obj, "m_dPositionProfit"),
            }
        if detail_type == "account":
            balance = self._get_value(obj, "m_dBalance")
            available = self._get_value(obj, "m_dAvailable")
            market_value = self._get_value(obj, "m_dInstrumentValue")
            payload = {
                "account_id": resolved_account_id,
                "balance": balance,
                "total_asset": balance,
                "assure_asset": self._get_value(obj, "m_dAssureAsset"),
                "market_value": market_value,
                "total_debit": self._get_value(obj, "m_dTotalDebit"),
                "available": available,
                "cash": available,
                "position_profit": self._get_value(obj, "m_dPositionProfit"),
                "m_dBalance": balance,
                "m_dAssureAsset": self._get_value(obj, "m_dAssureAsset"),
                "m_dInstrumentValue": market_value,
                "m_dTotalDebit": self._get_value(obj, "m_dTotalDebit"),
                "m_dAvailable": available,
                "m_strAccountID": self._get_value(obj, "m_strAccountID"),
                "m_dPositionProfit": self._get_value(obj, "m_dPositionProfit"),
            }
            for alias, raw_name in ACCOUNT_DETAIL_FIELDS:
                value = self._get_value(obj, raw_name)
                payload[alias] = value
                payload[raw_name] = value
            payload["frozen"] = payload["frozen_cash"]
            return payload
        return {"value": str(obj)}

    def _find_order_id(
        self,
        account_id,
        user_order_id,
        strategy_name="",
        wait_seconds=0.3,
        account_type="STOCK",
    ):
        wait_seconds = float(wait_seconds or 0)
        deadline = time.time() + wait_seconds
        query_account_type = self._order_account_type_name(account_type)
        while True:
            try:
                orders = self._query_trade_detail(
                    {
                        "account_id": account_id,
                        "account_type": query_account_type,
                        "strategy_name": strategy_name,
                    },
                    "order",
                )
                for order in orders or []:
                    remark = self._get_value(order, "order_remark")
                    if remark != user_order_id:
                        continue
                    returned_strategy = self._get_value(order, "strategy_name")
                    if (
                        strategy_name
                        and returned_strategy not in (None, "")
                        and str(returned_strategy) != str(strategy_name)
                    ):
                        continue
                    for attr in ("order_sysid", "order_id", "m_strOrderSysID", "m_nOrderID", "m_strOrderID"):
                        value = self._get_value(order, attr)
                        if self._is_usable_order_id(value):
                            return value
            except Exception:
                pass
            if wait_seconds <= 0 or time.time() > deadline:
                break
            time.sleep(0.05)
        return None

    def _is_usable_order_id(self, value):
        if value is None or isinstance(value, bool):
            return False
        text = str(value).strip()
        if not text:
            return False
        try:
            numeric = float(text)
        except (TypeError, ValueError):
            return True
        return math.isfinite(numeric) and numeric > 0

    def _first_value(self, obj, names):
        for name in names:
            value = self._get_value(obj, name)
            if value is not None and value != "":
                return value
        return None

    def _stock_code(self, obj):
        instrument_id = self._get_value(obj, "m_strInstrumentID")
        exchange_id = self._get_value(obj, "m_strExchangeID")
        if instrument_id and exchange_id:
            return "%s.%s" % (instrument_id, exchange_id)
        return instrument_id

    def _get_value(self, obj, name):
        if obj is None:
            return None
        try:
            return self._plain_value(getattr(obj, name))
        except AttributeError:
            pass
        except Exception as e:
            self._log(
                "trade detail getattr failed type=%s field=%s error=%s"
                % (type(obj).__name__, name, e)
            )
        try:
            getter = getattr(obj, "get", None)
            if callable(getter):
                return self._plain_value(getter(name))
        except AttributeError:
            pass
        except Exception as e:
            self._log(
                "trade detail get failed type=%s field=%s error=%s"
                % (type(obj).__name__, name, e)
            )
        return None

    def _plain_value(self, value):
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return str(value)
        try:
            item = getattr(value, "item", None)
            if callable(item):
                return item()
        except Exception:
            pass
        if isinstance(value, (list, tuple)):
            return [self._plain_value(item) for item in value]
        if isinstance(value, dict):
            return dict((str(k), self._plain_value(v)) for k, v in value.items())
        return str(value)

    def _account_type_name(self, account_type):
        mapping = dict((code, name.lower()) for code, name in ACCOUNT_TYPE_CODES.items())
        if isinstance(account_type, str):
            return account_type
        return mapping.get(account_type, "stock")

    def _send_trader_event(self, client_id, name, data):
        if client_id:
            self._send_event(client_id, "trader:%s" % name, data)

    def _client_ids_for_account(self, account_id):
        account_id = str(account_id or "").strip()
        if not account_id:
            return []
        with self.subscriber_lock:
            client_ids = set(self.account_subscribers.get(account_id, set()))
        client_ids.update(account_routing.client_ids(self.bridge_id, account_id))
        return sorted(client_ids)

    def _send_trader_event_to_account(self, account_id, name, data):
        for client_id in self._client_ids_for_account(account_id):
            self._send_trader_event(client_id, name, data)

    def _account_subscriber_status(self):
        with self.subscriber_lock:
            status = dict((account_id, len(client_ids)) for account_id, client_ids in self.account_subscribers.items())
        for account_id, count in account_routing.status(self.bridge_id).items():
            status[account_id] = max(status.get(account_id, 0), count)
        return status

    def _send_event(self, client_id, name, data, subscription_id=None):
        if not client_id or self.tx is None:
            return
        event = pack_event(name, data=data, client_id=client_id, subscription_id=subscription_id)
        self.tx.push("event", event, client_id)

    def _call_variants(self, func, variants):
        last_error = None
        for args, kwargs in variants:
            try:
                return func(*args, **kwargs)
            except TypeError as e:
                last_error = e
                continue
        if last_error:
            raise last_error
        return func()

    def _get_callable(self, *names):
        for name in names:
            func = self.globals_dict.get(name)
            if callable(func):
                return func
            if self.context is not None:
                func = getattr(self.context, name, None)
                if callable(func):
                    return func
        return None

    def _load_txl(self):
        try:
            from tx import txl
        except Exception as e:
            raise RuntimeError("failed to import txl: %s" % e)
        return txl

    def _log(self, msg):
        line = "%s %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
        try:
            with _BRIDGE_LOG_LOCK:
                self._rotate_log_if_needed()
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception:
            pass
        if self.show:
            print(msg)

    def _rotate_log_if_needed(self):
        if self.log_max_bytes <= 0 or not os.path.isfile(self.log_file):
            return
        if os.path.getsize(self.log_file) < self.log_max_bytes:
            return
        backup_count = max(0, self.log_backup_count)
        if backup_count == 0:
            os.remove(self.log_file)
            return
        oldest = "%s.%s" % (self.log_file, backup_count)
        if os.path.exists(oldest):
            os.remove(oldest)
        for index in range(backup_count - 1, 0, -1):
            source = "%s.%s" % (self.log_file, index)
            if os.path.exists(source):
                os.replace(source, "%s.%s" % (self.log_file, index + 1))
        os.replace(self.log_file, "%s.1" % self.log_file)


def start_tx_trade_bridge(
    context,
    ip="127.0.0.1",
    port=2049,
    token="LTtx",
    request_channel="cfquant.request",
    bridge_id="default",
    account_id="",
    show=True,
    account_locked=False,
    account_type=2,
    callback_publisher=None,
):
    try:
        globals_dict = sys._getframe(1).f_globals
    except Exception:
        globals_dict = {}
    return TxTradeBridge(
        context,
        ip=ip,
        port=port,
        token=token,
        request_channel=request_channel,
        bridge_id=bridge_id,
        account_id=account_id,
        show=show,
        globals_dict=globals_dict,
        account_locked=account_locked,
        account_type=account_type,
        callback_publisher=callback_publisher,
    )
