#!/usr/bin/env python3
import os
import time
import json
import signal
import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_ta as ta

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest, TrailingStopOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.requests import StockBarsRequest

# ========================== LOGGING ==========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    filename="ffb.log",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().addHandler(logging.StreamHandler())

# ========================== CONFIG ==========================
BOT_TAG = os.getenv("BOT_TAG", "ffb").strip() or "ffb"

ASSETS = [
    "SPY", "QQQ", "IWM", "DIA",
    "XLK", "XLE", "XLF",
    "SMH", "SOXX",
    "IEF", "HYG", "LQD",
    "GLD", "SLV", "TSLA",
    "AMZN", "GOOG", "MSFT",
    "AMD", "NVDA", "AAPL",
]
_seen = set()
ASSETS = [s for s in ASSETS if not (s in _seen or _seen.add(s))]

TIMEFRAME_MIN = int(os.getenv("TIMEFRAME_MIN", "5"))
EMA_SHORT = int(os.getenv("EMA_SHORT", "20"))
EMA_LONG = int(os.getenv("EMA_LONG", "50"))
VOL_EMA_LEN = int(os.getenv("VOL_EMA_LEN", "50"))

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
STALE_MINUTES = int(os.getenv("STALE_MINUTES", "15"))

SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "15"))
CLOSED_SLEEP_SECONDS = int(os.getenv("CLOSED_SLEEP_SECONDS", "60"))
UPDATE_EVERY_SEC = int(os.getenv("UPDATE_EVERY_SEC", "60"))

# Risk budget
RISK_BUDGET_TOTAL = float(os.getenv("RISK_BUDGET_TOTAL", "0.01"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "6"))

_rpt_env = os.environ.get("RISK_PER_TRADE")
if _rpt_env is not None and str(_rpt_env).strip() != "":
    RISK_PER_TRADE = float(_rpt_env)
else:
    RISK_PER_TRADE = (RISK_BUDGET_TOTAL / MAX_OPEN_POSITIONS) if MAX_OPEN_POSITIONS > 0 else 0.0

# Trailing stop: Alpaca expects percent units (1.0 = 1%)
TRAIL_PERCENT = float(os.getenv("TRAIL_PERCENT", "1.0"))

_stop_pct_env = os.environ.get("STOP_PCT")
if _stop_pct_env is None or str(_stop_pct_env).strip() == "":
    STOP_PCT = max(0.0001, float(TRAIL_PERCENT) / 100.0)
else:
    STOP_PCT = float(_stop_pct_env)

ENTRY_TIF = os.getenv("ENTRY_TIF", "day").strip().lower()
TRAIL_TIF = os.getenv("TRAIL_TIF", "gtc").strip().lower()

MAX_NEW_BUYS_PER_BUCKET = int(os.getenv("MAX_NEW_BUYS_PER_BUCKET", "3"))

MIN_QTY = int(os.getenv("MIN_QTY", "1"))
MIN_NOTIONAL = float(os.getenv("MIN_NOTIONAL", "50.0"))
ALLOW_MIN_QTY_OVERRIDE_RISK = os.getenv("ALLOW_MIN_QTY_OVERRIDE_RISK", "false").lower() == "true"

USE_BUYING_POWER_FOR_CAPS = os.getenv("USE_BUYING_POWER_FOR_CAPS", "true").lower() == "true"
MAX_SYMBOL_CASH_PCT = float(os.getenv("MAX_SYMBOL_CASH_PCT", "0.08"))

# Daily loss limit
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.02"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "0.0"))
DAILY_LOSS_MODE = os.getenv("DAILY_LOSS_MODE", "halt_entries").strip().lower()
TRADING_DAY_TZ = os.getenv("TRADING_DAY_TZ", "America/New_York")

# Volume filter
VOLUME_FILTER_ENABLED = os.getenv("VOLUME_FILTER_ENABLED", "true").lower() == "true"
ETF_VOLUME_BYPASS = os.getenv("ETF_VOLUME_BYPASS", "true").lower() == "true"
VOL_RELAX_MULT = float(os.getenv("VOL_RELAX_MULT", "0.60"))
ETF_SET = {
    "SPY", "QQQ", "IWM", "DIA",
    "XLK", "XLF", "XLE", "XLV",
    "SMH", "SOXX",
    "IEF", "HYG", "LQD",
    "GLD", "SLV",
}

# RSI filter (optional)
RSI_FILTER_ENABLED = os.getenv("RSI_FILTER", "false").lower() == "true"
RSI_LEN = int(os.getenv("RSI_LEN", "14"))
RSI_MAX = float(os.getenv("RSI_MAX", "70"))

# Telemetry
TELEMETRY_EVERY_SEC = int(os.getenv("TELEMETRY_EVERY_SEC", "60"))
POSITIONS_SYNC_EVERY_SEC = int(os.getenv("POSITIONS_SYNC_EVERY_SEC", "30"))
ORDERS_SYNC_EVERY_SEC = int(os.getenv("ORDERS_SYNC_EVERY_SEC", "30"))

STATE_PATH = os.getenv("STATE_PATH", "./ffb_state.json")

# Data feed
DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex").strip().lower() or "iex"

# ========================== ALPACA ==========================
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
if not API_KEY or not API_SECRET:
    raise SystemExit("Missing Alpaca API keys in environment variables.")

PAPER = os.getenv("PAPER", "true").lower() == "true"
if not PAPER:
    raise SystemExit("This portfolio build supports Alpaca paper trading only. Set PAPER=true.")
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
data_client = StockHistoricalDataClient(API_KEY, API_SECRET)

# ========================== STATE ==========================
def load_state(path: str) -> dict:
    try:
        with open(path, "r") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        logging.warning(f"STATE load failed: {e}")
        return {}

def save_state(path: str, data: dict) -> None:
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception as e:
        logging.warning(f"STATE save failed: {e}")

state = load_state(STATE_PATH)

# ========================== SIGNAL HANDLING ==========================
_STOP = {"flag": False}
def _handle_signal(signum, frame):
    _STOP["flag"] = True
    logging.warning(f"Signal {signum} received — shutting down gracefully...")

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ========================== HELPERS ==========================
def market_open() -> bool:
    try:
        return trading_client.get_clock().is_open
    except Exception as e:
        logging.warning(f"Clock check failed: {e}")
        return False

def tif_from_str(s: str) -> TimeInForce:
    s = (s or "").strip().lower()
    return TimeInForce.GTC if s == "gtc" else TimeInForce.DAY

def _norm_enumish(v) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    # OrderStatus.FILLED -> "orderstatus.filled" -> "filled"
    if "." in s:
        s = s.split(".")[-1]
    return s

def _infer_order_side(o) -> str:
    s = _norm_enumish(getattr(o, "side", None))
    if s in ("buy", "sell"):
        return s
    if "buy" in s:
        return "buy"
    if "sell" in s:
        return "sell"
    return ""

def _infer_order_type(o) -> str:
    for attr in ("order_type", "type"):
        s = _norm_enumish(getattr(o, attr, None))
        if s:
            return s
    return ""

def _to_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        try:
            return float(str(v))
        except Exception:
            return None

def _get_account_numbers():
    acct = trading_client.get_account()
    cash = float(getattr(acct, "cash", 0) or 0)
    equity = float(getattr(acct, "equity", cash) or cash)
    buying_power = float(getattr(acct, "buying_power", cash) or cash)
    return cash, equity, buying_power

def _cap_base(cash: float, buying_power: float) -> float:
    return buying_power if USE_BUYING_POWER_FOR_CAPS and buying_power > 0 else cash

def safe_get_open_orders(nested: bool = True, limit: int = 500):
    """
    CRITICAL: only OPEN orders, so we don't misclassify filled/canceled as active.
    Works across alpaca-py versions.
    """
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN, nested=nested, limit=limit)
        return trading_client.get_orders(filter=req) or []
    except Exception:
        pass

    try:
        # older style
        return trading_client.get_orders(status="open", nested=nested, limit=limit) or []
    except Exception:
        pass

    try:
        return trading_client.get_orders() or []
    except Exception:
        return []

def build_active_sets(open_orders):
    """Return (active_buy_symbols, trailing_sells_by_symbol) from OPEN orders."""
    active_buy_symbols = set()
    trailing_sells_by_symbol = {}  # sym -> [order_obj, ...]

    if not open_orders:
        logging.info("ORDERS_SYNC open=0 buys=0 trails=0")
        return active_buy_symbols, trailing_sells_by_symbol

    try:
        open_orders = list(open_orders)
    except TypeError:
        open_orders = []

    open_status = {'new','accepted','pending_new','partially_filled','held','pending_replace','replaced'}

    for o in open_orders:
        # symbol
        sym = getattr(o, 'symbol', None)
        if sym is None and isinstance(o, dict):
            sym = o.get('symbol')
        if not sym:
            continue
        sym = str(sym).upper()

        # side/type/status
        if isinstance(o, dict):
            side_s = str(o.get('side', '')).lower()
            type_s = str(o.get('type', '')).lower()
            status_s = str(o.get('status', '')).lower()
        else:
            side_s = str(getattr(o, 'side', '')).lower()
            type_s = str(getattr(o, 'type', '')).lower()
            status_s = str(getattr(o, 'status', '')).lower()

        if ('buy' in side_s) and (status_s in open_status):
            active_buy_symbols.add(sym)

        if ('sell' in side_s) and ('trailing' in type_s):
            trailing_sells_by_symbol.setdefault(sym, []).append(o)

    trails_n = sum(len(v) for v in trailing_sells_by_symbol.values())
    logging.info(f"ORDERS_SYNC open={len(open_orders)} buys={len(active_buy_symbols)} trails={trails_n}")
    return active_buy_symbols, trailing_sells_by_symbol

def cancel_order(order_id: str):
    try:
        trading_client.cancel_order_by_id(order_id)
        return True
    except Exception as e:
        logging.warning(f"Cancel failed order_id={order_id}: {e}")
        return False

def ensure_one_trailing_stop(symbol: str, qty: int, existing_trails):
    """
    Ensure we have exactly one SELL trailing stop per symbol.
    If Alpaca says insufficient qty available (40310000) with related_orders,
    that means our shares are already held by an existing sell order -> treat as OK.
    """
    import time as _time

    if qty <= 0:
        return

    sym = str(symbol).upper().strip()

    # per-symbol cooldown to avoid hammering submit attempts
    cd = getattr(ensure_one_trailing_stop, "_cooldown", {})
    now = _time.time()
    until = cd.get(sym, 0)
    if now < until:
        return

    def _oid(o):
        if o is None:
            return None
        if isinstance(o, dict):
            v = o.get("id", None)
        else:
            v = getattr(o, "id", None)
        return str(v) if v is not None else None

    def _as_list(x):
        if x is None:
            return []
        try:
            return list(x)
        except TypeError:
            return []

    # ---- normalize incoming trails ----
    trails = []
    seen = set()
    for o in _as_list(existing_trails):
        oid = _oid(o)
        if oid and oid not in seen:
            trails.append(o)
            seen.add(oid)

    # If we already have any trails, keep 1 and cancel extras
    if trails:
        keep = _oid(trails[0])
        for o in trails[1:]:
            oid = _oid(o)
            if oid and oid != keep:
                cancel_order(oid)
        return

    # ---- fallback: query open orders (throttled) ----
    cache = getattr(ensure_one_trailing_stop, "_open_trails_cache", None)
    ts = getattr(ensure_one_trailing_stop, "_open_trails_cache_ts", 0.0)
    if cache is None or (now - ts) > 15:
        cache = {}
        try:
            from alpaca.trading.requests import GetOrdersRequest
            from alpaca.trading.enums import QueryOrderStatus, OrderType

            req = GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500, nested=True)
            orders = trading_client.get_orders(filter=req) or []
            for o in orders:
                osym = getattr(o, "symbol", None)
                if osym is None and isinstance(o, dict):
                    osym = o.get("symbol")
                if not osym:
                    continue
                osym = str(osym).upper().strip()

                # side
                side = None
                if isinstance(o, dict):
                    side = o.get("side")
                else:
                    side = getattr(o, "side", None)

                # type/order_type
                otype = None
                if isinstance(o, dict):
                    otype = o.get("type") or o.get("order_type")
                else:
                    otype = getattr(o, "type", None) or getattr(o, "order_type", None)

                side_ok = False
                type_ok = False

                try:
                    side_ok = (side == OrderSide.SELL) or ("sell" in str(side).lower())
                except Exception:
                    side_ok = ("sell" in str(side).lower())

                try:
                    type_ok = (otype == OrderType.TRAILING_STOP) or ("trailing" in str(otype).lower())
                except Exception:
                    type_ok = ("trailing" in str(otype).lower())

                if side_ok and type_ok:
                    cache.setdefault(osym, []).append(o)

        except Exception as e:
            # If we can't read orders, don't spam submit attempts; back off briefly.
            logging.warning(f"TRAIL guard: get_orders failed ({sym}): {e}")
            cd[sym] = now + 30
            ensure_one_trailing_stop._cooldown = cd
            return

        ensure_one_trailing_stop._open_trails_cache = cache
        ensure_one_trailing_stop._open_trails_cache_ts = now

    existing = _as_list(cache.get(sym, []))

    if existing:
        # keep one, cancel extras
        keep = _oid(existing[0])
        for o in existing[1:]:
            oid = _oid(o)
            if oid and oid != keep:
                cancel_order(oid)
        return

    # ---- create one trail ----
    desired_tif = tif_from_str(TRAIL_TIF)
    desired_trail = float(TRAIL_PERCENT)

    try:
        trail_req = TrailingStopOrderRequest(
            symbol=sym,
            qty=int(qty),
            side=OrderSide.SELL,
            time_in_force=desired_tif,
            trail_percent=desired_trail,
            client_order_id=f"{BOT_TAG}-trail-{sym.lower()}-{int(now)}",
        )
        resp = trading_client.submit_order(trail_req)
        new_id = getattr(resp, "id", None)
        logging.info(f"SENT SELL TRAIL {sym} qty={qty} trail={desired_trail}% tif={TRAIL_TIF} order_id={new_id}")
    except Exception as e:
        msg = str(e)

        # Key fix: treat "insufficient qty available" as "already protected"
        if ("40310000" in msg) and ("insufficient qty available" in msg) and ("related_orders" in msg):
            logging.info(f"TRAIL already exists/qty held for {sym}; skipping recreate.")
            cd[sym] = now + 300
            ensure_one_trailing_stop._cooldown = cd
            return

        logging.warning(f"TRAIL create failed {sym}: {e}")
        cd[sym] = now + 60
        ensure_one_trailing_stop._cooldown = cd

def bulk_fetch_bars(symbols, feed: str):
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=LOOKBACK_DAYS)
        req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(TIMEFRAME_MIN, TimeFrameUnit.Minute),
            start=start,
            end=end,
            limit=5000,
            feed=feed,
        )
        bars = data_client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return {}

        df = bars.reset_index()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        out = {}
        for sym, g in df.groupby("symbol"):
            g = g.sort_values("timestamp").set_index("timestamp")
            out[sym] = g
        return out
    except Exception as e:
        logging.exception(f"bulk_fetch_bars error (feed={feed}): {e}")
        return {}

def fetch_bars_with_feed_fallback(symbols):
    global DATA_FEED
    bars = bulk_fetch_bars(symbols, DATA_FEED)
    if bars:
        return bars
    if DATA_FEED != "iex":
        logging.warning(f"Feed '{DATA_FEED}' returned no data. Falling back to 'iex'.")
        DATA_FEED = "iex"
        return bulk_fetch_bars(symbols, DATA_FEED)
    return {}

def submit_entry_buy(symbol: str, qty: int, sig_ts: datetime):
    cid = f"{BOT_TAG}-buy-{symbol.lower()}-{sig_ts.strftime('%Y%m%d%H%M')}-{int(time.time())}"
    order = MarketOrderRequest(
        symbol=symbol,
        qty=int(qty),
        side=OrderSide.BUY,
        time_in_force=tif_from_str(ENTRY_TIF),
        client_order_id=cid,
    )
    try:
        resp = trading_client.submit_order(order)
        oid = getattr(resp, "id", None)
        logging.info(f"SENT BUY {symbol} qty={qty} tif={ENTRY_TIF} client_id={cid} order_id={oid}")
        return True, str(oid) if oid else None
    except Exception as e:
        logging.warning(f"BUY failed {symbol} qty={qty}: {e}")
        return False, None

def try_get_open_position(symbol: str):
    try:
        return trading_client.get_open_position(symbol)
    except Exception:
        try:
            return trading_client.get_open_position(symbol_or_asset_id=symbol)
        except Exception:
            return None

# ========================== STARTUP LOG ==========================
logging.info("=== FFB BOT STARTING (TRAILING STOP MODE - SIMPLE) ===")
logging.info(f"PAPER={PAPER} BOT_TAG={BOT_TAG} ASSETS({len(ASSETS)}): {ASSETS}")
logging.info(f"TIMEFRAME={TIMEFRAME_MIN}m EMA={EMA_SHORT}/{EMA_LONG} VOL_EMA={VOL_EMA_LEN} RSI_FILTER={RSI_FILTER_ENABLED}")
logging.info(f"LOOKBACK_DAYS={LOOKBACK_DAYS} FEED={DATA_FEED}")
logging.info(f"ENTRY_TIF={ENTRY_TIF} TRAIL_TIF={TRAIL_TIF} TRAIL_PERCENT={TRAIL_PERCENT}% STOP_PCT={STOP_PCT}")
logging.info(f"RISK_BUDGET_TOTAL={RISK_BUDGET_TOTAL} RISK_PER_TRADE={RISK_PER_TRADE:.6f} MAX_OPEN_POSITIONS={MAX_OPEN_POSITIONS}")
logging.info(f"MAX_NEW_BUYS_PER_BUCKET={MAX_NEW_BUYS_PER_BUCKET} MIN_NOTIONAL={MIN_NOTIONAL} ALLOW_MIN_QTY_OVERRIDE_RISK={ALLOW_MIN_QTY_OVERRIDE_RISK}")
logging.info(f"DAILY_LOSS_LIMIT_PCT={DAILY_LOSS_LIMIT_PCT} DAILY_LOSS_LIMIT_USD={DAILY_LOSS_LIMIT_USD} MODE={DAILY_LOSS_MODE} TZ={TRADING_DAY_TZ}")
logging.info(f"POSITIONS_SYNC_EVERY_SEC={POSITIONS_SYNC_EVERY_SEC} ORDERS_SYNC_EVERY_SEC={ORDERS_SYNC_EVERY_SEC} TELEMETRY_EVERY_SEC={TELEMETRY_EVERY_SEC}")

# ========================== RUNTIME STATE ==========================
positions = {}
last_pos_sync = 0.0

active_buy_symbols = set()
trailing_sell_by_symbol = {}
last_orders_sync = 0.0

last_seen_bar = {}
last_order_bar = {}

last_update = 0.0
last_telemetry = 0.0

last_bars_bucket = None
bars_by_symbol = {}

last_bucket_id = None
new_buys_this_bucket = 0

daily_baseline_day = None
daily_baseline_equity = None
daily_kill_triggered = False

def log_position_telemetry():
    payload = []
    for sym, p in positions.items():
        qty = _to_float(getattr(p, "qty", None)) or 0.0
        avg = _to_float(getattr(p, "avg_entry_price", None)) or 0.0
        payload.append({"sym": sym, "qty": qty, "avg": avg})
    logging.info(f"TELEMETRY positions={payload}")

# ========================== MAIN LOOP ==========================
while True:
    try:
        if _STOP["flag"]:
            break

        now = datetime.now(timezone.utc)
        is_open = market_open()

        # ---- sync positions (always) ----
        if time.time() - last_pos_sync > POSITIONS_SYNC_EVERY_SEC:
            try:
                positions = {p.symbol: p for p in trading_client.get_all_positions()}
                last_pos_sync = time.time()
                logging.info(f"OPEN POSITIONS: {list(positions.keys())}")
            except Exception as e:
                logging.warning(f"Position sync failed: {e}")

        # ---- sync OPEN orders (always) ----
        if time.time() - last_orders_sync > ORDERS_SYNC_EVERY_SEC:
            try:
                open_orders = safe_get_open_orders(nested=True, limit=500)
                active_buy_symbols, trailing_sell_by_symbol = build_active_sets(open_orders)
                last_orders_sync = time.time()

                # enforce trailing stop for current positions
                for sym, p in positions.items():
                    qty = int(round(_to_float(getattr(p, "qty", None)) or 0))
                    if qty > 0:
                        ensure_one_trailing_stop(sym, qty, (trailing_sell_by_symbol.get(sym, []) or trailing_sell_by_symbol.get(str(sym).upper().strip(), []) or trailing_sell_by_symbol.get(str(sym).lower().strip(), [])))

            except Exception as e:
                logging.exception("Order sync failed")

        # ---- telemetry (always) ----
        if time.time() - last_telemetry > TELEMETRY_EVERY_SEC:
            last_telemetry = time.time()
            try:
                log_position_telemetry()
            except Exception:
                pass

        # ---- if market closed, do only protection/sync ----
        if not is_open:
            time.sleep(CLOSED_SLEEP_SECONDS)
            continue

        # ---- account fetch ----
        try:
            cash, equity, buying_power = _get_account_numbers()
            cap_base = _cap_base(cash=cash, buying_power=buying_power)
        except Exception as e:
            logging.warning(f"Account fetch failed: {e}")
            time.sleep(SLEEP_SECONDS)
            continue

        # ---- daily loss baseline / kill switch ----
        if DAILY_LOSS_LIMIT_PCT > 0 or DAILY_LOSS_LIMIT_USD > 0:
            local_day = datetime.now(ZoneInfo(TRADING_DAY_TZ)).date()
            if daily_baseline_day != local_day:
                daily_baseline_day = local_day
                daily_baseline_equity = equity
                daily_kill_triggered = False
                logging.info(f"DAILY_BASELINE set day={daily_baseline_day} equity={daily_baseline_equity:.2f}")

            if (daily_baseline_equity is not None) and (not daily_kill_triggered):
                dd = float(daily_baseline_equity) - float(equity)
                thresh_usd = float(DAILY_LOSS_LIMIT_USD) if DAILY_LOSS_LIMIT_USD > 0 else 0.0
                thresh_pct_usd = float(daily_baseline_equity) * float(DAILY_LOSS_LIMIT_PCT) if DAILY_LOSS_LIMIT_PCT > 0 else 0.0
                thresh = max(thresh_usd, thresh_pct_usd)
                if thresh > 0 and dd >= thresh:
                    daily_kill_triggered = True
                    logging.error(f"DAILY_LOSS KILL-SWITCH TRIGGERED dd={dd:.2f} threshold={thresh:.2f} mode={DAILY_LOSS_MODE}")

        if daily_kill_triggered and DAILY_LOSS_MODE in ("halt_entries", "halt"):
            time.sleep(SLEEP_SECONDS)
            continue

        # ---- bucket fetch (only once per new 5m bar bucket) ----
        current_bucket = int(now.timestamp() // (TIMEFRAME_MIN * 60))
        if current_bucket != last_bars_bucket:
            bars_by_symbol = fetch_bars_with_feed_fallback(ASSETS)
            for sym, df in list(bars_by_symbol.items()):
                if df is None or df.empty:
                    continue
                df["ema_s"] = ta.ema(df["close"], EMA_SHORT)
                df["ema_l"] = ta.ema(df["close"], EMA_LONG)
                df["vol_ema"] = df["volume"].ewm(span=VOL_EMA_LEN).mean()
                if RSI_FILTER_ENABLED:
                    df["rsi"] = ta.rsi(df["close"], RSI_LEN)
                bars_by_symbol[sym] = df

            last_bars_bucket = current_bucket
            if last_bucket_id != current_bucket:
                last_bucket_id = current_bucket
                new_buys_this_bucket = 0

            bucket_stats = {
                "symbols": len(ASSETS),
                "no_df": 0,
                "insufficient": 0,
                "same_bar": 0,
                "stale": 0,
                "ind_na": 0,
                "cross_up": 0,
                "trend_up": 0,
                "vol_fail": 0,
                "rsi_fail": 0,
                "dup_signal_bar": 0,
                "bucket_throttle": 0,
                "max_open_block": 0,
                "signal_all": 0,
                "qty0": 0,
                "min_notional": 0,
                "eligible": 0,
                "submitted": 0,
                "submit_fail": 0,
            }
        else:
            time.sleep(SLEEP_SECONDS)
            continue

        # ---- periodic "alive" update ----
        if time.time() - last_update > UPDATE_EVERY_SEC:
            last_update = time.time()
            logging.info(
                f"UPDATE: fetched {len(bars_by_symbol)}/{len(ASSETS)} symbols at {now.isoformat()} "
                f"feed={DATA_FEED} cash={cash:.2f} equity={equity:.2f} buying_power={buying_power:.2f}"
            )

        # ---- scan symbols ----
        for sym in ASSETS:
            if _STOP["flag"]:
                break

            df = bars_by_symbol.get(sym)
            if df is None:
                bucket_stats["no_df"] += 1
                continue

            need = EMA_LONG + 3
            if len(df) < need:
                bucket_stats["insufficient"] += 1
                continue

            current_bar_ts = df.index[-1]
            if last_seen_bar.get(sym) == current_bar_ts:
                bucket_stats["same_bar"] += 1
                continue
            last_seen_bar[sym] = current_bar_ts

            sig_i = -2
            sig_ts = df.index[sig_i]
            age_min = (now - sig_ts).total_seconds() / 60.0
            if age_min > STALE_MINUTES:
                bucket_stats["stale"] += 1
                continue

            if pd.isna(df["ema_s"].iloc[sig_i]) or pd.isna(df["ema_l"].iloc[sig_i]):
                bucket_stats["ind_na"] += 1
                continue

            ema_s = float(df["ema_s"].iloc[sig_i])
            ema_l = float(df["ema_l"].iloc[sig_i])
            price = float(df["close"].iloc[sig_i])

            cross_up = (ema_s > ema_l) and (float(df["ema_s"].iloc[sig_i - 1]) <= float(df["ema_l"].iloc[sig_i - 1]))
            trend_up = ema_s > ema_l
            if cross_up:
                bucket_stats["cross_up"] += 1
            if trend_up:
                bucket_stats["trend_up"] += 1

            # volume
            if not VOLUME_FILTER_ENABLED:
                vol_ok = True
            elif ETF_VOLUME_BYPASS and sym in ETF_SET:
                vol_ok = True
            else:
                vol_ok = df["volume"].iloc[sig_i] > (VOL_RELAX_MULT * df["vol_ema"].iloc[sig_i])
            if not vol_ok:
                bucket_stats["vol_fail"] += 1

            # rsi
            rsi_ok = True
            if RSI_FILTER_ENABLED:
                rsi_val = df["rsi"].iloc[sig_i] if "rsi" in df.columns else float("nan")
                rsi_ok = (not pd.isna(rsi_val)) and float(rsi_val) <= RSI_MAX
            if not rsi_ok:
                bucket_stats["rsi_fail"] += 1

            if last_order_bar.get(sym) == sig_ts:
                bucket_stats["dup_signal_bar"] += 1
                continue

            if new_buys_this_bucket >= MAX_NEW_BUYS_PER_BUCKET:
                bucket_stats["bucket_throttle"] += 1
                continue

            if (cross_up or trend_up) and vol_ok and rsi_ok:
                bucket_stats["signal_all"] += 1

            # ===== ENTRY =====
            if (cross_up or trend_up) and vol_ok and rsi_ok and sym not in positions and sym not in active_buy_symbols:
                if len(positions) + len(active_buy_symbols) >= MAX_OPEN_POSITIONS:
                    bucket_stats["max_open_block"] += 1
                    continue

                risk_amt = equity * float(RISK_PER_TRADE)
                stop_dist = price * STOP_PCT
                qty = int(risk_amt / stop_dist) if stop_dist > 0 else 0

                max_notional = cap_base * MAX_SYMBOL_CASH_PCT
                qty_cap = int(max_notional / price) if price > 0 else 0
                qty = min(qty, qty_cap)

                notional = qty * price
                if qty <= 0:
                    bucket_stats["qty0"] += 1
                    if qty_cap >= MIN_QTY and cap_base >= MIN_NOTIONAL and ALLOW_MIN_QTY_OVERRIDE_RISK:
                        qty = MIN_QTY
                        notional = qty * price
                    else:
                        continue

                if notional < MIN_NOTIONAL:
                    bucket_stats["min_notional"] += 1
                    continue

                bucket_stats["eligible"] += 1
                ok, buy_oid = submit_entry_buy(sym, qty, sig_ts)
                if ok:
                    bucket_stats["submitted"] += 1
                    last_order_bar[sym] = sig_ts
                    active_buy_symbols.add(sym)
                    new_buys_this_bucket += 1
                else:
                    bucket_stats["submit_fail"] += 1

                time.sleep(1)

        logging.info(
            "BUCKET_STATS "
            f"bucket={current_bucket} ts={now.isoformat()} "
            f"pos={len(positions)} active_buy_syms={len(active_buy_symbols)} "
            f"new_buys_bucket={new_buys_this_bucket} stats={bucket_stats}"
        )

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        logging.exception(f"MAIN LOOP ERROR: {e}")
        time.sleep(SLEEP_SECONDS)

logging.info("=== FFB BOT STOPPED ===")
