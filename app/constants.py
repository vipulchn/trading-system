from decimal import Decimal

# ── Risk parameters ──────────────────────────────────────────────────────────
MAX_RISK_PER_TRADE_PCT: Decimal = Decimal("0.01")
DAILY_LOSS_LIMIT_PCT: Decimal   = Decimal("0.02")
MAX_OPEN_POSITIONS: int         = 2
MAX_TOTAL_CAPITAL_AT_RISK_PCT: Decimal = Decimal("0.02")
MIN_RR_RATIO: Decimal           = Decimal("2.0")
MAX_STOP_DISTANCE_PCT: Decimal  = Decimal("0.01")

# ── Session timing (IST, 24h) ─────────────────────────────────────────────────
IST_TIMEZONE: str          = "Asia/Kolkata"
SESSION_OPEN_HOUR: int     = 9
SESSION_OPEN_MINUTE: int   = 15
NO_TRADE_UNTIL_HOUR: int   = 9
NO_TRADE_UNTIL_MINUTE: int = 45
TRADE_END_HOUR: int        = 15
TRADE_END_MINUTE: int      = 0
FORCE_EXIT_HOUR: int       = 15
FORCE_EXIT_MINUTE: int     = 15
OPEN_DRIVE_WINDOW_END_HOUR: int   = 10
OPEN_DRIVE_WINDOW_END_MINUTE: int = 15

# ── Setups ────────────────────────────────────────────────────────────────────
SETUP_NAMES: dict[int, str] = {
    1: "POC Magnet Pull",
    2: "Value Area Breakout",
    3: "HVN Rejection",
    4: "LVN Acceleration",
    5: "Open Drive",
}
DISABLED_SETUPS: frozenset[int] = frozenset({6})

# ── VRVP settings (backtester only) ───────────────────────────────────────────
VRVP_LOOKBACK_SESSIONS: int  = 20
VRVP_ROWS: int               = 200
VRVP_VALUE_AREA_PCT: float   = 0.70

# ── Universe filters ──────────────────────────────────────────────────────────
MIN_AVG_DAILY_TURNOVER_CR: int = 50
MIN_AVG_DAILY_VOLUME: int      = 500_000
UNIVERSE_TOP_N: int            = 50
MIN_BACKTEST_TRADES: int       = 30

# ── Walk-forward ──────────────────────────────────────────────────────────────
WALKFORWARD_TRAIN_MONTHS: int       = 4
WALKFORWARD_OOS_MONTHS: int         = 2
WALKFORWARD_VALIDATION_RATIO: Decimal = Decimal("0.70")
SCORE_WEIGHT_SHARPE: float  = 0.40
SCORE_WEIGHT_WIN_RATE: float = 0.30
SCORE_WEIGHT_AVG_R: float   = 0.30
SUSPICIOUS_WIN_RATE: float  = 0.70

# ── Agent 2 ───────────────────────────────────────────────────────────────────
MAX_DAILY_CANDIDATES: int = 8
MIN_DAILY_CANDIDATES: int = 3
CANDIDATE_MIN_SCORE: int  = 7
AGENT2_MODEL: str         = "claude-sonnet-4-6"
AGENT2_MAX_TOKENS: int    = 600
CAPITAL_MISMATCH_PCT: Decimal = Decimal("0.02")

# ── Agent 3 ───────────────────────────────────────────────────────────────────
SIGNAL_DEDUP_TTL_SECONDS: int = 900

# ── Agent 7 ───────────────────────────────────────────────────────────────────
WATCHDOG_INTERVAL_MINUTES: int       = 5
WATCHDOG_TV_STALE_MINUTES: int       = 90
WATCHDOG_RETRY_INTERVAL_SECONDS: int = 60
WATCHDOG_MAX_RETRIES: int            = 5
WATCHDOG_REDIS_TIMEOUT_S: float      = 1.0
WATCHDOG_PG_TIMEOUT_S: float         = 2.0
WATCHDOG_DHAN_TIMEOUT_S: float       = 3.0
WATCHDOG_MEMORY_LIMIT_MB: int        = 512

# ── Market volume filter ──────────────────────────────────────────────────────
LOW_VOLUME_MARKET_THRESHOLD_PCT: Decimal = Decimal("0.60")

# ── Transaction costs (backtester) ───────────────────────────────────────────
BROKERAGE_PCT: Decimal          = Decimal("0.0003")
STT_PCT: Decimal                = Decimal("0.001")
GST_ON_BROKERAGE: Decimal       = Decimal("0.18")
EXCHANGE_CHARGE_PER_ORDER: Decimal = Decimal("20")
SEBI_CHARGE_PCT: Decimal        = Decimal("0.0001")

# ── Redis keys ────────────────────────────────────────────────────────────────
REDIS_KEY_CAPITAL: str       = "system:capital_inr"
REDIS_KEY_HALT: str          = "system:halt"
REDIS_KEY_TRADING_ACTIVE: str = "trading:active"
REDIS_QUEUE_SIGNALS: str     = "queue:signals"
REDIS_QUEUE_EXECUTION: str   = "queue:execution"
REDIS_KEY_DEDUP_PREFIX: str  = "signal:dedup:"
REDIS_KEY_LAST_TV_WEBHOOK: str = "watchdog:last_tv_webhook"
REDIS_CAPITAL_TTL_SECONDS: int = 86_400
