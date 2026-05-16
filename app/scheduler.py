import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)
TZ = "Asia/Kolkata"


def create_scheduler() -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=TZ)
    s.add_job(_backtest,       CronTrigger(day_of_week="sun",     hour=6,  minute=0,  timezone=TZ), id="agent1_backtest",  replace_existing=True, misfire_grace_time=300)
    s.add_job(_csv_export,     CronTrigger(day_of_week="sun",     hour=5,  minute=55, timezone=TZ), id="agent6_export",    replace_existing=True, misfire_grace_time=120)
    s.add_job(_capital,        CronTrigger(day_of_week="mon-fri", hour=8,  minute=0,  timezone=TZ), id="agent2_capital",   replace_existing=True, misfire_grace_time=120)
    s.add_job(_premarket,      CronTrigger(day_of_week="mon-fri", hour=8,  minute=30, timezone=TZ), id="agent2_llm",       replace_existing=True, misfire_grace_time=120)
    s.add_job(_force_exit,     CronTrigger(day_of_week="mon-fri", hour=15, minute=15, timezone=TZ), id="agent5_forceexit", replace_existing=True, misfire_grace_time=60)
    s.add_job(_eod_report,     CronTrigger(day_of_week="mon-fri", hour=15, minute=30, timezone=TZ), id="agent6_eod",       replace_existing=True, misfire_grace_time=120)
    s.add_job(_watchdog,       IntervalTrigger(minutes=5, timezone=TZ),                             id="agent7_watchdog",  replace_existing=True, misfire_grace_time=30)
    return s


async def _backtest():
    from app.agents.backtester import run_universe_backtest
    await run_universe_backtest()

async def _csv_export():
    from app.agents.agent6_auditor import run_weekly_csv_export
    await run_weekly_csv_export()

async def _capital():
    from app.agents.agent2_premarket import run_capital_reconcile
    await run_capital_reconcile()

async def _premarket():
    from app.agents.agent2_premarket import run_premarket_analyst
    await run_premarket_analyst()

async def _force_exit():
    from app.agents.agent5_order_manager import run_force_exit
    await run_force_exit()

async def _eod_report():
    from app.agents.agent6_auditor import run_eod_report
    await run_eod_report()

async def _watchdog():
    from app.agents.agent7_watchdog import run_health_watchdog
    await run_health_watchdog()


# ─