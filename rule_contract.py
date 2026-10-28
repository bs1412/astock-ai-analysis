# -*- coding: utf-8 -*-
"""Current five-step rule identity; thresholds are read from their owning modules."""
import hashlib
import json


RULE_VERSION = "five_step_v1"


def parameters():
    # Lazy imports let entry/exit/decision/paper use this module without cycles.
    import entry_signals as entry
    import exit_signals as exit_rule
    import signal_snapshot as quality
    import market_clock

    return {
        "entry": {
            "lookback_calendar_days": entry.LOOKBACK_CALENDAR_DAYS,
            "min_daily_rows": entry.MIN_DAILY_ROWS,
            "weekly_ma_weeks": entry.WEEKLY_MA_WEEKS,
            "weekly_slope_lag_weeks": entry.WEEKLY_SLOPE_LAG_WEEKS,
            "pullback_window_days": entry.PULLBACK_WINDOW_DAYS,
            "pullback_distance": entry.PULLBACK_DISTANCE,
            "zero_axis_window_days": entry.ZERO_AXIS_WINDOW_DAYS,
            "zero_axis_distance": entry.ZERO_AXIS_DISTANCE,
            "volume_base_days": entry.VOLUME_BASE_DAYS,
            "volume_multiple": entry.VOLUME_MULTIPLE,
        },
        "exit": {
            "trail_arm_profit": exit_rule.TRAIL_ARM_PROFIT,
            "trail_drawdown": exit_rule.TRAIL_DRAWDOWN,
            "ma21_break_distance": exit_rule.MA21_BREAK_DISTANCE,
            "ma21_confirm_days": exit_rule.MA21_CONFIRM_DAYS,
            "max_trail_history_days": exit_rule.MAX_TRAIL_HISTORY_DAYS,
        },
        "quality": {
            "max_quote_age_calendar_days": entry.MAX_QUOTE_AGE_DAYS,
            "daily_only_completed_before_today": True,
            "active_signal_requires_independent_source": True,
            "universe_risk_name_policy": "exclude_st_and_delisting_prefix_suffix_v222",
            "quote_adapter_policy": "bj920_sh689_and_sina_ohlcv_qfq_v224",
            "source_daily_requires_registered_trading_day":True,
            "calendar_policy_version":market_clock.POLICY_VERSION,
            "calendar_verified_years":list(market_clock.CALENDAR_YEARS),
            "interval": "1d", "adjustment": "qfq", "volume_unit": "shares",
            "source_names": list(quality.SOURCE_NAMES),
            "close_review_fraction": quality.CLOSE_REVIEW_FRACTION,
            "volume_review_fraction": quality.VOLUME_REVIEW_FRACTION,
        },
    }


def metadata():
    params = parameters()
    encoded = json.dumps(params, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    return {"rule_version": RULE_VERSION,
            "rule_parameters_sha256": hashlib.sha256(encoded).hexdigest(),
            "rule_parameters": params}


def matches(record):
    if not isinstance(record, dict):
        return False
    current = metadata()
    return (record.get("rule_version") == current["rule_version"]
            and record.get("rule_parameters_sha256") == current["rule_parameters_sha256"]
            and record.get("rule_parameters") == current["rule_parameters"])
