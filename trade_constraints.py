"""Reviewed quantity constraints for hypothetical ordinary SH/SZ stock orders.

This is not an exchange order router. Account permissions, daily price limits,
price cages, order-book queues and corporate actions remain unverified.
"""
import math
import re


POLICY_VERSION = "ordinary_sh_sz_quantity_2026_v1"
REVIEWED_ON = "2026-10-25"
EFFECTIVE_FROM = "2026-07-06"
SSE_RULES = "https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml"
SZSE_RULES = "https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf"
STAR_GUIDE = "https://edu.sse.com.cn/tib/qa/"


def profile(code):
    code = str(code)
    if not re.fullmatch(r"\d{6}", code):
        return None
    if code.startswith("688"):
        board, minimum, step, cap, sources = "科创板普通股票", 200, 1, 50000, [SSE_RULES, STAR_GUIDE]
    elif code.startswith(("300", "301")):
        board, minimum, step, cap, sources = "创业板普通股票", 100, 100, 150000, [SZSE_RULES]
    elif code.startswith(("600", "601", "603", "605")):
        board, minimum, step, cap, sources = "沪市主板普通股票", 100, 100, 1000000, [SSE_RULES]
    elif code.startswith(("000", "001", "002", "003")):
        board, minimum, step, cap, sources = "深市主板普通股票", 100, 100, 1000000, [SZSE_RULES]
    else:
        return None
    return {"version": POLICY_VERSION, "board": board,
            "minimum_buy_shares": minimum, "buy_increment": step,
            "single_order_cap": cap, "price_tick": 0.01,
            "effective_from": EFFECTIVE_FROM, "reviewed_on": REVIEWED_ON,
            "sources": sources,
            "scope": "ordinary_stocks_hypothetical_next_open_full_exit",
            "cap_policy": "use_smaller_limit_or_market_order_cap",
            "real_execution_confirmed": False}


def integer_shares(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        if not math.isfinite(number) or number <= 0 or not number.is_integer():
            return None
        return int(number)
    except (TypeError, ValueError, OverflowError):
        return None


def quantity_error(code, side, shares, holding_shares=None):
    rule = profile(code)
    if rule is None:
        return "证券身份或板块交易单位未核实，模拟暂不登记该证券成交"
    quantity = integer_shares(shares)
    if quantity is None:
        return "股数必须为正整数，不能截断小数或把零股数当订单"
    if quantity > rule["single_order_cap"]:
        return "数量超过已核对的单笔模拟上限，须另行核对拆单与流动性"
    if side == "buy":
        minimum, step = rule["minimum_buy_shares"], rule["buy_increment"]
        if quantity < minimum or (quantity - minimum) % step:
            return f"{rule['board']}买入至少{minimum}股，按{step}股递增"
    elif side == "sell":
        if holding_shares is not None and quantity != integer_shares(holding_shares):
            return "持仓数量已变或不是整笔退出；本模拟不静默扩大卖出订单"
        # Full exits include any residual odd shares. Partial exits are not generated.
    else:
        return "买卖方向无效"
    return None


def affordable_shares(code, budget, price, broker_fee_pct, min_commission):
    rule = profile(code)
    if rule is None:
        return 0
    try:
        budget, price, fee, minimum_fee = map(float, (budget, price, broker_fee_pct, min_commission))
        if (not all(math.isfinite(value) for value in (budget, price, fee, minimum_fee))
                or budget <= 0 or price <= 0 or fee < 0 or minimum_fee < 0):
            return 0
        raw = min((budget - minimum_fee) / price,
                  budget / (price * (1 + fee / 100)), rule["single_order_cap"])
        minimum, step = rule["minimum_buy_shares"], rule["buy_increment"]
        if raw < minimum:
            return 0
        quantity = minimum + int((raw - minimum) // step) * step
        while quantity >= minimum:
            amount = round(price * quantity, 2)
            commission = round(max(minimum_fee, amount * fee / 100), 2)
            if amount + commission <= budget + 1e-9:
                return quantity
            quantity -= step
        return 0
    except (TypeError, ValueError, OverflowError):
        return 0
