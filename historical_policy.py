"""Dated ordinary-stock research assumptions; never certifies actual fills."""
from datetime import date
import math
import re

SSE_2023 = "https://www.sse.com.cn/lawandrules/sselawsrules2025/repeal/rules/c/c_20250612_10824490.shtml"
SZSE_2023 = "https://investor.szse.cn/lawrules/index/rule/t20230217_598773.html"
START_EVIDENCE = "https://www.csrc.gov.cn/shanghai/c105566/c7402810/content.shtml"
STAR = "https://edu.sse.com.cn/tib/qa/c/4761956.shtml"
CHINEXT = "https://investor.szse.cn/knowledge/t20200819_580804.html"
SSE_2026 = "https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml"
SZSE_2026 = "https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf"
STAMP = "https://m.mof.gov.cn/czxw/202308/t20230827_3904226.htm"


def policy(code, execution_date):
    text = str(execution_date)
    date.fromisoformat(text)
    code = str(code)
    if not re.fullmatch(r"\d{6}", code) or text < "2023-04-10":
        return None
    if code.startswith("688"):
        board, minimum, step, cap, band, evidence = "STAR", 200, 1, 50000, .20, STAR
    elif code.startswith(("300", "301")):
        board, minimum, step, cap, band, evidence = "CHINEXT", 100, 100, 150000, .20, CHINEXT
    elif code.startswith(("600", "601", "603", "605")):
        board, minimum, step, cap, band, evidence = "SH_MAIN", 100, 100, 1000000, .10, SSE_2023
    elif code.startswith(("000", "001", "002", "003")):
        board, minimum, step, cap, band, evidence = "SZ_MAIN", 100, 100, 1000000, .10, SZSE_2023
    else:
        return None
    current = text >= "2026-07-06"
    return {"version": "ordinary_2026_research_v1" if current else "ordinary_2023_research_v1",
            "valid_from": "2026-07-06" if current else "2023-04-10",
            "valid_until": None if current else "2026-07-05", "board": board,
            "minimum_buy_shares": minimum, "buy_increment": step, "single_order_cap": cap,
            "price_tick": .01, "ordinary_limit_fraction": band,
            "source_urls": [SSE_2026 if current and code.startswith("6") else SZSE_2026 if current else evidence, evidence, START_EVIDENCE],
            "scope": "ordinary_status_assumed_ipo_st_permissions_price_cage_unverified",
            "cap_policy": "smaller_limit_or_market_order_cap", "real_execution_confirmed": False}


def stamp_rate(execution_date):
    text = str(execution_date)
    date.fromisoformat(text)
    return .001 if text < "2023-08-28" else .0005


def affordable(policy_record, budget, price, rate, minimum_fee):
    if policy_record is None or not all(math.isfinite(v) for v in (budget, price, rate, minimum_fee)) or price <= 0 or budget <= 0 or rate<0 or minimum_fee<0:
        return 0
    minimum, step = policy_record["minimum_buy_shares"], policy_record["buy_increment"]
    raw = min((budget-minimum_fee)/price, budget/(price*(1+rate)), policy_record["single_order_cap"])
    if raw < minimum:
        return 0
    quantity = minimum+math.floor((raw-minimum)/step)*step
    while quantity >= minimum:
        amount = round(quantity*price, 2)
        if amount+round(max(minimum_fee, amount*rate), 2) <= budget+1e-8:
            return quantity
        quantity -= step
    return 0
