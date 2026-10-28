"""Frozen cash-account historical scenarios, separate from the legacy replay.

Raw nominal prices execute; retrieved qfq prices drive unchanged five-step rules.
Provider events and ordinary-status assumptions do not certify real execution.
"""
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, ROUND_FLOOR
import math

import pandas as pd
import entry_signals
import exit_signals
import historical_data
import historical_policy
import rule_contract
import rule_inputs

EXPERIMENT = "nominal_cash_five_step_v1"
PARAMETERS = {"initial_cash": 100000.0, "entry_nav_fraction": .20,
              "broker_other_rate": .001, "minimum_fee": 5.0,
              "slippage_fraction": .0005, "dividend_tax_pressure_fraction": .20,
              "holding_subsequent_closes": 10, "holdout_fraction": .40,
              "maximum_observed_session_gap_days": 14,
              "fold_count": 4, "minimum_fold_sessions": 60,
              "same_open_order": "exits_then_entries_code_ascending",
              "baseline": "initial_20pct_each_four_stocks_remaining_cash",
              "unknown_dividend_payment": "receivable_not_spendable",
              "window_end": "mark_only_no_forced_close_fill"}


def money(value):
    return float(Decimal(str(value)).quantize(Decimal(".01"), rounding=ROUND_HALF_UP))


def price_tick(value, side):
    rounding = ROUND_CEILING if side == "buy" else ROUND_FLOOR
    return float(Decimal(str(value)).quantize(Decimal(".01"), rounding=rounding))


def _finite(value):
    try:
        return math.isfinite(float(value))
    except (ValueError, TypeError, OverflowError):
        return False


def metadata():
    payload = {"experiment": EXPERIMENT, "parameters": PARAMETERS,
               "signal_contract": rule_contract.metadata(),
               "policy_sources": [historical_policy.SSE_2023, historical_policy.SZSE_2023,
                   historical_policy.SSE_2026, historical_policy.SZSE_2026, historical_policy.STAMP]}
    return {**payload, "experiment_sha256": historical_data.content_id(payload),
            "evidence_grade": "未验证", "probability_claim_allowed": False,
            "signal_contract_scope": "仅复用入场/退出计算阈值；历史单源未通过当前二源/真实成交资格，不生成当前信号"}


def execution_price(row, side, policy, reference):
    if policy is None:
        return None, "historical_policy_unavailable"
    if row is None or not all(_finite(row.get(key)) for key in ("open", "high", "low", "close", "volume")):
        return None, "daily_ohlcv_missing"
    opening, high, low, close, volume = (float(row[key]) for key in ("open", "high", "low", "close", "volume"))
    if min(opening, high, low, close) <= 0 or volume <= 0:
        return None, "nonpositive_price_or_volume"
    if high < max(opening,close) or low > min(opening,close) or high <= low:
        return None, "inconsistent_or_single_price_bar"
    if not _finite(reference) or reference <= 0:
        return None, "previous_session_or_ex_reference_unknown"
    band = policy["ordinary_limit_fraction"]
    upper, lower = money(reference*(1+band)), money(reference*(1-band))
    if opening > upper+.001 or opening < lower-.001 or high>upper+.001 or low<lower-.001:
        return None, "outside_assumed_ordinary_price_band"
    if (side == "buy" and opening >= upper-.001) or (side == "sell" and opening <= lower+.001):
        return None, "opening_at_adverse_limit_queue_unverified"
    price = price_tick(opening*(1+PARAMETERS["slippage_fraction"] if side=="buy" else 1-PARAMETERS["slippage_fraction"]),side)
    if price <= 0 or price > high+.001 or price < low-.001 or price > upper+.001 or price < lower-.001:
        return None, "slipped_price_outside_daily_range_or_band"
    return price, None


class Inputs:
    def __init__(self, payload):
        self.payload = payload
        self.today = date.fromisoformat(payload["evaluation_date"])
        self.codes = list(payload["fixed_codes"])
        if len(set(self.codes)) != len(self.codes):
            raise ValueError("duplicate research codes")
        self.items = {item["code"]:item for item in payload["inputs"]}
        if set(self.items) != set(self.codes):
            raise ValueError("requested research code denominator changed")
        self.frames, self.rows, self.events, self.entry_cache = {}, {}, {}, {}
        self.coverage, axis = [], set()
        for code in self.codes:
            item = self.items[code]
            self.frames[code], self.rows[code] = {}, {}
            row = {"code":code,"source_input_id":item.get("input_id"),"source_errors":item.get("errors",[]),
                   "event_coverage":item.get("event_coverage","unavailable"),"rejected_rows":{},"contract_errors":[]}
            for kind in ("raw","signal"):
                source = item.get(kind)
                frame = historical_data.frame_from_payload(source) if source else None
                rejected = 0
                if frame is not None:
                    allowed = ("none",) if kind=="raw" else ("qfq","qfq_request_raw_identical_observed")
                    if (frame.attrs.get("adjustment") not in allowed or frame.attrs.get("interval")!="1d"
                            or frame.attrs.get("volume_unit")!="shares"):
                        row["contract_errors"].append(kind+"_basis_or_units_unknown")
                        frame = None
                if frame is not None:
                    valid = frame["date"].notna() & frame["date"].dt.date.lt(self.today)
                    rejected = int((~valid).sum())
                    frame = frame.loc[valid].sort_values("date").reset_index(drop=True)
                    if frame["date"].duplicated().any():
                        rejected += int(frame["date"].duplicated().sum())
                        frame = None  # Conflicting input dates are never arbitrarily resolved.
                self.frames[code][kind] = frame
                mapping = {} if frame is None else {r["date"].date().isoformat():r.to_dict() for _,r in frame.iterrows()}
                self.rows[code][kind] = mapping
                axis.update(mapping)
                row[kind+"_rows"] = len(mapping)
                row["rejected_rows"][kind] = rejected
                if source:
                    row[kind+"_source"] = source.get("attrs",{})
            grouped = {}
            for event in item.get("events",[]):
                grouped.setdefault(event.get("ex_date"),[]).append(event)
            self.events[code] = [events[0] if len(events)==1 else
                {"ex_date":day,"kind":"multiple_records_unresolved","source_records":events}
                for day,events in grouped.items()]
            self.coverage.append(row)
        self.axis = sorted(axis)
        for row in self.coverage:
            code = row["code"]
            for kind in ("raw","signal"):
                missing = [day for day in self.axis if day not in self.rows[code][kind]]
                row[kind+"_missing_sessions"] = len(missing)
                row[kind+"_missing_examples"] = missing[:10]

    def history(self, code, day):
        frame = self.frames[code]["signal"]
        if frame is None:
            return None
        cutoff = date.fromisoformat(day)-timedelta(days=entry_signals.LOOKBACK_CALENDAR_DAYS)
        return frame.loc[frame["date"].dt.date.between(cutoff,date.fromisoformat(day))]

    def entry(self, code, day, evaluate=None):
        key = (code,day)
        if evaluate is None and key in self.entry_cache:
            return self.entry_cache[key]
        history = self.history(code,day)
        if day not in self.rows[code]["signal"]:
            result = {"status":"data_insufficient","as_of":None}
        else:
            result = (evaluate or entry_signals.evaluate_entry_frame)(history,today=date.fromisoformat(day)+timedelta(days=1))
        if result.get("status")=="candidate" and result.get("as_of")!=day:
            result = {"status":"data_insufficient","as_of":None}
        result = dict(result)
        if result.get("status")=="candidate":
            result["rule_input"] = rule_inputs.evidence(code,history,date.fromisoformat(day)+timedelta(days=1))
        if evaluate is None:
            self.entry_cache[key] = result
        return result

    def reference(self, code, day, previous_day):
        prior = self.rows[code]["raw"].get(previous_day)
        if not prior or not _finite(prior.get("close")):
            return None
        reference, ratio = float(prior["close"]), 0.0
        for event in self.events[code]:
            if event.get("ex_date")==day:
                if event.get("kind")!="dividend_bonus" or not all(_finite(event.get(k)) and event[k]>=0 for k in ("cash_per_share","bonus_ratio")):
                    return None
                reference -= float(event["cash_per_share"])
                ratio += float(event["bonus_ratio"])
        return reference/(1+ratio) if reference>0 and ratio>=0 else None


class Replay:
    def __init__(self, inputs, dates, mode="strategy", entry_evaluator=None, exit_evaluator=None):
        self.inputs, self.dates, self.mode = inputs, dates, mode
        self.entry_evaluator, self.exit_evaluator = entry_evaluator, exit_evaluator
        self.cash = PARAMETERS["initial_cash"]
        self.positions, self.pending = {}, []
        self.receivables, self.trades, self.order_events, self.action_events, self.curve = [], [], [], [], []
        self.counts = Counter({key:0 for key in ("entry_signals","entry_fills","completed_trades","entry_unfilled","exit_unfilled_attempts")})
        self.signal_states = {c:Counter() for c in inputs.codes}
        self.processed_actions = set()

    def nav(self, day, column):
        value = self.cash+sum(item["amount"] for item in self.receivables)
        missing = []
        for code,position in self.positions.items():
            row = self.inputs.rows[code]["raw"].get(day)
            if position.get("unresolved") or row is None or not _finite(row.get(column)) or row[column]<=0:
                missing.append(code)
            else:
                value += position["shares"]*float(row[column])
        return (None if missing else money(value)), missing

    def event(self, order, reason, day):
        self.order_events.append({"code":order["code"],"action":order["action"],
            "signal_date":order["signal_date"],"expected_date":day,"reason":reason})
        self.counts["entry_unfilled" if order["action"]=="buy" else "exit_unfilled_attempts"] += 1

    def actions(self, day, previous_day):
        for code,position in self.positions.items():
            for number,source in enumerate(self.inputs.events[code]):
                ex_day = source.get("ex_date")
                key = (code,number)
                if not ex_day or key in self.processed_actions or not previous_day<ex_day<=day:
                    continue
                self.processed_actions.add(key)
                event = {"code":code,"ex_date":ex_day,"kind":source.get("kind"),"shares_before":position["shares"]}
                if (ex_day!=day or source.get("kind")!="dividend_bonus"
                        or not source.get("announcement_date") or source["announcement_date"]>day
                        or source.get("record_date")!=previous_day
                        or not all(_finite(source.get(k)) and source[k]>=0 for k in ("cash_per_share","bonus_ratio"))):
                    position["unresolved"] = True
                    event["status"] = "unresolved_action_or_entitlement"
                    self.action_events.append(event)
                    continue
                gross = money(position["shares"]*source["cash_per_share"])
                net = money(gross*(1-PARAMETERS["dividend_tax_pressure_fraction"]))
                payment = source.get("cash_available_date")
                if payment and payment<ex_day:
                    position["unresolved"] = True
                    event["status"] = "cash_payment_precedes_entitlement"
                    self.action_events.append(event)
                    continue
                if net:
                    self.receivables.append({"code":code,"amount":net,"available_on":payment,"ex_date":ex_day})
                    position["dividend_net_claims"] += net
                bonus = position["shares"]*source["bonus_ratio"]
                if abs(bonus-round(bonus))>1e-7:
                    position["unresolved"] = True
                    event["status"] = "fractional_bonus_unresolved"
                else:
                    addition = int(round(bonus))
                    position["shares"] += addition
                    if addition:
                        position["locked_bonus"].append({"shares":addition,"available_on":source.get("shares_available_date")})
                    event["status"] = "applied_with_provider_evidence"
                event.update({"gross_dividend":gross,"net_receivable":net,"assumed_dividend_tax":money(gross-net),
                              "payment_date":payment,"bonus_added":bonus})
                self.action_events.append(event)

    def execute(self, order, day, previous_day, index):
        code, side = order["code"], order["action"]
        if order["signal_date"]>=day:
            self.event(order,"signal_not_before_execution",day); return
        if (date.fromisoformat(day)-date.fromisoformat(order["signal_date"])).days>PARAMETERS["maximum_observed_session_gap_days"]:
            self.event(order,"observed_session_gap_too_large",day); return
        policy = historical_policy.policy(code,day)
        row = self.inputs.rows[code]["raw"].get(day)
        price, reason = execution_price(row,side,policy,self.inputs.reference(code,day,previous_day))
        if reason:
            self.event(order,reason,day); return
        if self.inputs.items[code].get("event_coverage") not in ("provider_only","verified"):
            self.event(order,"corporate_action_source_unavailable",day); return
        if side=="buy":
            if code in self.positions:
                self.event(order,"position_already_open",day); return
            signal_row = self.inputs.rows[code]["signal"].get(day)
            if not signal_row or not _finite(signal_row.get("open")) or signal_row["open"]<=0:
                self.event(order,"entry_price_basis_unknown",day); return
            nav,_ = self.nav(day,"open")
            if nav is None:
                self.event(order,"entry_risk_budget_unvalued",day); return
            basis = PARAMETERS["initial_cash"] if self.mode=="baseline" else nav
            budget = min(self.cash,basis*PARAMETERS["entry_nav_fraction"])
            shares = historical_policy.affordable(policy,budget,price,PARAMETERS["broker_other_rate"],PARAMETERS["minimum_fee"])
            if not shares:
                self.event(order,"minimum_quantity_and_fee_exceed_budget",day); return
            amount = money(shares*price)
            commission = money(max(PARAMETERS["minimum_fee"],amount*PARAMETERS["broker_other_rate"]))
            self.cash = money(self.cash-amount-commission)
            if self.cash < -.001:
                raise ValueError("research cash became negative")
            self.positions[code] = {"code":code,"shares":shares,"entry_date":day,"entry_index":index,
                "entry_cash":money(amount+commission),"signal_cost":(price+commission/shares)*float(signal_row["open"])/float(row["open"]),
                "entry_input":order.get("rule_input"),"dividend_net_claims":0.0,"locked_bonus":[],"unresolved":False}
            stamp = 0.0
            self.counts["entry_fills"] += 1
        else:
            position = self.positions.get(code)
            if not position:
                self.event(order,"position_absent",day); return
            if day<=position["entry_date"]:
                self.event(order,"T_plus_1",day); return
            if position.get("unresolved"):
                self.event(order,"position_quantity_or_entitlement_unresolved",day); return
            if any(not item["available_on"] or item["available_on"]>day for item in position["locked_bonus"]):
                self.event(order,"bonus_not_confirmed_tradable",day); return
            shares = position["shares"]
            if shares>policy["single_order_cap"]:
                self.event(order,"full_exit_exceeds_single_order_cap",day); return
            amount = money(shares*price)
            commission = money(max(PARAMETERS["minimum_fee"],amount*PARAMETERS["broker_other_rate"]))
            stamp = money(amount*historical_policy.stamp_rate(day))
            self.cash = money(self.cash+amount-commission-stamp)
            economic = money(amount-commission-stamp+position["dividend_net_claims"]-position["entry_cash"])
            self.counts["completed_trades"] += 1
            self.trades.append({"action":"round_trip","code":code,"entry_date":position["entry_date"],"exit_date":day,
                "entry_cash":position["entry_cash"],"sale_net_cash":money(amount-commission-stamp),
                "holding_observed_sessions":index-position["entry_index"],
                "first_expected_exit_date":position.get("first_expected_exit_date"),
                "net_dividend_claims":money(position["dividend_net_claims"]),"economic_pnl":economic,
                "return_pct":round(economic/position["entry_cash"]*100,4),"entry_rule_input":position["entry_input"]})
            del self.positions[code]
        self.trades.append({"action":side,"code":code,"signal_date":order["signal_date"],"date":day,
            "price":price,"shares":shares,"amount":amount,"commission":commission,"stamp_tax":stamp,
            "policy":policy,"rule_input":order.get("rule_input"),"reason":order.get("reason"),
            "source_input_id":self.inputs.items[code].get("input_id")})

    def queue_entries(self, signal_day, fill_day, boundary=False):
        for code in sorted(self.inputs.codes):
            if code in self.positions:
                if not boundary:self.signal_states[code]["held_not_reentered"]+=1
                continue
            signal = self.inputs.entry(code,signal_day,self.entry_evaluator)
            state = signal.get("status","data_insufficient")
            if not boundary:self.signal_states[code][state]+=1
            if state=="candidate":
                self.counts["entry_signals"]+=1
                self.pending.append({"code":code,"action":"buy","signal_date":signal_day,"fill_date":fill_day,
                    "reason":"unchanged_five_step_candidate","rule_input":signal.get("rule_input"),"boundary_signal":boundary})

    def run(self):
        if not self.dates:
            return {"status":"insufficient","error":"no_test_sessions"}
        first_index = self.inputs.axis.index(self.dates[0])
        if first_index==0:
            return {"status":"insufficient","error":"prior_session_missing"}
        previous_day = self.inputs.axis[first_index-1]
        if self.mode=="baseline":
            self.pending = [{"code":code,"action":"buy","signal_date":previous_day,"fill_date":self.dates[0],
                             "reason":"predeclared_equal_initial_allocation"} for code in sorted(self.inputs.codes)]
        else:
            self.queue_entries(previous_day,self.dates[0],boundary=True)
        for index,day in enumerate(self.dates):
            self.actions(day,previous_day)
            pending, self.pending = self.pending, []
            for order in sorted(pending,key=lambda item:(0 if item["action"]=="sell" else 1,item["code"])):
                if order["fill_date"]!=day:
                    self.event(order,"missed_expected_session_not_backfilled",day)
                else:
                    self.execute(order,day,previous_day,index)
            mature = [item for item in self.receivables if item["available_on"] and item["available_on"]<=day]
            self.cash = money(self.cash+sum(item["amount"] for item in mature))
            self.receivables = [item for item in self.receivables if item not in mature]
            nav,missing = self.nav(day,"close")
            self.curve.append({"date":day,"value":nav,"cash":self.cash,
                "receivables":money(sum(item["amount"] for item in self.receivables)),
                "positions":len(self.positions),"valuation_missing_codes":missing})
            fill_day = self.dates[index+1] if index+1<len(self.dates) else None
            if self.mode=="strategy":
                for code,position in self.positions.items():
                    history = self.inputs.history(code,day)
                    holding = {"code":code,"cost_price":position["signal_cost"],"acquired_on":position["entry_date"]}
                    state = ((self.exit_evaluator or exit_signals.evaluate_exit_frame)(history,holding,
                        today=date.fromisoformat(day)+timedelta(days=1)) if day in self.inputs.rows[code]["signal"] else {"status":"data_insufficient"})
                    expired = index-position["entry_index"]>=PARAMETERS["holding_subsequent_closes"]
                    if (state.get("status")=="risk_alert" and state.get("as_of")==day) or expired:
                        if not position.get("first_expected_exit_date") and fill_day:
                            position["first_expected_exit_date"] = fill_day
                        self.pending.append({"code":code,"action":"sell","signal_date":day,"fill_date":fill_day,
                            "rule_input":rule_inputs.evidence(code,history,date.fromisoformat(day)+timedelta(days=1),holding),
                            "timer_context":{"entry_date":position["entry_date"],"observed_sessions":index-position["entry_index"],
                                             "expired":expired,"horizon_subsequent_closes":PARAMETERS["holding_subsequent_closes"]},
                            "reason":"subsequent_10_closes_then_next_open" if expired else "unchanged_five_step_risk"})
                self.queue_entries(day,fill_day)
            previous_day = day
        final = self.curve[-1]["value"]
        complete_curve = all(point["value"] is not None for point in self.curve)
        peak, drawdown = PARAMETERS["initial_cash"], 0.0
        if complete_curve:
            for point in self.curve:
                peak = max(peak,point["value"])
                drawdown = min(drawdown,point["value"]/peak-1)
        fills = [row for row in self.trades if row["action"] in ("buy","sell")]
        return {"status":"conditional_historical_scenario","mode":self.mode,"counts":dict(self.counts),
            "signal_state_counts":{code:dict(states) for code,states in self.signal_states.items()},
            "evaluated_security_days":sum(sum(states.values()) for states in self.signal_states.values()),
            "ending_nav":final,"total_return_pct":round((final/PARAMETERS["initial_cash"]-1)*100,4) if final is not None else None,
            "max_drawdown_pct":round(drawdown*100,4) if complete_curve else None,
            "valuation_missing_days":sum(point["value"] is None for point in self.curve),
            "commission_total":money(sum(row["commission"] for row in fills)),
            "stamp_tax_total":money(sum(row["stamp_tax"] for row in fills)),
            "unsettled_receivables":self.receivables,"open_positions":list(self.positions.values()),
            "pending_at_window_end":self.pending,"trades":self.trades,"order_events":self.order_events,
            "action_events":self.action_events,"curve":self.curve,
            "valuation_note":"收盘名义市值加已发生费用后现金及压力税假设下的应收；未强制卖出，不含未来退出费用"}


def study(payload, progress=None, entry_evaluator=None, exit_evaluator=None):
    inputs = Inputs(payload)
    start = max(entry_signals.MIN_DAILY_ROWS,math.floor(len(inputs.axis)*(1-PARAMETERS["holdout_fraction"])))
    test = inputs.axis[start:]
    specifications = [("whole_holdout",test)]+[("fold_"+str(i+1),test[math.floor(i*len(test)/4):math.floor((i+1)*len(test)/4)]) for i in range(4)]
    windows = []
    for name,dates in specifications:
        row = {"name":name,"test_sessions":len(dates),"start":dates[0] if dates else None,
               "end":dates[-1] if dates else None,"minimum_sessions":PARAMETERS["minimum_fold_sessions"]}
        if len(inputs.axis)<260 or len(dates)<PARAMETERS["minimum_fold_sessions"]:
            row.update({"status":"insufficient","reason":"window_below_predeclared_minimum","strategy":None,"baseline":None,"cash_baseline":None})
        else:
            row.update({"status":"conditional_unverified",
                "strategy":Replay(inputs,dates,"strategy",entry_evaluator,exit_evaluator).run(),
                "baseline":Replay(inputs,dates,"baseline").run(),
                "cash_baseline":{"ending_nav":PARAMETERS["initial_cash"],"total_return_pct":0.0,"max_drawdown_pct":0.0}})
            complete_inputs = all(inputs.frames[code][kind] is not None and len(inputs.frames[code][kind])>0
                                  for code in inputs.codes for kind in ("raw","signal"))
            complete_inputs = complete_inputs and all(inputs.items[code].get("event_coverage") in ("provider_only","verified") for code in inputs.codes)
            row["requested_inputs_present"] = complete_inputs
            if not complete_inputs:
                row["status"] = "partial_input_coverage"
                for mode in ("strategy","baseline"):
                    row[mode]["total_return_pct"] = row[mode]["max_drawdown_pct"] = None
                    row[mode]["metric_unavailable_reason"] = "requested_stock_input_missing_or_invalid"
        windows.append(row)
        if progress:progress("windows",len(windows),len(specifications),"已核对历史区间 "+name)
    return {**metadata(),"evaluation_date":payload["evaluation_date"],"fixed_codes":inputs.codes,
        "input_bundle_id":historical_data.content_id(payload),"calendar":{"source":"observed_union_of_provided_stock_sessions_not_official_calendar",
            "total_sessions":len(inputs.axis),"holdout_start_index":start,"holdout_sessions":len(test)},
        "coverage":inputs.coverage,"windows":windows,"window_denominator":len(windows),
        "fold_denominator":4,"source_audit_notes":[{
            "code":"688981","topic":"2025-09-01至2025-09-08的六个观察日无行情",
            "finding":"发行人公告确认9月1日开市起停牌、9月9日开市复牌；该事后核对未改变冻结执行规则，复牌首日仍因前交易日报价未知而保守阻断",
            "source_url":"https://star.sse.com.cn/disclosure/listedinfo/announcement/c/new/2025-09-09/688981_20250909_2BQ9.pdf",
            "scope":"context_after_result_audit_not_used_to_change_signals_or_fills"}],
        "limitations":["公司行动仅供应商公开表，未独立证明完整性",
            "信号用取回时前复权序列，未消除事后调整和当前股票名单偏差",
            "普通证券状态假设；未逐日核实ST/IPO/券商板块权限/价格笼子/盘口排队",
            "应收分红不提前变可用现金；股息税20%为统一压力假设，非个人实际税率",
            "日线价格范围只能排除明显不可能成交，不能证明开盘交易可执行",
            "期末估值不是假定收盘成交，未包含尚未发生的退出费用；各段独立重置资金"]}
