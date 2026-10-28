# -*- coding: utf-8 -*-
"""用 plotly 生成 K线+指标+回测权益曲线 的 HTML。"""
import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _indicators(df):
    d = df.copy()
    for c in ("open", "close", "high", "low", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["MA5"] = d["close"].rolling(5).mean()
    d["MA10"] = d["close"].rolling(10).mean()
    d["MA20"] = d["close"].rolling(20).mean()
    ema12 = d["close"].ewm(span=12, adjust=False).mean()
    ema26 = d["close"].ewm(span=26, adjust=False).mean()
    d["DIF"] = ema12 - ema26
    d["DEA"] = d["DIF"].ewm(span=9, adjust=False).mean()
    d["MACD"] = (d["DIF"] - d["DEA"]) * 2
    delta = d["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    d["RSI"] = 100 - 100 / (1 + gain / loss.replace(0, 1e-12))
    return d


def _backtest_curve(df):
    d = _indicators(df).reset_index(drop=True)
    close, ma5, ma20 = d["close"], d["MA5"], d["MA20"]
    pos, eq, curve = 0, 1.0, []
    for i in range(len(d)):
        if i > 0 and pd.notna(ma5.iloc[i]) and pd.notna(ma20.iloc[i]):
            if pos == 0 and ma5.iloc[i - 1] <= ma20.iloc[i - 1] and ma5.iloc[i] > ma20.iloc[i]:
                pos = 1
            elif pos == 1 and ma5.iloc[i - 1] >= ma20.iloc[i - 1] and ma5.iloc[i] < ma20.iloc[i]:
                pos = 0
        if pos == 1 and i > 0:
            eq *= close.iloc[i] / close.iloc[i - 1]
        curve.append(eq)
    return pd.DataFrame({"date": d["date"], "strategy": curve, "buy_hold": (close / close.iloc[0]).values})


def build_chart_html(full_df):
    d = _indicators(full_df)
    k = d.tail(60)
    bt = _backtest_curve(full_df)

    fig = make_subplots(
        rows=5, cols=1, shared_xaxes=False, vertical_spacing=0.03,
        row_heights=[0.42, 0.12, 0.16, 0.12, 0.18],
        subplot_titles=("K线与均线(近60日)", "成交量", "MACD", "RSI(14)", "回测权益曲线(近一年)"),
    )
    fig.add_trace(go.Candlestick(x=k["date"], open=k["open"], high=k["high"], low=k["low"], close=k["close"],
                                 increasing_line_color="#ef4444", decreasing_line_color="#22c55e", name="K线"), 1, 1)
    for ma, color in (("MA5", "#f59e0b"), ("MA10", "#3b82f6"), ("MA20", "#a855f7")):
        fig.add_trace(go.Scatter(x=k["date"], y=k[ma], mode="lines", name=ma, line=dict(width=1, color=color)), 1, 1)

    vc = ["#ef4444" if c >= o else "#22c55e" for c, o in zip(k["close"], k["open"])]
    fig.add_trace(go.Bar(x=k["date"], y=k["volume"], marker_color=vc, name="成交量"), 2, 1)

    mc = ["#ef4444" if v >= 0 else "#22c55e" for v in k["MACD"]]
    fig.add_trace(go.Bar(x=k["date"], y=k["MACD"], marker_color=mc, name="MACD"), 3, 1)
    fig.add_trace(go.Scatter(x=k["date"], y=k["DIF"], mode="lines", name="DIF", line=dict(width=1, color="#f59e0b")), 3, 1)
    fig.add_trace(go.Scatter(x=k["date"], y=k["DEA"], mode="lines", name="DEA", line=dict(width=1, color="#3b82f6")), 3, 1)

    fig.add_trace(go.Scatter(x=k["date"], y=k["RSI"], mode="lines", name="RSI", line=dict(width=1, color="#a855f7")), 4, 1)
    fig.add_hline(y=70, line_dash="dot", line_color="#555", row=4, col=1)
    fig.add_hline(y=30, line_dash="dot", line_color="#555", row=4, col=1)

    fig.add_trace(go.Scatter(x=bt["date"], y=bt["strategy"], mode="lines", name="策略(MA5/20)", line=dict(width=1.5, color="#ef4444")), 5, 1)
    fig.add_trace(go.Scatter(x=bt["date"], y=bt["buy_hold"], mode="lines", name="买入持有", line=dict(width=1.5, color="#9aa3b2")), 5, 1)

    fig.update_xaxes(showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=3, col=1)
    fig.update_xaxes(showticklabels=False, row=4, col=1)
    fig.update_layout(
        height=1120, template="plotly_white", xaxis_rangeslider_visible=False,
        margin=dict(l=45, r=20, t=45, b=20), paper_bgcolor="#ffffff", plot_bgcolor="#ffffff",
        font=dict(color="#192335"), legend=dict(orientation="h", yanchor="bottom", y=1.01),
    )
    return fig.to_html(include_plotlyjs=True if os.getenv("SHARE_APP_MODE") == "1" else "cdn", full_html=True)


def build_minute_chart(code):
    """当日分时走势（新浪1分钟线）"""
    import akshare as ak
    px = "sh" if code[0] in "69" else ("bj" if code[0] in "48" else "sz")
    df = ak.stock_zh_a_minute(symbol=px + code, period="1")
    if df is None or df.empty:
        return None
    last_day = str(df["day"].iloc[-1])[:10]
    d = df[df["day"].astype(str).str.startswith(last_day)]
    fig = go.Figure(go.Scatter(x=d["day"], y=d["close"], mode="lines", name="分时",
                               line=dict(color="#3d8bfd", width=1.3)))
    fig.update_layout(height=320, template="plotly_white", margin=dict(l=40, r=20, t=30, b=20),
                      paper_bgcolor="#ffffff", plot_bgcolor="#ffffff", font=dict(color="#192335"),
                      title=dict(text=f"{code} 分时走势（{last_day}）", font=dict(size=14)))
    return fig.to_html(include_plotlyjs=True if os.getenv("SHARE_APP_MODE") == "1" else "cdn", full_html=True)
