# -*- coding: utf-8 -*-
"""自选池：纯行情跟踪列表，本地JSON存储，不含成本。"""
import json
import os
import concurrent.futures

import holdings
import private_storage
import threading
from functools import wraps

BASE = os.path.dirname(os.path.abspath(__file__))
WATCH_FILE = os.path.join(BASE, "watchlist.json")
_WRITE_LOCK = threading.RLock()


def _serialized(function):
    @wraps(function)
    def guarded(*args, **kwargs):
        with _WRITE_LOCK:
            return function(*args, **kwargs)
    return guarded


def load():
    return private_storage.read(WATCH_FILE, [])


def save(items):
    private_storage.write(WATCH_FILE, items)


@_serialized
def add(code):
    items = load()
    if not any(x.get("code") == code for x in items):
        items.append({"code": code})
        save(items)
    return load()


@_serialized
def remove(code):
    items = [x for x in load() if x.get("code") != code]
    save(items)
    return items


def quotes():
    items = load()

    def quote(item):
        public=holdings.public_quote(item['code'])
        price,name,prev,source_time,retrieved_at=(public['price'],public['name'],public['previous_close'],public['source_time'],public['retrieved_at'])
        return {
            "code": item["code"],
            "name": name or item["code"],
            "price": round(price, 2) if price is not None else None,
            "change_pct": round((price / prev - 1) * 100, 2) if price is not None and prev else None,
            "price_trade_date": source_time[:10] if source_time else None,
            "price_source_time": source_time,
            "price_retrieved_at": retrieved_at,
            "price_evidence": public['price_evidence'], "market_time": public['market_time'],
        }

    if not items:
        return []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(items))) as pool:
        return list(pool.map(quote, items))
