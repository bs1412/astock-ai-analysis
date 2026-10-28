# -*- coding: utf-8 -*-
"""本地配置：DeepSeek Key 等，config.json 优先，其次环境变量。"""
import json
import os
import threading
import private_storage

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, "config.json")
_LOCK = threading.Lock()


def _read():
    return private_storage.read(CONFIG_FILE, {})


def get_key():
    return get("deepseek_key") or os.getenv("DEEPSEEK_API_KEY", "")


def set_key(key):
    set("deepseek_key", key.strip())


def get(key, default=None):
    with _LOCK:
        return _read().get(key, default)


def set(key, value):
    update({key: value})


def update(values):
    with _LOCK:
        cfg = _read()
        cfg.update(values)
        private_storage.write(CONFIG_FILE, cfg)
