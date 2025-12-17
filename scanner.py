# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V30 (邏輯確認版)

【V30 策略確認】
1. 拉回策略：使用 abs() 函數確保「底部打樁」條件包含 ±1% 內的波動 (不論微漲或微跌)。
2. VCP 策略：維持漲幅 < 6% 限制。
3. 時間管制：維持 13:30 收盤後才寫入歷史。

【策略 A：拉回佈局】
   ... (前6項同前) ...
   7. 【打樁確認】：|今日最低 - 昨日最低| / 昨日最低 <= 1% (±1% 內)。

【策略 B：VCP 技術面】
   ... (前6項同前) ...
   7. 【避免追高】：當日漲幅 <= 6%。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
from datetime import datetime, time as dt_time
import pytz

# ==========================================
# 1. 資料庫管理
# ==========================================
DB_INDUSTRY = 'cmoney_industry_cache.json'
DB_HISTORY = 'history.json' 

def load_json(filename):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ==========================================
# 2. 產業分類解析
# ==========================================
SEED_INDUSTRY_MAP = {} 

def get_stock_group(code, db_data):
    group = "其他"
    if code in db_data:
        raw_data = db_data[code]
        if isinstance(raw_data, dict):
            if 'sub' in raw_data and raw_data['sub']: group = raw_data['sub']
            elif 'main' in raw_data and raw_data['main']: group = raw_data['main']
            elif 'industry' in raw_data: group = raw_data['industry']
        elif isinstance(raw_data, str):
            group = raw_data
    elif code in twstock.codes:
        group = twstock.codes[code].group.replace("工業", "").replace("業", "")
    if not isinstance(group, str): group = str(group)
    return group

def get_all_tickers():
    twse = twstock.twse
    tpex = twstock.tpex
    ticker_list = []
    for code in twse:
        if len(code) == 4: ticker_list.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4: ticker_list.append(f"{code}.TWO")
    return ticker_list

# ==========================================
# 4-A. 策略邏輯：拉回佈局 (Original)
# ==========================================
def check_strategy_original(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']
    low = df['Low'] # 取得最低價
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    ma240 = close.rolling(240).mean()
    vol_ma5 = volume.rolling(5).mean()
    
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_low = low.iloc[-1] # 今日最低
    
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma120 = ma120.iloc[-1] 
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    
    prev_low = low.iloc[-2] # 昨日最低

    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None
    if curr_vol_ma5 < 500000: return False, None 

    # 1. 長線保護
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: 
        return False, None
    
    # 2. 多頭排列
    if not ((curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)): return False, None
    
    # 3. 位階控制
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    
    # 4. 均線糾結
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    
    # 5. 量縮整理
    if curr_v >= curr_vol_ma5: return False, None
    
    #


