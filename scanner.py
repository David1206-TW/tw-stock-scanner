# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V30 (修正版：T+1 績效計算)

【修正說明】
- 在計算 ROI 時加入日期檢查。
- 規則：只有當 (今天日期 > 建倉日期) 時，才計算 ROI。
- 目的：避免同日盤中更新時出現虛假的「當日績效」，確保績效是從 T+1 開始計算。

(其餘策略邏輯保持 V29 不變)
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
from datetime import datetime, time as dt_time
import pytz

# ... (前面 import 與 DB 設定保持不變) ...
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

# ... (get_stock_group, get_all_tickers, check_strategy 函式保持不變) ...
# (為節省篇幅，請保留您原本 V29 的策略函式內容)
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

def check_strategy_original(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']
    low = df['Low']
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    ma240 = close.rolling(240).mean()
    vol_ma5 = volume.rolling(5).mean()
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma120 = ma120.iloc[-1] 
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    prev_l = low.iloc[-2]
    curr_l = low.iloc[-1]
    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None
    if curr_vol_ma5 < 500000: return False, None 
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
    if not ((curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)): return False, None
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    if curr_v >= curr_vol_ma5: return False, None
    if curr_c <= curr_ma10: return False, None
    if prev_l > 0:
        low_diff_pct = abs(curr_l - prev_l) / prev_l
        if low_diff_pct > 0.01: return False, None
    return True, {
        "tag": "拉回佈局",
        "price": round(curr_c, 2),
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma240": round(curr_ma240, 2),
        "vol_ratio": round(curr_v / curr_vol_ma5, 2)
    }

def check_strategy_vcp(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    ma240 = close.rolling(240).mean()
    std = close.rolling(20).std()
    bw = ( (ma20 + 2*std) - (ma20 - 2*std) ) / ma20
    vol_ma20 = volume.rolling(20).mean()
    curr_c = close.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma120 = ma120.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_bw = bw.iloc[-1]
    curr_vol_ma20 = vol_ma20.iloc[-1]
    prev_c = close.iloc[-2]
    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
    if not (curr_ma5 > curr_ma10 > curr_ma20): return False, None
    if curr_bw > 0.18: return False, None
    if curr_vol_ma20 < 500000: return False, None
    mas = [curr_ma5, curr_ma10, curr_ma20]
    entangle_pct = (max(mas) - min(mas)) / min(mas)
    if entangle_pct > 0.05: return False, None
    if curr_c <= curr_ma10: return False, None
    if prev_c > 0:
        daily_change = (curr_c - prev_c) / prev_c
        if daily_change > 0.06: return False, None
    return True, {
        "tag": "VCP",
        "price": round(curr_c, 2),
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma240": round(curr_ma240, 2),
        "bw": round(curr_bw * 100, 1)
    }

# ==========================================
# 5. 更新歷史績效 (修正版：加入 T+1 檢查)
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效...")
    
    # 取得台北時間的今天日期 (格式 YYYY/MM/DD)
    tw_tz = pytz.timezone('Asia/Taipei')
    today_str = datetime.now(tw_tz).strftime("%Y/%m/%d")
    today_date = datetime.strptime(today_str, "%Y/%m/%d")

    tickers_to_check = set()
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            tickers_to_check.add(symbol)

    if not tickers_to_check: return history_db

    print(f"追蹤股票數量: {len(tickers_to_check)}")
    current_data = {}
    try:
        data = yf.download(list(tickers_to_check), period="5d", auto_adjust=False, threads=True)
        close_df = data['Close']
        
        if len(tickers_to_check) == 1:
             ticker = list(tickers_to_check)[0]
             closes = close_df.dropna().values
             if len(closes) >= 2:
                 current_data[ticker] = { 'price': float(closes[-1]), 'prev': float(closes[-2]) }
        else:
            for ticker in tickers_to_check:
                try:
                    series = close_df[ticker].dropna()
                    if len(series) >= 2:
                        current_data[ticker] = { 'price': float(series.iloc[-1]), 'prev': float(series.iloc[-2]) }
                except: pass
    except: return history_db

    for date_str, stocks in history_db.items():
        # 解析這批名單的建倉日期
        try:
            entry_date = datetime.strptime(date_str, "%Y/%m/%d")
        except:
            continue

        # 計算持有天數差
        days_diff = (today_date - entry_date).days

        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = stock['buy_price']
                
                # 【關鍵修改】T+1 規則：
                # 如果 (今天 <= 建倉日)，代表是當天建倉，ROI 強制為 0
                if days_diff <= 0:
                    roi = 0.0
                    daily_change = 0.0 # 或者您想看當日漲跌也可以保留計算，但 ROI 建議歸零
                else:
                    roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                    daily_change = round(((latest_price - prev_price) / prev_price) * 100, 2)
                
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                stock['daily_change'] = daily_change

    print("歷史績效更新完成。")
    return history_db

# ==========================================
# 6. 主程式 (V29 雙策略優化版)
# ==========================================
def run_scanner():
    tw_tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tw_tz)
    current_time = now.time()
    market_close_time = dt_time(13, 30)
    is_after_market = current_time >= market_close_time
    
    print(f"目前時間: {now.strftime('%H:%M:%S')}, 收盤後: {is_after_market}")

    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    existing_stock_ids = set()
    for date_str, stocks in history_db.items():
        for s in stocks:
            existing_stock_ids.add(s['id'])
            
    print(f"歷史已追蹤: {len(existing_stock_ids)} 檔")
    print(f"開始雙策略掃描...")
    
    daily_results = []
    new_history_entries = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=False)
            for ticker in batch:
                try:
                    raw_code = ticker.split('.')[0]
                    if len(batch) > 1: df = data[ticker] if ticker in data.columns.levels[0] else pd.DataFrame()
                    else: df = data
                    
                    df = df.dropna()
                    if df.empty: continue

                    is_match_1, info_1 = check_strategy_original(df)
                    is_match_2, info_2 = check_strategy_vcp(df)
                    
                    final_match = False
                    final_info = {}
                    strategy_tags = []

                    if is_match_1:
                        final_match = True
                        final_info = info_1
                        strategy_tags.append("拉回佈局")
                    if is_match_2:
                        final_match = True
                        if not final_info: final_info = info_2
                        strategy_tags.append("VCP")
                    
                    if final_match:
                        name = raw_code
                        if raw_code in twstock.codes: name = twstock.codes[raw_code].name
                        group = get_stock_group(raw_code, industry_db)
                        if raw_code not in industry_db: industry_db[raw_code] = group
                        
                        prev_c = df['Close'].iloc[-2]
                        change_rate = round((final_info['price'] - prev_c) / prev_c * 100, 2)
                        tags_str = " & ".join(strategy_tags)
                        note_ma240 = round(final_info.get('ma240', 0), 2)
                        note_str = f"{tags_str} / 年線{note_ma240}"

                        stock_entry = {
                            "id": raw_code,
                            "name": name,
                            "group": group,
                            "type": "上櫃" if ".TWO" in ticker else "上市",
                            "price": final_info['price'], 
                            "ma5": final_info['ma5'],
                            "ma10": final_info['ma10'],
                            "changeRate": change_rate,
                            "isValid": True,
                            "note": note_str,
                            "buy_price": final_info['price'], 
                            "latest_price": final_info['price'], 
                            "roi": 0.0, 
                            "daily_change": change_rate
                        }
                        
                        daily_results.append(stock_entry)
                        
                        # 僅當非重複且在收盤後，才加入歷史待存區
                        if raw_code not in existing_stock_ids and is_after_market:
                            new_history_entries.append(stock_entry)
                            
                except: continue
        except: continue

    # 更新歷史 ROI (這裡會套用 T+1 規則)
    history_db = update_history_roi(history_db)

    # 寫入新名單
    if new_history_entries and is_after_market:
        today_str = datetime.now().strftime("%Y/%m/%d")
        if today_str in history_db:
             history_db[today_str].extend(new_history_entries)
        else:
             history_db[today_str] = new_history_entries
        print(f"✅ 收盤後掃描：已將 {len(new_history_entries)} 檔新股加入歷史庫。")
        save_json(DB_HISTORY, history_db)
    elif not is_after_market:
        print("⚠️ 盤中執行模式：僅更新即時看板，不寫入歷史績效庫。")

    save_json(DB_INDUSTRY, industry_db)
    
    return daily_results

if __name__ == "__main__":
    results = run_scanner()
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": results
    }
    save_json('data.json', output_payload)


