# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V15 (雙策略引擎版)

【功能升級】
1. 整合雙策略：同時執行「拉回佈局(Original)」與「VCP壓縮(New)」掃描。
2. 效率優化：單次下載數據，同時進行兩種邏輯運算。

【策略 A：拉回佈局 (Original Trend & Pullback)】
1. 收盤 > MA240, MA10 > MA20 > MA60。
2. 乖離率 < 25%, 均線糾結 < 8%。
3. 量縮整理 (今日量 < 5日均量), 收盤 > MA10。

【策略 B：VCP 技術面 (Volatility Contraction Pattern)】
1. 多頭排列: MA5 > MA10 > MA20, 收盤 > MA240。
2. 極致壓縮: 布林帶寬 (BandWidth) < 12%。
3. 均線糾結: MA5/10/20 差異 < 2.5%。
4. 流動性: 20日均量 > 500張。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
from datetime import datetime

# ==========================================
# 1. 資料庫管理
# ==========================================
# 【修改】改用您的 CMoney 產業快取檔
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
# 2. 次產業對照表 (種子資料)
# ==========================================
SEED_INDUSTRY_MAP = {
    # 【修改】已移除硬編碼資料，全面使用 cmoney_industry_cache.json
}

def get_stock_group(code, db_data):
    # 1. 優先查您的 cmoney_industry_cache.json (已載入至 db_data)
    if code in db_data: return db_data[code]
    
    # 2. 查種子表 (目前為空)
    if code in SEED_INDUSTRY_MAP: return SEED_INDUSTRY_MAP[code]
    
    # 3. 若都沒找到，才使用 twstock 官方大分類
    if code in twstock.codes:
        return twstock.codes[code].group.replace("工業", "").replace("業", "")
    return "其他"

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
# 4-A. 策略邏輯：原版 (Original)
# ==========================================
def check_strategy_original(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    vol_ma5 = volume.rolling(5).mean()
    
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]

    if math.isnan(curr_ma240) or curr_ma240 <= 0: return False, None
    if curr_vol_ma5 < 500000: return False, None # 500張

    # 條件
    cond_life_line = curr_c > curr_ma240
    cond_trend = (curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    cond_not_too_high = bias_ma60 < 0.25
    
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    cond_consolidation = ma_divergence < 0.08
    
    # 【確認新增】量縮整理：今日成交量 < 5日均量
    cond_vol_dry = curr_v < curr_vol_ma5
    
    cond_support = curr_c > curr_ma10

    is_match = cond_life_line and cond_trend and cond_not_too_high and cond_consolidation and cond_vol_dry and cond_support
    
    if is_match:
        return True, {
            "tag": "拉回佈局",
            "price": round(curr_c, 2),
            "ma5": round(curr_ma5, 2),
            "ma10": round(curr_ma10, 2),
            "ma240": round(curr_ma240, 2),
            "vol_ratio": round(curr_v / curr_vol_ma5, 2)
        }
    return False, None

# ==========================================
# 4-B. 策略邏輯：VCP 技術面 (New)
# ==========================================
def check_strategy_vcp(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']

    # 計算指標
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma240 = close.rolling(240).mean()
    
    # 布林帶寬 (BandWidth)
    # BW = (Upper - Lower) / Middle
    std = close.rolling(20).std()
    bw = ( (ma20 + 2*std) - (ma20 - 2*std) ) / ma20
    
    # 20日均量
    vol_ma20 = volume.rolling(20).mean()

    # 取得最新值
    curr_c = close.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_bw = bw.iloc[-1]
    curr_vol_ma20 = vol_ma20.iloc[-1]

    if math.isnan(curr_ma240) or curr_ma240 <= 0: return False, None

    # --- VCP 策略條件 ---
    
    # 1. 守住 10 日線
    if curr_c < curr_ma10: return False, None
    
    # 2. 強勢多頭排列 (5 > 10 > 20)
    if not (curr_ma5 > curr_ma10 > curr_ma20): return False, None
    
    # 3. 站上年線 (240MA)
    if curr_c <= curr_ma240: return False, None
    
    # 4. VCP 極致壓縮 (BandWidth < 12%)
    if curr_bw > 0.12: return False, None
    
    # 5. 成交量濾網 (20日均量 > 500張)
    if curr_vol_ma20 < 500000: return False, None
    
    # 6. 均線超級糾結 (2.5% 以內)
    mas = [curr_ma5, curr_ma10, curr_ma20]
    entangle_pct = (max(mas) - min(mas)) / min(mas)
    if entangle_pct > 0.025: return False, None

    return True, {
        "tag": "VCP壓縮",
        "price": round(curr_c, 2),
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma240": round(curr_ma240, 2),
        "bw": round(curr_bw * 100, 1) # 帶寬百分比
    }

# ==========================================
# 5. 更新歷史績效
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效...")
    tickers_to_check = set()
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id']
            if stock['type'] == '上市': symbol += '.TW'
            else: symbol += '.TWO'
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
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = stock['buy_price']
                
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = round(((latest_price - buy_price) / buy_price) * 100, 2)
                stock['daily_change'] = round(((latest_price - prev_price) / prev_price) * 100, 2)

    print("歷史績效更新完成。")
    return history_db

# ==========================================
# 6. 主程式
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    # 建立歷史名單快取 (防止重複建倉)
    existing_stock_ids = set()
    for date_str, stocks in history_db.items():
        for s in stocks:
            existing_stock_ids.add(s['id'])
            
    print(f"歷史追蹤: {len(existing_stock_ids)} 檔")
    print(f"開始雙策略掃描 (Original + VCP)...")
    
    daily_results = []
    new_history_entries = []
    
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}...")
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=False)
            for ticker in batch:
                try:
                    raw_code = ticker.split('.')[0]
                    if len(batch) > 1: df = data[ticker] if ticker in data.columns.levels[0] else pd.DataFrame()
                    else: df = data
                    
                    df = df.dropna()
                    if df.empty: continue

                    # --- 雙策略引擎 ---
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
                        # 如果原本沒匹配 A 策略，就用 B 策略的 info
                        if not final_info: final_info = info_2
                        strategy_tags.append("VCP")
                    
                    if final_match:
                        name = raw_code
                        if raw_code in twstock.codes: name = twstock.codes[raw_code].name
                        group = get_stock_group(raw_code, industry_db)
                        if raw_code not in industry_db: industry_db[raw_code] = group
                        
                        # 取得漲跌幅
                        prev_c = df['Close'].iloc[-2]
                        change_rate = round((final_info['price'] - prev_c) / prev_c * 100, 2)

                        # 組合備註
                        tags_str = " & ".join(strategy_tags)
                        note_str = f"{tags_str} / 年線{final_info['ma240']}"
                        
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
                            # 歷史欄位
                            "buy_price": final_info['price'],
                            "latest_price": final_info['price'],
                            "roi": 0.0,
                            "daily_change": change_rate
                        }
                        
                        daily_results.append(stock_entry)
                        
                        if raw_code not in existing_stock_ids:
                            new_history_entries.append(stock_entry)
                            
                except: continue
        except: continue

    history_db = update_history_roi(history_db)

    if new_history_entries:
        today_str = datetime.now().strftime("%Y/%m/%d")
        if today_str in history_db:
             history_db[today_str].extend(new_history_entries)
        else:
             history_db[today_str] = new_history_entries
        print(f"今日新納入歷史庫: {len(new_history_entries)} 檔")

    save_json(DB_INDUSTRY, industry_db)
    save_json(DB_HISTORY, history_db)
    
    return daily_results

if __name__ == "__main__":
    results = run_scanner()
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": results
    }
    save_json('data.json', output_payload)
