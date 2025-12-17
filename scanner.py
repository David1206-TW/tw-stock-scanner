# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V24 (雙策略詳盡註釋版)

=============================================================
                          功能說明
=============================================================
1. 雙策略引擎：同時執行「拉回佈局」與「VCP壓縮」掃描。
2. 資料分離：data.json 供當日報價用；history.json 供績效回測用。
3. 即時架構：Python 僅記錄買入成本，最新價和 ROI 由前端 JavaScript 即時查詢計算。

=============================================================
                        策略 A：拉回佈局
=============================================================
   核心概念：趨勢向上但在休息，量縮回檔找買點 (類似旗形整理)。
   
   [長線與位階]
   1. 長線保護 (三線之上)：收盤價 > MA240, 且 > MA120, 且 > MA60。
   2. 多頭排列 (趨勢)：MA10 > MA20 > MA60。
   3. 位階安全：股價距離 MA60 的乖離率 < 25% (避免噴出)。
   
   [整理與進場]
   4. 均線糾結 (壓縮準備)：MA5, MA10, MA20 的最大差異 < 8%。
   5. 量縮整理 (籌碼沉澱)：今日成交量 < 5日均量 (V < V_MA5)。
   6. 支撐確認：收盤價 > MA10。

=============================================================
                        策略 B：VCP 技術面
=============================================================
   核心概念：股價在高檔經過極致波動收縮 (VCP)，均線極度糾結，準備大發動。

   1. 長線保護 (三線之上)：收盤價 > MA240, 且 > MA120, 且 > MA60。
   2. 強勢多頭：MA5 > MA10 > MA20。
   3. 極致壓縮 (布林帶寬)：布林帶寬 (BandWidth) < 12%。
   4. 均線超級糾結：MA5, MA10, MA20 差異 < 2.5%。
   5. 流動性：20日均量 > 500張。
   6. 守住攻擊線：收盤價 > MA10 (隱含在多頭排列中，但強化檢查)。
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
# 2. 產業分類解析邏輯
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

    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None
    if curr_vol_ma5 < 500000: return False, None 

    # 1. 長線保護 (三線之上)
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: 
        return False, None
    
    # 2. 多頭排列
    if not ((curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)): return False, None
    
    # 3. 位階控制 (乖離 < 25%)
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    
    # 4. 均線糾結 (5, 10, 20 差異 < 8%)
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    
    # 5. 量縮整理 (今日量 < 5日均量)
    if curr_v >= curr_vol_ma5: return False, None
    
    # 6. 支撐確認 (收盤 > MA10)
    if curr_c <= curr_ma10: return False, None

    return True, {
        "tag": "拉回佈局",
        "price": round(curr_c, 2),
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma240": round(curr_ma240, 2),
        "vol_ratio": round(curr_v / curr_vol_ma5, 2)
    }

# ==========================================
# 4-B. 策略邏輯：VCP 技術面
# ==========================================
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

    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None

    # 1. 長線保護 (三線之上)
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: 
        return False, None
    
    # 2. 強勢多頭 (5>10>20)
    if not (curr_ma5 > curr_ma10 > curr_ma20): return False, None
    
    # 3. 極致壓縮 (BW < 12%)
    if curr_bw > 0.12: return False, None
    
    # 4. 流動性
    if curr_vol_ma20 < 500000: return False, None
    
    # 5. 超級糾結 (< 2.5%)
    mas = [curr_ma5, curr_ma10, curr_ma20]
    entangle_pct = (max(mas) - min(mas)) / min(mas)
    if entangle_pct > 0.025: return False, None

    # 6. 守住 10 日線
    if curr_c <= curr_ma10: return False, None

    return True, {
        "tag": "VCP",
        "price": round(curr_c, 2),
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma240": round(curr_ma240, 2),
        "bw": round(curr_bw * 100, 1)
    }

# ==========================================
# 5. 更新歷史績效 (移除 ROI 計算，轉移給前端)
# ==========================================
def update_history_roi(history_db):
    print("Python 端只做歷史資料格式化和清洗...")
    # 這裡只確保 history.json 的結構
    for date_str, stocks in history_db.items():
        for stock in stocks:
            if 'latest_price' not in stock:
                stock['latest_price'] = stock['buy_price']
                stock['roi'] = 0.0
                stock['daily_change'] = 0.0
    
    return history_db


# ==========================================
# 6. 主程式
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    existing_stock_ids = set()
    for date_str, stocks in history_db.items():
        for s in stocks:
            existing_stock_ids.add(s['id'])
            
    print(f"歷史已追蹤: {len(existing_stock_ids)} 檔")
    print(f"開始雙策略掃描 (V24 詳盡註釋版)...")
    
    daily_results = []
    new_history_entries = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_tickers]
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
                        
                        tags_str = " & ".join(strategy_tags)
                        note_ma240 = round(final_info.get('ma240', 0), 2)
                        note_str = f"{tags_str} / 年線{note_ma240}"

                        stock_entry = {
                            "id": raw_code,
                            "name": name,
                            "group": group,
                            "type": "上櫃" if ".TWO" in ticker else "上市",
                            "price": final_info['price'], # 昨收價 (待前端覆蓋)
                            "ma5": final_info['ma5'],
                            "ma10": final_info['ma10'],
                            "changeRate": 0.0, # 待前端計算
                            "isValid": True,
                            "note": note_str,
                            "buy_price": final_info['price'], # 買入成本 (昨收價)
                            "latest_price": final_info['price'], # 初始值
                            "roi": 0.0, # 初始值
                            "daily_change": 0.0 # 初始值
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

