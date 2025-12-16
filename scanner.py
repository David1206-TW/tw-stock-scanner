# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V5 (含流動性過濾)

【基本過濾 (Filter)】
- 流動性：5日均量必須 > 500張 (避免成交量過低的冷門股)。

【篩選條件說明 (Strategy)】
1. 站上年線 (Life Line): 
   - 條件：收盤價 > 240日均線 (MA240)。
   - 目的：確保長線趨勢偏多。

2. 多頭排列 (Trend): 
   - 條件：20日均線 (月線) > 60日均線 (季線)。
   - 條件：5日均線 (周線) > 20日均線 (月線)。
   - 目的：確認短中長期均線同步向上。

3. 均線支撐 (Support): 
   - 條件：收盤價必須同時站上 5日均線 與 10日均線。
   - 條件：連續3日日K「最低點」差異在 ±1% 內 (打樁確認)。
   - 目的：確保短線強勢，且底部有特定買盤防守。

4. 漲多拉回 (Pullback): 
   - 條件：收盤價未創近5日新高。
   - 條件：股價距離 5日線 乖離率 < 3%。
   - 目的：尋找回檔整理買點。

5. 量縮整理 (Volume): 
   - 條件：今日成交量 < 3日均量。
   - 目的：確認籌碼沉澱。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import time
from datetime import datetime

# ==========================================
# 1. [核心] 產業資料庫管理
# ==========================================
DB_FILENAME = 'industry.json'

def load_industry_db():
    if os.path.exists(DB_FILENAME):
        try:
            with open(DB_FILENAME, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_industry_db(db_data):
    with open(DB_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(db_data, f, ensure_ascii=False, indent=2)

# ==========================================
# 2. 次產業對照表 (種子資料)
# ==========================================
SEED_INDUSTRY_MAP = {
    '3260': '記憶體模組', '8299': 'NAND控制IC', '2408': 'DRAM', '2344': 'DRAM', '2451': '創見(記憶體)',
    '2330': '晶圓代工(AI)', '2317': 'AI伺服器', '3231': 'AI伺服器', '2382': 'AI伺服器', '6669': 'AI伺服器',
    '3661': 'ASIC(IP)', '3443': 'ASIC(IP)', '3035': 'ASIC(IP)', '2356': 'AI伺服器',
    '3017': '散熱模組', '3324': '散熱模組', '3653': '散熱(液冷)', '2421': '散熱',
    '3450': '矽光子', '3363': '矽光子', '4979': '矽光子', '4908': '光通訊', '3081': '光學封裝',
    '1513': '重電(變壓器)', '1519': '重電', '1503': '重電', '1514': '重電', '1609': '電線電纜',
    '2383': 'CCL(銅箔基板)', '6274': 'CCL', '6213': 'PCB', '3037': 'ABF載板', '8046': 'PCB', '2368': 'PCB',
    '2345': '網通設備', '3704': '網通', '5388': '網通', '2314': '台揚(衛星)',
    '3548': '軸承(摺疊機)', '3376': '軸承', '6805': '軸承'
}

def get_stock_group(code, db_data):
    if code in db_data: return db_data[code]
    if code in SEED_INDUSTRY_MAP: return SEED_INDUSTRY_MAP[code]
    if code in twstock.codes:
        return twstock.codes[code].group.replace("工業", "").replace("業", "")
    return "其他"

# ==========================================
# 3. 取得上市上櫃股票代碼
# ==========================================
def get_all_tickers():
    print("正在獲取台股上市櫃代碼清單...")
    twse = twstock.twse
    tpex = twstock.tpex
    ticker_list = []
    
    for code in twse:
        if len(code) == 4: ticker_list.append(f"{code}.TW")
    for code in tpex:
        if len(code) == 4: ticker_list.append(f"{code}.TWO")
            
    print(f"共取得 {len(ticker_list)} 檔股票代碼。")
    return ticker_list

# ==========================================
# 4. 策略邏輯核心 (Updated V5)
# ==========================================
def check_strategy(df):
    # 資料長度不足無法計算 MA240
    if len(df) < 240: return False, {}

    close = df['Close']
    volume = df['Volume']
    high = df['High']
    low = df['Low'] 
    
    # 計算成交量均線 (用來過濾垃圾股)
    vol_ma5 = volume.rolling(5).mean()
    curr_vol_ma5 = vol_ma5.iloc[-1]

    # 【新增】流動性過濾：5日均量 < 500張 (500,000股) 直接淘汰
    if curr_vol_ma5 < 500000:
        return False, {}

    # 計算價格均線
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    
    # 計算 3日均量 (策略用)
    vol_ma3 = volume.rolling(3).mean()
    
    # 近 5 日最高價
    recent_high = high.rolling(5).max()

    # 取得最新數據 (t)
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma3 = vol_ma3.iloc[-1]
    curr_recent_high = recent_high.iloc[-2] 
    
    # 取得昨日收盤價
    prev_c = close.iloc[-2]

    # --- 策略條件判斷 ---
    
    # 1. 站上年線 (Life Line)
    cond_life_line = curr_c > curr_ma240
    
    # 2. 多頭排列 (Trend)
    # MA20 > MA60 且 MA5 > MA20 
    cond_trend = (curr_ma20 > curr_ma60) and (curr_ma5 > curr_ma20)
    
    # 3. 均線支撐 (Support)
    # A. 站上 MA5 & MA10
    cond_ma_support = (curr_c > curr_ma5) and (curr_c > curr_ma10)
    
    # B. 連續 3 日最低價差異 < 1%
    if len(low) < 3: return False, {}
    l0, l1, l2 = low.iloc[-1], low.iloc[-2], low.iloc[-3]
    diff_1 = abs(l0 - l1) / l1
    diff_2 = abs(l1 - l2) / l2
    cond_low_stable = (diff_1 < 0.01) and (diff_2 < 0.01)
    
    cond_support = cond_ma_support and cond_low_stable
    
    # 4. 漲多拉回 (Pullback)
    proximity = (curr_c - curr_ma5) / curr_ma5
    cond_pullback = (curr_c < curr_recent_high) and (proximity < 0.03)
    
    # 5. 量縮整理 (Volume) - 小於 3日均量
    cond_volume = curr_v < curr_vol_ma3

    # --- 最終判定 ---
    is_match = cond_life_line and cond_trend and cond_support and cond_pullback and cond_volume
    
    if is_match:
        change_rate = 0.0
        if prev_c > 0:
            change_rate = round((curr_c - prev_c) / prev_c * 100, 2)
            
        return True, {
            "price": round(curr_c, 2),
            "ma5": round(curr_ma5, 2),
            "ma10": round(curr_ma10, 2),
            "ma240": round(curr_ma240, 2),
            "changeRate": change_rate,
            "vol_ratio": round(curr_v / curr_vol_ma3, 2)
        }
    else:
        return False, {}

# ==========================================
# 5. 批次執行掃描
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    
    industry_db = load_industry_db()
    print(f"已載入產業資料庫，共 {len(industry_db)} 筆資料。")
    print(f"開始下載並分析 {len(full_list)} 檔股票 (需下載2年數據)...")
    
    valid_stocks = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}...")
        
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False)
            
            for ticker in batch:
                try:
                    if len(batch) > 1:
                        df = data[ticker] if ticker in data.columns.levels[0] else pd.DataFrame()
                    else:
                        df = data
                    
                    df = df.dropna()
                    if df.empty: continue
                        
                    match, info = check_strategy(df)
                    
                    if match:
                        raw_code = ticker.split('.')[0]
                        name = raw_code
                        if raw_code in twstock.codes:
                            name = twstock.codes[raw_code].name
                        
                        group = get_stock_group(raw_code, industry_db)
                        if raw_code not in industry_db:
                            industry_db[raw_code] = group
                        
                        stock_entry = {
                            "id": raw_code,
                            "name": name,
                            "group": group,
                            "type": "上櫃" if ".TWO" in ticker else "上市",
                            "price": info['price'],
                            "ma5": info['ma5'],
                            "ma10": info['ma10'],
                            "changeRate": info['changeRate'],
                            "isValid": True,
                            "note": f"量比{info['vol_ratio']} / 年線{info['ma240']}"
                        }
                        valid_stocks.append(stock_entry)
                except: continue
        except: continue

    save_industry_db(industry_db)
    print("產業資料庫已更新並儲存。")

    return valid_stocks

# ==========================================
# 主程式
# ==========================================
if __name__ == "__main__":
    print("啟動自動掃描程序 (V5 含流動性過濾)...")
    results = run_scanner()
    
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions Auto Scan",
        "list": results
    }
    
    filename = 'data.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)
    
    print(f"掃描完成！共有 {len(results)} 檔符合條件。")
