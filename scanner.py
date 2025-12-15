# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V3 (含產業資料庫快取)

【篩選條件說明 (Strategy)】
1. 站上年線 (Life Line): 
   - 條件：收盤價 > 240日均線 (MA240)。
   - 目的：確保長線趨勢偏多，排除空頭走勢的股票。

2. 多頭排列 (Trend): 
   - 條件：20日均線 (月線) > 60日均線 (季線)，且季線呈現上揚趨勢 (今日MA60 > 昨日MA60)。
   - 目的：確認中短期趨勢向上。

3. 均線支撐 (Support): 
   - 條件：收盤價必須同時站上 5日均線 與 10日均線。
   - 目的：確保短線強勢，股價有支撐。

4. 漲多拉回 (Pullback): 
   - 條件：收盤價未創近5日新高 (非突破當下)，且股價距離 5日線 乖離率 < 3%。
   - 目的：尋找回檔整理、回測均線的買點，而非追高。

5. 量縮整理 (Volume): 
   - 條件：今日成交量 < 5日均量。
   - 目的：確認籌碼沉澱，價穩量縮。

【V3 功能升級】
1. 實作「產業資料庫 (industry.json)」：
   - 首次執行會建立資料庫。
   - 之後執行若該股票已在資料庫中，直接讀取，不再重新判斷/爬取。
   - 大幅提升每日掃描速度。
2. 擴充熱門次產業關鍵字。
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
# 當資料庫沒有資料時，優先查這張表
SEED_INDUSTRY_MAP = {
    # --- 記憶體 ---
    '3260': '記憶體模組', '8299': 'NAND控制IC', '2408': 'DRAM', '2344': 'DRAM', '2451': '創見(記憶體)',
    # --- AI / 伺服器 ---
    '2330': '晶圓代工(AI)', '2317': 'AI伺服器', '3231': 'AI伺服器', '2382': 'AI伺服器', '6669': 'AI伺服器',
    '3661': 'ASIC(IP)', '3443': 'ASIC(IP)', '3035': 'ASIC(IP)', '2356': 'AI伺服器',
    # --- 散熱 ---
    '3017': '散熱模組', '3324': '散熱模組', '3653': '散熱(液冷)', '2421': '散熱',
    # --- 矽光子 / CPO ---
    '3450': '矽光子', '3363': '矽光子', '4979': '矽光子', '4908': '光通訊', '3081': '光學封裝',
    # --- 重電 / 綠能 ---
    '1513': '重電(變壓器)', '1519': '重電', '1503': '重電', '1514': '重電', '1609': '電線電纜',
    # --- PCB / CCL ---
    '2383': 'CCL(銅箔基板)', '6274': 'CCL', '6213': 'PCB', '3037': 'ABF載板', '8046': 'PCB', '2368': 'PCB',
    # --- 網通 / 低軌衛星 ---
    '2345': '網通設備', '3704': '網通', '5388': '網通', '2314': '台揚(衛星)',
    # --- 軸承 ---
    '3548': '軸承(摺疊機)', '3376': '軸承', '6805': '軸承'
}

def get_stock_group(code, db_data):
    # 1. [最優先] 檢查資料庫是否已有紀錄 (有就直接回傳，極速!)
    if code in db_data:
        return db_data[code]
    
    # 2. [次要] 查種子對照表
    if code in SEED_INDUSTRY_MAP:
        return SEED_INDUSTRY_MAP[code]
    
    # 3. [最後] 使用 twstock 官方分類
    if code in twstock.codes:
        raw_group = twstock.codes[code].group
        # 簡單處理字串，移除"工業"、"業"讓顯示更簡潔
        return raw_group.replace("工業", "").replace("業", "")
    
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
# 4. 策略邏輯核心
# ==========================================
def check_strategy(df):
    if len(df) < 240: return False, {}

    close = df['Close']
    volume = df['Volume']
    high = df['High']
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    
    vol_ma5 = volume.rolling(5).mean()
    recent_high = high.rolling(5).max()

    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    curr_recent_high = recent_high.iloc[-2]
    prev_c = close.iloc[-2]

    # --- 策略條件 ---
    cond_above_annual = curr_c > curr_ma240 
    cond_trend = (curr_ma20 > curr_ma60) and (curr_ma60 > ma60.iloc[-2]) 
    cond_support = (curr_c > curr_ma5) and (curr_c > curr_ma10) 
    proximity = (curr_c - curr_ma5) / curr_ma5
    cond_pullback = (curr_c < curr_recent_high) and (proximity < 0.03) 
    cond_volume = curr_v < curr_vol_ma5 

    is_match = cond_above_annual and cond_trend and cond_support and cond_pullback and cond_volume
    
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
            "vol_ratio": round(curr_v / curr_vol_ma5, 2)
        }
    else:
        return False, {}

# ==========================================
# 5. 批次執行掃描
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    
    # 載入現有的產業資料庫
    industry_db = load_industry_db()
    print(f"已載入產業資料庫，共 {len(industry_db)} 筆資料。")
    
    print(f"開始下載並分析 {len(full_list)} 檔股票 (需下載2年數據)...")
    
    valid_stocks = []
    batch_size = 100 
    total_batches = (len(full_list) // batch_size) + 1
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{total_batches}...")
        
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
                        
                        # 【核心功能】取得族群分類 (優先查DB -> 查表 -> 查官方)
                        group = get_stock_group(raw_code, industry_db)
                        
                        # 如果資料庫裡沒有這筆，把它加進去 (下次就有了)
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

    # 掃描結束後，將更新後的產業資料庫存檔
    save_industry_db(industry_db)
    print("產業資料庫已更新並儲存。")

    return valid_stocks

# ==========================================
# 主程式
# ==========================================
if __name__ == "__main__":
    print("啟動自動掃描程序...")
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
    print(f"結果已儲存於 {filename}")
