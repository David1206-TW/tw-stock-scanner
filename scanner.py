# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V9 (高檔洗盤/壓縮版)

【針對痛點】
修正 V8 對 K 棒要求過嚴的問題。
允許高檔整理時出現「十字線」、「上影線」、「小黑K」等洗盤型態，
只要股價守住關鍵支撐 (MA10) 且量縮，都視為蓄勢待發。

【篩選條件說明 (Strategy)】
1. 強勢趨勢 (Strong Trend):
   - MA10 > MA20 > MA60。
   - MA10 維持上揚 (今日MA10 > 昨日MA10)。

2. 高檔蓄勢 (Near High):
   - 收盤價 >= 近 20日最高價 * 0.95 (維持在高檔區)。

3. 波動壓縮 (Squeeze):
   - 近 5 日波動幅度 < 10% (盤整待變)。

4. 量縮整理 (Dry Volume):
   - 今日成交量 < 5日均量 (籌碼沉澱)。

5. 支撐確認 (Support Check) 【關鍵修改】:
   - 收盤價 > MA10：不論紅黑K，只要收盤沒破 10日線，趨勢就沒壞。
   - RSI(6) > 55：允許指標稍微降溫，但仍維持在多方區。
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
# 2. 次產業對照表
# ==========================================
SEED_INDUSTRY_MAP = {
    '3260': '記憶體模組', '8299': 'NAND控制IC', '2408': 'DRAM', '2344': 'DRAM', '2451': '創見(記憶體)',
    '2330': '晶圓代工(AI)', '2317': 'AI伺服器', '3231': 'AI伺服器', '2382': 'AI伺服器', '6669': 'AI伺服器',
    '3661': 'ASIC(IP)', '3443': 'ASIC(IP)', '3035': 'ASIC(IP)', '2356': 'AI伺服器',
    '3017': '散熱模組', '3324': '散熱模組', '3653': '散熱(液冷)', '2421': '散熱',
    '3450': '矽光子', '3363': '矽光子', '4979': '矽光子', '4908': '光通訊', '3081': '光學封裝', '6442': '光聖(光通訊)',
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
# 4. 技術指標計算 (RSI)
# ==========================================
def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# ==========================================
# 5. 策略邏輯核心 (V9 - 高檔洗盤/壓縮版)
# ==========================================
def check_strategy(df):
    if len(df) < 120: return False, {}

    close = df['Close']
    volume = df['Volume']
    high = df['High']
    low = df['Low']
    
    # 計算均線
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    
    # 計算均量
    vol_ma5 = volume.rolling(5).mean()
    
    # 計算 RSI (6日短線動能)
    rsi_6 = calculate_rsi(close, 6)

    # 取得最新數據
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    curr_rsi = rsi_6.iloc[-1]
    
    # 取得昨日數據 (用於判斷斜率)
    prev_ma10 = ma10.iloc[-2]
    prev_c = close.iloc[-2]

    # 【Filter 1】流動性過濾
    if curr_vol_ma5 < 500000: return False, {}

    # --- 策略條件 ---

    # 1. 強勢多頭 (Trend) & 均線斜率 (Slope)
    # MA10 > MA20 > MA60，且 MA10 向上
    cond_trend = (curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60) and (curr_ma10 >= prev_ma10)

    # 2. 高檔蓄勢 (Near High)
    high_20 = high.rolling(20).max().iloc[-1]
    cond_near_high = curr_c >= (high_20 * 0.95)

    # 3. 波動壓縮 (Squeeze)
    # 近 5 天股價波動幅度小於 10%
    recent_high_5 = high.rolling(5).max().iloc[-1]
    recent_low_5 = low.rolling(5).min().iloc[-1]
    volatility_5 = (recent_high_5 - recent_low_5) / curr_c
    cond_squeeze = volatility_5 < 0.10

    # 4. 量縮整理 (Dry Volume)
    cond_vol_dry = curr_v < curr_vol_ma5

    # 5. 支撐確認 (Support Check) - 關鍵修改！
    # A. 收盤價守住 MA10 (允許紅黑K、十字線，只要不破支撐)
    cond_support = curr_c > curr_ma10
    
    # B. RSI(6) > 55 (稍微放寬，允許整理時指標降溫，但不能轉弱)
    cond_rsi_strong = curr_rsi > 55

    # --- 最終判定 ---
    is_match = cond_trend and cond_near_high and cond_squeeze and cond_vol_dry and cond_support and cond_rsi_strong
    
    if is_match:
        change_rate = 0.0
        if prev_c > 0:
            change_rate = round((curr_c - prev_c) / prev_c * 100, 2)
            
        return True, {
            "price": round(curr_c, 2),
            "ma5": round(curr_ma5, 2),
            "ma10": round(curr_ma10, 2),
            "ma240": round(close.rolling(240).mean().iloc[-1], 2) if len(close) > 240 else 0,
            "changeRate": change_rate,
            "vol_ratio": round(curr_v / curr_vol_ma5, 2)
        }
    else:
        return False, {}

# ==========================================
# 6. 批次執行掃描
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
                            "note": f"RSI強勢 / 量縮整理"
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
    print("啟動自動掃描程序 (V9 高檔洗盤/壓縮版)...")
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
