# -*- coding: utf-8 -*-
import yfinance as yf
import pandas as pd
import twstock
import json
import os
from datetime import datetime

# ==========================================
# 1. 取得上市上櫃股票代碼
# ==========================================
def get_all_tickers():
    print("正在獲取台股上市櫃代碼清單...")
    twse = twstock.twse
    tpex = twstock.tpex
    ticker_list = []
    
    # 上市
    for code in twse:
        if len(code) == 4: ticker_list.append(f"{code}.TW")
    # 上櫃
    for code in tpex:
        if len(code) == 4: ticker_list.append(f"{code}.TWO")
            
    print(f"共取得 {len(ticker_list)} 檔股票代碼。")
    return ticker_list

# ==========================================
# 2. 策略邏輯核心
# ==========================================
def check_strategy(df):
    # 資料長度不足無法計算 MA240 (年線)
    if len(df) < 240: return False, {}

    close = df['Close']
    volume = df['Volume']
    high = df['High']
    
    # 計算均線
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    
    # 計算均量 & 近期高點
    vol_ma5 = volume.rolling(5).mean()
    recent_high = high.rolling(5).max()

    # 取得最新一日數據 (iloc[-1])
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    curr_recent_high = recent_high.iloc[-2] # 前一日的近期高點
    prev_c = close.iloc[-2]

    # --- 策略條件 ---
    
    # 1. 站上年線 (多頭生命線)
    cond_above_annual = curr_c > curr_ma240 
    
    # 2. 多頭排列 (月線 > 季線 且 季線向上)
    cond_trend = (curr_ma20 > curr_ma60) and (curr_ma60 > ma60.iloc[-2]) 
    
    # 3. 均線支撐 (收盤價 > 5日線 & 10日線)
    cond_support = (curr_c > curr_ma5) and (curr_c > curr_ma10) 
    
    # 4. 漲多拉回 (未創近5日新高 & 乖離率 < 3%)
    proximity = (curr_c - curr_ma5) / curr_ma5
    cond_pullback = (curr_c < curr_recent_high) and (proximity < 0.03) 
    
    # 5. 量縮 (今日量 < 5日均量)
    cond_volume = curr_v < curr_vol_ma5 

    is_match = cond_above_annual and cond_trend and cond_support and cond_pullback and cond_volume
    
    if is_match:
        return True, {
            "price": round(curr_c, 2),
            "ma5": round(curr_ma5, 2),
            "ma10": round(curr_ma10, 2),
            "ma240": round(curr_ma240, 2),
            "changeRate": round((curr_c - prev_c) / prev_c * 100, 2),
            "vol_ratio": round(curr_v / curr_vol_ma5, 2)
        }
    else:
        return False, {}

# ==========================================
# 3. 批次執行掃描
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    print(f"開始下載並分析 {len(full_list)} 檔股票 (需下載2年數據)...")
    
    valid_stocks = []
    batch_size = 100 
    total_batches = (len(full_list) // batch_size) + 1
    
    # GitHub Actions 不適合用 tqdm 進度條，改用簡單 print
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{total_batches}...")
        
        try:
            # 下載 2 年數據
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
                        # 嘗試取得中文名稱
                        name = raw_code
                        if raw_code in twstock.codes:
                            name = twstock.codes[raw_code].name
                        
                        stock_entry = {
                            "id": raw_code,
                            "name": name,
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
    
    # 儲存為 data.json
    filename = 'data.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)
    
    print(f"掃描完成！共有 {len(results)} 檔符合條件。")
    print(f"結果已儲存於 {filename}")
