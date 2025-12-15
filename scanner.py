# -*- coding: utf-8 -*-
"""
Colab_Strategy_Scanner_v9.1
功能：策略掃描 + 自動產出 JSON 檔案下載
策略：多頭 + 漲多拉回 + 量縮 + 均線支撐 + 站上年線(240MA)
"""

!pip install yfinance twstock pandas tqdm -q

import yfinance as yf
import pandas as pd
import twstock
import json
import os
from datetime import datetime
from tqdm import tqdm

# ==========================================
# 1. 取得上市上櫃股票代碼
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
# 2. 策略邏輯核心
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

    # 最新一日數據
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
    cond_above_annual = curr_c > curr_ma240 # 站上年線
    cond_trend = (curr_ma20 > curr_ma60) and (curr_ma60 > ma60.iloc[-2]) # 多頭排列
    cond_support = (curr_c > curr_ma5) and (curr_c > curr_ma10) # 均線支撐
    
    proximity = (curr_c - curr_ma5) / curr_ma5
    cond_pullback = (curr_c < curr_recent_high) and (proximity < 0.03) # 漲多拉回
    
    cond_volume = curr_v < curr_vol_ma5 # 量縮

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
    print(f"開始下載並分析 {len(full_list)} 檔股票 (需下載2年數據計算年線)...")
    
    valid_stocks = []
    batch_size = 100 # 批次大小
    
    with tqdm(total=len(full_list)) as pbar:
        for i in range(0, len(full_list), batch_size):
            batch = full_list[i:i+batch_size]
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
                            name = twstock.codes[raw_code].name if raw_code in twstock.codes else raw_code
                            
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
            finally: pbar.update(len(batch))

    return valid_stocks

# ==========================================
# 主程式：加入自動下載功能
# ==========================================
if __name__ == "__main__":
    results = run_scanner()
    
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "Colab Scanner v9.1 (File Download)",
        "list": results
    }
    
    # 1. 存為檔案
    filename = 'stock_strategy_list.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)
    
    print("\n" + "="*50)
    print(f"掃描完成！共有 {len(results)} 檔符合條件。")
    print(f"正在自動下載檔案：{filename} ...")
    print("="*50)

    # 2. 觸發 Colab 瀏覽器下載
    try:
        from google.colab import files
        files.download(filename)
    except ImportError:
        print("非 Colab 環境，檔案已儲存於本地目錄，請手動開啟。")
