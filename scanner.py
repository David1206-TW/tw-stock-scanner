# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V6 (強勢線性趨勢版)

【策略核心：抓取沿著均線爬升的線性強勢股】

【篩選條件說明】
1. 基本門檻 (Filter):
   - 5日均量 > 500張 (剔除殭屍股)。
   - 收盤價 > 年線 (MA240) (長線多頭保護)。

2. 均線完美排列 (Perfect Order):
   - 條件：收盤價 > MA5 > MA10 > MA20 > MA60。
   - 目的：這是「線性上漲」最強烈的特徵，代表短、中、長期趨勢一致向上。

3. 攻擊態勢 (Momentum):
   - 條件：MA5 與 MA10 必須呈現上揚 (今日 > 昨日)。
   - 目的：確保目前股價正在攻擊狀態，而非多頭架構下的盤整。

4. 線性乖離控制 (Linearity):
   - 條件：(收盤價 - MA10) / MA10 < 8%。
   - 目的：我們要找的是「沿著均線爬」的股票，而不是已經噴出乖離過大的股票(避免追高)。
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
# 4. 策略邏輯核心 (V6 - 強勢線性趨勢)
# ==========================================
def check_strategy(df):
    # 資料長度不足無法計算 MA240
    if len(df) < 240: return False, {}

    close = df['Close']
    volume = df['Volume']
    
    # 計算成交量均線 (流動性濾網)
    vol_ma5 = volume.rolling(5).mean()
    curr_vol_ma5 = vol_ma5.iloc[-1]

    # 【濾網 1】流動性：5日均量 < 500張 (500,000股) 淘汰
    if curr_vol_ma5 < 500000:
        return False, {}

    # 計算價格均線
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    
    # 取得最新數據 (t)
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    
    # 取得昨日數據 (t-1) 用於判斷均線方向
    prev_ma5 = ma5.iloc[-2]
    prev_ma10 = ma10.iloc[-2]
    prev_c = close.iloc[-2]

    # --- 策略條件判斷 (尋找線性強勢股) ---
    
    # 1. 均線完美多頭排列 (Perfect Order)
    # 股價 > 5日 > 10日 > 20日 > 60日 > 240日
    # 這是最強的趨勢型態，代表所有週期的持有者都賺錢，賣壓最小
    cond_perfect_order = (curr_c > curr_ma5) and \
                         (curr_ma5 > curr_ma10) and \
                         (curr_ma10 > curr_ma20) and \
                         (curr_ma20 > curr_ma60) and \
                         (curr_ma60 > curr_ma240)

    # 2. 攻擊態勢 (Momentum)
    # 確保短期均線 (5MA, 10MA) 是向上的，代表動能還在
    cond_momentum = (curr_ma5 > prev_ma5) and (curr_ma10 > prev_ma10)

    # 3. 線性乖離控制 (Linearity Check)
    # 我們要找「沿著均線爬」的股票，而不是「沖天炮」
    # 如果股價離 10日線太遠 (乖離率 > 8%)，代表可能過熱，暫時不追
    bias_ma10 = (curr_c - curr_ma10) / curr_ma10
    cond_linearity = 0 < bias_ma10 < 0.08  # 正乖離且在 8% 以內

    # --- 最終判定 ---
    is_match = cond_perfect_order and cond_momentum and cond_linearity
    
    if is_match:
        change_rate = 0.0
        if prev_c > 0:
            change_rate = round((curr_c - prev_c) / prev_c * 100, 2)
            
        # 計算 3日均量比 (量能指標)
        vol_ma3 = volume.rolling(3).mean().iloc[-1]
        vol_ratio = 0
        if vol_ma3 > 0:
            vol_ratio = round(curr_v / vol_ma3, 2)

        return True, {
            "price": round(curr_c, 2),
            "ma5": round(curr_ma5, 2),
            "ma10": round(curr_ma10, 2),
            "ma240": round(curr_ma240, 2),
            "changeRate": change_rate,
            "vol_ratio": vol_ratio
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
    print("啟動自動掃描程序 (V6 強勢線性趨勢)...")
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
