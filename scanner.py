# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V10 (第一次突破前高版)

【目標型態：12/4 光聖模式 (The First Breakout)】
尋找「均線多頭排列」但「乖離率尚未過大」的股票。
排除已經大漲一段的個股，鎖定在起漲點前夕、均線糾結壓縮的標的。

【篩選條件說明 (Strategy)】
1. 長線保護短線 (Long-term Trend):
   - 收盤價 > 240日均線 (MA240)。(年線之上)

2. 多頭排列 (Trend):
   - MA10 > MA20 > MA60。(趨勢向上)

3. 位階控制 (Position Control) 【關鍵新增】:
   - 乖離率限制：(收盤價 - MA60) / MA60 < 25%。
   - 目的：排除已經噴離季線太遠、漲幅過大的股票 (避開 12/16 光聖)。
   - 確保買在「起漲區」而非「追高區」。

4. 均線糾結/壓縮 (Consolidation) 【關鍵新增】:
   - (MA5, MA10, MA20) 最高值與最低值差異 < 8%。
   - 目的：找出均線糾結、籌碼沉澱、準備發動的股票。

5. 量縮整理 (Dry Volume):
   - 今日成交量 < 5日均量 (量縮)。

6. 支撐確認 (Support):
   - 收盤價 > MA10 (守住短期攻擊線)。
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
# 4. 策略邏輯核心 (V10 - 第一次突破前高版)
# ==========================================
def check_strategy(df):
    if len(df) < 240: return False, {}

    close = df['Close']
    volume = df['Volume']
    
    # 計算均線
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma240 = close.rolling(240).mean()
    
    # 計算均量
    vol_ma5 = volume.rolling(5).mean()
    
    # 取得最新數據
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    
    prev_c = close.iloc[-2]

    # 【Filter 1】流動性過濾
    if curr_vol_ma5 < 500000: return False, {}

    # --- V10 策略條件 ---

    # 1. 長線保護短線 (Life Line)
    # 股價必須在年線之上
    cond_life_line = curr_c > curr_ma240

    # 2. 多頭排列 (Trend)
    # 至少要 MA10 > MA20 > MA60 (趨勢向上)
    cond_trend = (curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)

    # 3. 位階控制 (Position Control) - 避免選到已經噴出的
    # 股價距離季線 (MA60) 的乖離率不能超過 25%
    # 12/16 的光聖乖離率可能已經超過 40%，會被這條擋掉
    # 12/4 的光聖乖離率還在安全範圍內
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    cond_not_too_high = bias_ma60 < 0.25

    # 4. 均線糾結/壓縮 (Consolidation) - 找出醞釀中的
    # 檢查 MA5, MA10, MA20 是否糾結在一起
    # 定義：這三條均線的最大值與最小值差距 < 8%
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    cond_consolidation = ma_divergence < 0.08

    # 5. 量縮整理 (Dry Volume)
    # 確保不是爆量出貨盤，而是量縮洗盤
    cond_vol_dry = curr_v < curr_vol_ma5

    # 6. 短線支撐 (Support)
    # 收盤價守住 MA10 (允許破 MA5 洗盤，但不能破 MA10)
    cond_support = curr_c > curr_ma10

    # --- 最終判定 ---
    is_match = cond_life_line and cond_trend and cond_not_too_high and cond_consolidation and cond_vol_dry and cond_support
    
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
                            "note": f"均線糾結 / 準備突破"
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
    print("啟動自動掃描程序 (V10 第一次突破前高版)...")
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


