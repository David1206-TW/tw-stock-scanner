# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V13 (含績效回測追蹤 + 策略說明)

【V13 新增功能】
- 績效追蹤 (Performance Tracking)：
  自動記錄每天選出的名單，並在後續的 14 個交易日內，
  每天更新這些名單的最新價格與累積報酬率 (ROI)。

【篩選條件說明 (Strategy)】
1. 長線保護短線 (Life Line):
   - 收盤價 > 240日均線 (MA240)。(絕對條件)

2. 多頭排列 (Trend):
   - MA10 > MA20 > MA60。

3. 位階控制 (Position Control):
   - (收盤價 - MA60) / MA60 < 25%。(避免追高)

4. 均線糾結/壓縮 (Consolidation):
   - (MA5, MA10, MA20) 差異 < 8%。

5. 量縮整理 (Dry Volume):
   - 今日成交量 < 5日均量。

6. 支撐確認 (Support):
   - 收盤價 > MA10。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import time
import math
from datetime import datetime, timedelta

# ==========================================
# 1. [核心] 資料庫管理
# ==========================================
DB_INDUSTRY = 'industry.json'
DB_HISTORY = 'history.json' # 用來存過去的名單與績效

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
# 4. 策略邏輯核心 (V12)
# ==========================================
def check_strategy(df):
    if len(df) < 250: return False, {}
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
    prev_c = close.iloc[-2]

    if math.isnan(curr_ma240) or curr_ma240 <= 0: return False, {}
    if curr_vol_ma5 < 500000: return False, {}

    # 策略條件
    cond_life_line = curr_c > curr_ma240
    cond_trend = (curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    cond_not_too_high = bias_ma60 < 0.25
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    cond_consolidation = ma_divergence < 0.08
    cond_vol_dry = curr_v < curr_vol_ma5
    cond_support = curr_c > curr_ma10

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
# 5. [新功能] 更新歷史績效
# ==========================================
def update_history_roi(history_db):
    """
    遍歷歷史資料庫，更新過去 14 天內名單的最新股價與 ROI
    """
    print("正在更新歷史名單績效...")
    
    # 1. 收集需要查詢的股票代碼 (過去 14 天)
    tickers_to_check = set()
    today = datetime.now()
    valid_dates = []

    for date_str in list(history_db.keys()):
        try:
            # 簡單判斷日期，若超過 20 天的資料就視為過期 (保留一點緩衝)
            record_date = datetime.strptime(date_str.split(' ')[0], "%Y/%m/%d")
            if (today - record_date).days <= 20: 
                valid_dates.append(date_str)
                for stock in history_db[date_str]:
                    # 判斷代碼格式 (加上 .TW 或 .TWO)
                    symbol = stock['id']
                    if stock['type'] == '上市': symbol += '.TW'
                    else: symbol += '.TWO'
                    tickers_to_check.add(symbol)
            else:
                # 刪除太舊的資料，保持檔案輕量
                del history_db[date_str]
        except:
            continue

    if not tickers_to_check:
        print("沒有歷史資料需要更新。")
        return history_db

    # 2. 批次下載最新股價
    print(f"追蹤股票數量: {len(tickers_to_check)}")
    try:
        # 使用 yfinance 一次抓取所有需要的股票最新價
        data = yf.download(list(tickers_to_check), period="1d", auto_adjust=False, threads=True)
        # 處理單一股票與多股票的資料結構差異
        current_prices = {}
        
        # 轉置一下方便處理，或直接取最後一列
        if len(tickers_to_check) == 1:
             # 單支股票
             ticker = list(tickers_to_check)[0]
             price = data['Close'].iloc[-1]
             current_prices[ticker] = float(price)
        else:
            # 多支股票
            for ticker in tickers_to_check:
                try:
                    price = data['Close'][ticker].iloc[-1]
                    if not math.isnan(price):
                        current_prices[ticker] = float(price)
                except:
                    pass
    except Exception as e:
        print(f"歷史股價更新失敗: {e}")
        return history_db

    # 3. 更新 ROI
    for date_str in valid_dates:
        for stock in history_db[date_str]:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            
            if symbol in current_prices:
                latest_price = current_prices[symbol]
                buy_price = stock['buy_price'] # 這是當初選出時的價格
                
                # 計算累積報酬率
                roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                
                # 更新欄位
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                
    print("歷史績效更新完成。")
    return history_db

# ==========================================
# 6. 批次執行掃描
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY) # 載入歷史資料庫
    
    print(f"產業庫: {len(industry_db)} 筆 | 歷史庫: {len(history_db)} 天")
    print(f"開始掃描 (V12 原始股價)...")
    
    valid_stocks = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}...")
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=False)
            for ticker in batch:
                try:
                    if len(batch) > 1: df = data[ticker] if ticker in data.columns.levels[0] else pd.DataFrame()
                    else: df = data
                    
                    df = df.dropna()
                    if df.empty: continue
                    match, info = check_strategy(df)
                    
                    if match:
                        raw_code = ticker.split('.')[0]
                        name = raw_code
                        if raw_code in twstock.codes: name = twstock.codes[raw_code].name
                        group = get_stock_group(raw_code, industry_db)
                        if raw_code not in industry_db: industry_db[raw_code] = group
                        
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
                            "note": f"年線{info['ma240']}",
                            # 【新增】給歷史資料用的欄位
                            "buy_price": info['price'], # 記錄當天選出的價格
                            "latest_price": info['price'], # 初始最新價等於現價
                            "roi": 0.0 # 初始 ROI 為 0
                        }
                        valid_stocks.append(stock_entry)
                except: continue
        except: continue

    # 1. 儲存當日新名單到 History
    today_str = datetime.now().strftime("%Y/%m/%d")
    history_db[today_str] = valid_stocks
    
    # 2. 更新歷史 ROI (這是最重要的一步)
    history_db = update_history_roi(history_db)

    # 3. 存檔
    save_json(DB_INDUSTRY, industry_db)
    save_json(DB_HISTORY, history_db) # 儲存 history.json
    print("所有資料庫已更新。")

    return valid_stocks

if __name__ == "__main__":
    results = run_scanner()
    # data.json 保持只存「當日最新」，讓首頁載入快一點
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": results
    }
    save_json('data.json', output_payload)


