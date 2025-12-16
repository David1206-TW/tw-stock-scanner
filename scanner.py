# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V16 (邏輯修正版)

【V16 修正說明】
- 修正「再次執行時 data.json 為空」的問題。
- 邏輯調整：
  1. 掃描所有股票，只要符合策略，就放入 data.json (供熱力圖/列表顯示)。
  2. 檢查 history.json，只有當該股票「不在」歷史名單中，才將其視為「新進場」並存入 history.json。
  3. 若股票已在歷史名單中，則透過 update_history_roi 更新其最新價，保留原始買點。

【篩選條件 (V12)】
1. 收盤 > MA240 (Raw Price)。
2. MA10 > MA20 > MA60。
3. 乖離率 < 25%。
4. 均線糾結 < 8%。
5. 成交量 < 5日均量。
6. 收盤 > MA10。
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
DB_INDUSTRY = 'industry.json'
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
# 4. 策略邏輯
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
# 5. 更新歷史績效 (全面追蹤)
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

    if not tickers_to_check:
        return history_db

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
    except Exception as e:
        print(f"歷史股價更新失敗: {e}")
        return history_db

    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = stock['buy_price']
                
                roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                daily_change = round(((latest_price - prev_price) / prev_price) * 100, 2)
                
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                stock['daily_change'] = daily_change

    print("歷史績效更新完成。")
    return history_db

# ==========================================
# 6. 主程式
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    # 建立已存在名單 Set (用於判斷是否為新面孔)
    existing_stock_ids = set()
    for date_str, stocks in history_db.items():
        for s in stocks:
            existing_stock_ids.add(s['id'])
            
    print(f"歷史已追蹤: {len(existing_stock_ids)} 檔")
    print(f"開始掃描 (V12 原始股價)...")
    
    daily_results = [] # 存放今日所有符合條件的股票 (給 data.json)
    new_history_entries = [] # 存放今日「新發現」的股票 (給 history.json)
    
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}...")
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=False)
            for ticker in batch:
                try:
                    raw_code = ticker.split('.')[0]
                    # 【修正點】移除這裡的 continue，所有股票都要檢查策略
                    # if raw_code in existing_stocks: continue 
                    
                    if len(batch) > 1: df = data[ticker] if ticker in data.columns.levels[0] else pd.DataFrame()
                    else: df = data
                    
                    df = df.dropna()
                    if df.empty: continue
                    match, info = check_strategy(df)
                    
                    if match:
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
                            # 歷史追蹤欄位
                            "buy_price": info['price'],
                            "latest_price": info['price'],
                            "roi": 0.0,
                            "daily_change": info['changeRate']
                        }
                        
                        # 1. 加入今日顯示列表 (data.json 用)
                        daily_results.append(stock_entry)
                        
                        # 2. 如果是新面孔，加入歷史待存區 (history.json 用)
                        if raw_code not in existing_stock_ids:
                            new_history_entries.append(stock_entry)
                            
                except: continue
        except: continue

    # 更新歷史 ROI
    history_db = update_history_roi(history_db)

    # 將新發現的名單寫入 history
    if new_history_entries:
        today_str = datetime.now().strftime("%Y/%m/%d")
        # 檢查今天日期是否已經有key，避免覆蓋
        if today_str in history_db:
             # 如果今天已經跑過，合併新發現的 (雖然理論上一次跑完不會有這情況，但防呆)
             # 這裡我們簡單覆蓋或是追加，假設一天只跑一次完整掃描
             history_db[today_str].extend(new_history_entries)
        else:
             history_db[today_str] = new_history_entries
        print(f"今日新納入歷史庫: {len(new_history_entries)} 檔")

    # 存檔
    save_json(DB_INDUSTRY, industry_db)
    save_json(DB_HISTORY, history_db)
    
    return daily_results # 回傳今日所有符合的，供 data.json 使用

if __name__ == "__main__":
    results = run_scanner()
    # data.json 存入今日所有符合策略的股票 (包含舊面孔)
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": results
    }
    save_json('data.json', output_payload)


