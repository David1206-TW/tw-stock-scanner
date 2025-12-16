# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V14 (完整交易系統版)

【V14 功能升級】
1. 停損機制 (Stop Loss): 累積報酬率 < -20% 自動剔除。
2. 鎖定原始買點 (Lock Entry): 若股票已在歷史名單中，今日再次選出也不會覆蓋，保留最早買點。
3. 完整數據 (Full Data): 新增「當日漲跌幅」與「建倉日期」欄位。

【篩選條件說明】
1. 長線保護短線: 收盤 > MA240 (Raw Price)。
2. 多頭排列: MA10 > MA20 > MA60。
3. 位階控制: (收盤 - MA60)/MA60 < 25%。
4. 均線糾結: MA5/10/20 差異 < 8%。
5. 量縮整理: 成交量 < 5日均量。
6. 支撐確認: 收盤 > MA10。
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
# 4. 策略邏輯 (V12)
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
# 5. 更新歷史績效 (含停損與日漲跌)
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效...")
    
    tickers_to_check = set()
    today = datetime.now()
    
    # 清理過的資料庫 (移除停損或過期的)
    new_history_db = {}

    # 1. 收集代碼
    for date_str, stocks in history_db.items():
        try:
            record_date = datetime.strptime(date_str.split(' ')[0], "%Y/%m/%d")
            # 追蹤 20 天內的資料 (可自行調整天數)
            if (today - record_date).days <= 20:
                new_history_db[date_str] = stocks # 先保留，稍後更新內容
                for stock in stocks:
                    symbol = stock['id']
                    if stock['type'] == '上市': symbol += '.TW'
                    else: symbol += '.TWO'
                    tickers_to_check.add(symbol)
        except: pass

    if not tickers_to_check:
        print("沒有歷史資料需要更新。")
        return {}

    # 2. 批次下載最新股價 (下載 2 天以計算日漲跌)
    print(f"追蹤股票數量: {len(tickers_to_check)}")
    current_data = {}
    try:
        # 下載 5 天避免假日剛過導致資料不足
        data = yf.download(list(tickers_to_check), period="5d", auto_adjust=False, threads=True)
        
        # 處理多層索引
        close_df = data['Close']
        
        if len(tickers_to_check) == 1:
             ticker = list(tickers_to_check)[0]
             # 單支股票處理
             closes = close_df.dropna().values
             if len(closes) >= 2:
                 current_data[ticker] = {
                     'price': float(closes[-1]),
                     'prev': float(closes[-2])
                 }
        else:
            for ticker in tickers_to_check:
                try:
                    # 取得該股票的收盤價序列，並移除 NaN
                    series = close_df[ticker].dropna()
                    if len(series) >= 2:
                        current_data[ticker] = {
                            'price': float(series.iloc[-1]),
                            'prev': float(series.iloc[-2])
                        }
                except: pass
    except Exception as e:
        print(f"歷史股價更新失敗: {e}")
        return history_db

    # 3. 更新 ROI 並執行停損
    final_db = {}
    
    for date_str, stocks in new_history_db.items():
        active_stocks = []
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = stock['buy_price']
                
                # 計算累積 ROI
                roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                
                # 計算當日漲跌幅
                daily_change = round(((latest_price - prev_price) / prev_price) * 100, 2)
                
                # 【規則 3】停損檢查：如果 ROI < -20%，剔除
                if roi < -20:
                    print(f"停損觸發: {stock['name']} ROI {roi}% < -20%")
                    continue # 跳過此股票，不加入 active_stocks
                
                # 更新欄位
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                stock['daily_change'] = daily_change # 【規則 6】新增欄位
                
                active_stocks.append(stock)
            else:
                # 抓不到資料的股票保留原樣
                active_stocks.append(stock)
        
        # 只有當該日期還有股票存活時才保留該日期
        if active_stocks:
            final_db[date_str] = active_stocks

    print("歷史績效更新完成 (已執行停損)。")
    return final_db

# ==========================================
# 6. 批次執行掃描
# ==========================================
def run_scanner():
    full_list = get_all_tickers()
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    print(f"歷史庫: {len(history_db)} 天")
    
    # 【規則 4】建立已存在名單的集合 (Set)，防止重複加入
    # 如果 12/16 已經選過光聖，12/17 就不再加入，保留 12/16 的低成本位階
    existing_stocks = set()
    for date_str, stocks in history_db.items():
        for s in stocks:
            existing_stocks.add(s['id'])
            
    print(f"已追蹤股票數: {len(existing_stocks)} (這些股票若今日符合也不會重複新增)")
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
                    # 預先過濾：如果已經在追蹤清單中，直接跳過計算，節省時間
                    raw_code = ticker.split('.')[0]
                    if raw_code in existing_stocks:
                        continue

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
                        valid_stocks.append(stock_entry)
                except: continue
        except: continue

    # 1. 更新歷史 ROI (先更新舊的，再加新的)
    history_db = update_history_roi(history_db)

    # 2. 儲存當日新名單
    if valid_stocks:
        today_str = datetime.now().strftime("%Y/%m/%d")
        history_db[today_str] = valid_stocks
        print(f"今日新增監控: {len(valid_stocks)} 檔")

    # 3. 存檔
    save_json(DB_INDUSTRY, industry_db)
    save_json(DB_HISTORY, history_db)
    
    return valid_stocks

if __name__ == "__main__":
    results = run_scanner()
    # data.json 只存今日新增的，首頁只看當天新訊
    output_payload = {
        "date": datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": results
    }
    save_json('data.json', output_payload)


