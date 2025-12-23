# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V55 Strategy A Perfect Alignment

【版本資訊】
Base Version: V54
Update: 策略 A (拉回佈局) 多頭排列條件升級為六線完美排列。

【保留策略說明】
1. 策略 A (拉回佈局): 
   1. 長線保護：收盤 > MA300, MA120, MA60。
   2. 多頭排列：MA10 > MA20 > MA30 > MA60 > MA120 > MA240。(修改：六線完美排列)
   3. 位階安全：乖離率 < 25%。
   4. 均線糾結：差異 < 8%。
   5. 量縮整理：成交量 < 5日均量。
   6. 支撐確認：收盤 > MA12。
   7. 底部打樁：|今日最低 - 昨日最低| < 1%。
   8. 流動性：5日均量 > 1000張。
2. 策略 B (Strict VCP):
  1. 硬指標過濾：股價 > MA300 & > MA60 & 成交量 > 1000張。
  2. 多頭排列：MA60 > MA120 > MA240。
  3. 價格位階：靠近 52 週新高。
  4. 波動收縮：布林帶寬度 < 15%。
  5. 量能遞減：5日均量 < 20日均量。
  6. 回檔收縮：r1(60日) > r2(20日) > r3(10日)。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
from datetime import datetime, time as dt_time, timedelta
import pytz
import time

# ==========================================
# 1. 資料庫管理
# ==========================================
DB_INDUSTRY = 'cmoney_industry_cache.json'
DB_HISTORY = 'history.json'
DATA_JSON = 'data.json'

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
# 2. 產業分類解析邏輯
# ==========================================
def get_stock_group(code, db_data):
    group = "其他"
    if code in db_data:
        raw_data = db_data[code]
        if isinstance(raw_data, dict):
            if 'sub' in raw_data and raw_data['sub']: group = raw_data['sub']
            elif 'main' in raw_data and raw_data['main']: group = raw_data['main']
            elif 'industry' in raw_data: group = raw_data['industry']
        elif isinstance(raw_data, str):
            group = raw_data
    elif code in twstock.codes:
        if code in twstock.codes and twstock.codes[code].group:
            group = twstock.codes[code].group.replace("工業", "").replace("業", "")
    
    if not isinstance(group, str): group = str(group)
    return group

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
# 3. 策略邏輯 (V55 更新)
# ==========================================

def check_strategy_original(df):
    """
    策略 A：拉回佈局 (V55: 六線完美多頭排列)
    """
    # 資料長度檢查
    if len(df) < 310: return False, None
    
    close = df['Close']
    volume = df['Volume']
    low = df['Low']
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma12 = close.rolling(12).mean()
    ma20 = close.rolling(20).mean()
    # 【新增】計算 MA30
    ma30 = close.rolling(30).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    # 【新增】計算 MA240 (配合多頭排列檢查)
    ma240 = close.rolling(240).mean()
    # MA300 (配合長線保護)
    ma300 = close.rolling(300).mean()
    
    vol_ma5 = volume.rolling(5).mean()
    
    curr_c = float(close.iloc[-1])
    curr_v = float(volume.iloc[-1])
    curr_l = float(low.iloc[-1])
    
    curr_ma5 = float(ma5.iloc[-1])
    curr_ma10 = float(ma10.iloc[-1])
    curr_ma12 = float(ma12.iloc[-1])
    curr_ma20 = float(ma20.iloc[-1])
    # 【新增】取得 curr_ma30
    curr_ma30 = float(ma30.iloc[-1])
    curr_ma60 = float(ma60.iloc[-1])
    curr_ma120 = float(ma120.iloc[-1]) 
    # 【新增】取得 curr_ma240
    curr_ma240 = float(ma240.iloc[-1])
    curr_ma300 = float(ma300.iloc[-1])
    
    curr_vol_ma5 = float(vol_ma5.iloc[-1])
    prev_l = float(low.iloc[-2])

    # === 強制檢查 MA300 (長線過濾, V53新增) ===
    if math.isnan(curr_ma300): return False, None 
    if curr_c < curr_ma300: return False, None    

    # 過濾：成交量門檻需 > 1000張
    if curr_vol_ma5 < 1000000: return False, None 

    # 1. 長線保護
    if curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
    
    # 2. 【修改】六線完美多頭排列
    # MA10 > MA20 > MA30 > MA60 > MA120 > MA240
    if math.isnan(curr_ma30) or math.isnan(curr_ma240): return False, None
    if not (curr_ma10 > curr_ma20 > curr_ma30 > curr_ma60 > curr_ma120 > curr_ma240): return False, None
    
    # 3. 位階控制
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    
    # 4. 均線糾結
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    
    # 5. 量縮整理
    if curr_v >= curr_vol_ma5: return False, None
    
    # 6. 支撐確認 (MA12)
    if curr_c <= curr_ma12: return False, None
    
    # 7. 底部打樁
    if prev_l > 0:
        low_diff_pct = abs(curr_l - prev_l) / prev_l
        if low_diff_pct > 0.01: return False, None

    return True, {
        "tag": "拉回佈局",
        "price": round(curr_c, 2),
        "ma5": round(close.rolling(5).mean().iloc[-1], 2),
        "ma10": round(curr_ma10, 2),
        "ma20": round(curr_ma20, 2),
        "ma300": round(curr_ma300, 2)
    }

def check_strategy_vcp_pro(df):
    """
    策略 B：VCP 技術面 (Strict VCP)
    """
    try:
        close = df['Close']
        volume = df['Volume']

        if len(close) < 310: return False, None

        # ===== 1. 計算指標 =====
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ma60 = close.rolling(60).mean()
        ma300 = close.rolling(300).mean()
        
        # 多頭排列檢查用
        ma120 = close.rolling(120).mean()
        ma240 = close.rolling(240).mean()
        
        # 布林帶
        std20 = close.rolling(20).std()
        bb_upper = ma20 + (std20 * 2)
        bb_lower = ma20 - (std20 * 2)
        bb_width = (bb_upper - bb_lower) / ma20

        curr_c = float(close.iloc[-1])
        curr_v = float(volume.iloc[-1])

        curr_ma20 = float(ma20.iloc[-1])
        curr_ma50 = float(ma50.iloc[-1])
        curr_ma150 = float(ma150.iloc[-1])
        curr_ma200 = float(ma200.iloc[-1])
        curr_ma60 = float(ma60.iloc[-1])
        curr_ma300 = float(ma300.iloc[-1])
        
        curr_ma120 = float(ma120.iloc[-1])
        curr_ma240 = float(ma240.iloc[-1])
        
        curr_bb_width = float(bb_width.iloc[-1])

        # ===== 硬指標過濾 =====
        # 1. 股價必須站上 MA300
        if math.isnan(curr_ma300) or curr_c < curr_ma300: return False, None
        
        # 2. 股價必須站上 MA60
        if math.isnan(curr_ma60) or curr_c <= curr_ma60: return False, None
        
        # 3. 多頭排列檢查: MA60 > MA120 > MA240
        if math.isnan(curr_ma120) or math.isnan(curr_ma240): return False, None
        if not (curr_ma60 > curr_ma120 > curr_ma240): return False, None

        # 4. 成交量 > 1000 張
        if curr_v < 1000000: return False, None

        # ===== 條件 1：趨勢確認 =====
        if curr_c < curr_ma200: return False, None
        if curr_ma200 <= float(ma200.iloc[-20]): return False, None
        if curr_c < curr_ma150: return False, None

        # ===== 條件 2：價格位階 =====
        high_52w = close.iloc[-250:].max()
        low_52w = close.iloc[-250:].min()
        if curr_c < low_52w * 1.3: return False, None
        if curr_c < high_52w * 0.75: return False, None

        # ===== 條件 3：波動收縮 =====
        if curr_bb_width > 0.15: return False, None
        if curr_c < curr_ma20 * 0.98: return False, None

        # ===== 條件 4：量能遞減 =====
        vol_ma5 = volume.rolling(5).mean()
        vol_ma20 = volume.rolling(20).mean()
        if float(vol_ma5.iloc[-1]) > float(vol_ma20.iloc[-1]): return False, None
        if float(vol_ma5.iloc[-1]) < 300000: return False, None

        # ===== 條件 5：回檔幅度遞減 =====
        def calc_retrace(series):
            peak = series.max()
            trough = series.min()
            return (peak - trough) / peak if peak > 0 else 1.0

        r1 = calc_retrace(close.iloc[-60:])
        r2 = calc_retrace(close.iloc[-20:])
        r3 = calc_retrace(close.iloc[-10:])
        
        if not (r1 > r2 > r3): return False, None

    except Exception:
        return False, None

    return True, {
        "tag": "Strict-VCP",
        "price": round(curr_c, 2),
        "ma5": round(close.rolling(5).mean().iloc[-1], 2),
        "ma10": round(ma10.iloc[-1], 2),
        "ma20": round(curr_ma20, 2),
        "ma150": round(curr_ma150, 2),
        "ma200": round(curr_ma200, 2),
        "ma300": round(curr_ma300, 2),
        "bb_width": round(curr_bb_width * 100, 1)
    }

# ==========================================
# 4. 更新歷史績效 (盤中即時更新)
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效 (ROI Update)...")
    tickers_to_check = set()
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            tickers_to_check.add(symbol)

    if not tickers_to_check: return history_db

    print(f"追蹤股票數量: {len(tickers_to_check)}")
    current_data = {}
    try:
        data = yf.download(list(tickers_to_check), period="5d", auto_adjust=True, threads=True, progress=False)
        close_df = data['Close']
        
        if len(tickers_to_check) == 1:
             ticker = list(tickers_to_check)[0]
             if isinstance(close_df, pd.DataFrame):
                 closes = close_df[ticker].dropna().values if ticker in close_df.columns else close_df.iloc[:, 0].dropna().values
             else:
                 closes = close_df.dropna().values
             if len(closes) >= 1:
                 current_price = float(closes[-1])
                 prev_price = float(closes[-2]) if len(closes) >= 2 else current_price
                 current_data[ticker] = { 'price': current_price, 'prev': prev_price }
        else:
            for ticker in tickers_to_check:
                try:
                    if ticker in close_df.columns:
                        series = close_df[ticker].dropna()
                    else:
                        continue
                    if len(series) >= 1:
                        current_price = float(series.iloc[-1])
                        prev_price = float(series.iloc[-2]) if len(series) >= 2 else current_price
                        current_data[ticker] = { 'price': current_price, 'prev': prev_price }
                except: pass
    except Exception as e:
        print(f"Error updating history: {e}")
        return history_db

    for date_str, stocks in history_db.items():
        try:
             pass
        except: continue
        
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = float(stock['buy_price'])
                
                roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                daily_change = round(((latest_price - prev_price) / prev_price) * 100, 2)
                
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                stock['daily_change'] = daily_change

    print("歷史績效更新完成 (In-Memory)。")
    return history_db

# ==========================================
# 5. 主程式
# ==========================================
def run_scanner():
    tw_tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tw_tz)
    
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    history_db = update_history_roi(history_db)
    save_json(DB_HISTORY, history_db)
    print("✅ history.json 已更新最新報價與 ROI。")

    full_list = get_all_tickers()
    print(f"開始掃描全市場... 時間: {now.strftime('%H:%M:%S')}")
    
    daily_results = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        try:
            # 2y (730天) 足夠計算 MA300
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
            
            for ticker in batch:
                try:
                    raw_code = ticker.split('.')[0]
                    df = pd.DataFrame()
                    if len(batch) > 1:
                        if ticker in data.columns.levels[0]:
                            df = data[ticker].copy()
                    else:
                        df = data.copy()
                    
                    df = df.dropna()
                    if df.empty: continue
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(0)

                    required_cols = ['Close', 'Volume', 'Low']
                    if not all(col in df.columns for col in required_cols): continue

                    is_match_1, info_1 = check_strategy_original(df)
                    is_match_2, info_2 = check_strategy_vcp_pro(df)
                    
                    final_match = False
                    final_info = {}
                    strategy_tags = []

                    if is_match_1:
                        final_match = True
                        final_info = info_1
                        strategy_tags.append("拉回佈局")
                    if is_match_2:
                        final_match = True
                        if not final_info: final_info = info_2
                        strategy_tags.append("Strict-VCP")
                    
                    if final_match:
                        name = raw_code
                        if raw_code in twstock.codes: name = twstock.codes[raw_code].name
                        group = get_stock_group(raw_code, industry_db)
                        if raw_code not in industry_db: industry_db[raw_code] = group
                        
                        try:
                            prev_c = df['Close'].iloc[-2]
                            change_rate = round((final_info['price'] - prev_c) / prev_c * 100, 2)
                        except:
                            change_rate = 0.0
                            
                        tags_str = " & ".join(strategy_tags)
                        
                        # 顯示 MA300 在備註
                        note_ma300 = round(final_info.get('ma300', 0), 2)
                        note_str = f"{tags_str} / MA300 {note_ma300}"

                        stock_entry = {
                            "id": raw_code,
                            "name": name,
                            "group": group,
                            "type": "上櫃" if ".TWO" in ticker else "上市",
                            "price": final_info['price'], 
                            "ma5": final_info['ma5'],
                            "ma10": final_info['ma10'],
                            "changeRate": change_rate,
                            "isValid": True,
                            "note": note_str,
                            "buy_price": final_info['price'], 
                            "latest_price": final_info['price'], 
                            "roi": 0.0, 
                            "daily_change": change_rate
                        }
                        daily_results.append(stock_entry)
                        print(f" -> Found: {raw_code} {name} [{tags_str}]")
                        
                except Exception: continue
        except Exception: continue
        time.sleep(1.0)

    save_json(DB_INDUSTRY, industry_db)
    
    print(f"掃描結束，共發現 {len(daily_results)} 檔。更新 data.json...")
    data_payload = {
        "date": now.strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": daily_results
    }
    save_json(DATA_JSON, data_payload)

    current_time = now.time()
    market_open = dt_time(9, 0, 0)
    market_close = dt_time(13, 30, 0)
    is_market_session = market_open <= current_time <= market_close

    if is_market_session:
        print(f"⚠️ 現在是盤中時間 ({current_time.strftime('%H:%M')})，跳過 History 新增歸檔。")
    else:
        if current_time > market_close:
            record_date_str = now.strftime("%Y/%m/%d")
        else:
            yesterday = now - timedelta(days=1)
            record_date_str = yesterday.strftime("%Y/%m/%d")

        print(f"✅ 盤後時段，準備將新資料歸檔至 History: {record_date_str}")
        
        if daily_results:
            existing_ids = set()
            for date_key, stocks in history_db.items():
                for s in stocks:
                    existing_ids.add(s['id'])
            
            unique_results = []
            for stock in daily_results:
                if stock['id'] in existing_ids:
                    print(f" ⟳ Skip duplicate in history: {stock['id']} {stock['name']}")
                else:
                    unique_results.append(stock)
            
            if unique_results:
                history_db[record_date_str] = unique_results
                sorted_history = dict(sorted(history_db.items(), reverse=True))
                save_json(DB_HISTORY, sorted_history)
                print(f"History.json 新增 {len(unique_results)} 筆資料 (已過濾重複)。")
            else:
                print("今日所有掃描結果均已存在於歷史紀錄中，不新增任何資料。")
        else:
            print("今日無符合策略標的，不新增 History。")

    return daily_results

if __name__ == "__main__":
    run_scanner()
   
