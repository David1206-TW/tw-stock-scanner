# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V53 De-Duplicate Logic

【V53 修正重點：重複名單清洗】
1. 新增 remove_duplicates_keep_earliest():
   - 確保 History 中同一支股票只會出現一次。
   - 保留「最早」的紀錄 (Entry Date)，讓 ROI 以最初進場價計算。
2. 掃描結果過濾:
   - 今日掃描到的新股，若已存在於 History (不管哪一天)，直接忽略，不重複建檔。

【V52 包含功能】
1. 強化 update_history_roi: 增加詳細 Log，處理 yfinance 單檔/多檔資料結構差異。
2. 錯誤處理: 當 yfinance 下載失敗時，會明確印出錯誤原因。
3. 資料回補: 針對 NaN 資料增加 ffill() 回補。

【策略 A：拉回佈局】
   1. 長線保護：收盤 > MA240, MA120, MA60。
   2. 多頭排列：MA10 > MA20 > MA60。
   3. 位階安全：乖離率 < 25%。
   4. 均線糾結：差異 < 8%。
   5. 量縮整理：成交量 < 5日均量。
   6. 支撐確認：收盤 > MA10。
   7. 底部打樁：|今日最低 - 昨日最低| < 1%。
   8. 流動性：5日均量 > 500張。

【策略 B：VCP 技術面 (Strict VCP)】
  1. 硬指標過濾：股價 > MA240 & > MA60 & 成交量 > 500張。
  2. 價格位階：靠近 52 週新高。
  3. 波動收縮：布林帶寬度 < 15%。
  4. 量能遞減：5日均量 < 20日均量。
  5. 回檔收縮：r1(60日) > r2(20日) > r3(10日)。
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
# 3. [新增] 歷史名單清洗邏輯
# ==========================================
def remove_duplicates_keep_earliest(history_db):
    """
    清洗 History DB:
    1. 將所有日期的股票攤平。
    2. 按日期由舊到新排序。
    3. 保留每支股票「第一次」出現的紀錄，刪除後續日期的重複項。
    4. 重組回 history_db 格式。
    """
    if not history_db:
        return {}, set()
        
    print("🧹 正在執行歷史名單去重 (保留最早進場點)...")
    
    # 1. 將資料攤平為 (DateObject, DateStr, StockData) 的列表
    flat_list = []
    for date_str, stocks in history_db.items():
        try:
            dt = datetime.strptime(date_str, "%Y/%m/%d")
            for stock in stocks:
                flat_list.append({
                    'dt': dt,
                    'date_str': date_str,
                    'stock': stock
                })
        except: continue

    # 2. 按日期排序 (舊 -> 新)
    flat_list.sort(key=lambda x: x['dt'])

    # 3. 過濾重複 ID
    seen_ids = set()
    cleaned_map = {} # key: date_str, value: list of stocks

    duplicates_removed = 0
    
    for item in flat_list:
        stock_id = item['stock']['id']
        date_str = item['date_str']
        
        if stock_id not in seen_ids:
            seen_ids.add(stock_id)
            # 加入新名單
            if date_str not in cleaned_map:
                cleaned_map[date_str] = []
            cleaned_map[date_str].append(item['stock'])
        else:
            duplicates_removed += 1
    
    print(f"🧹 清洗完成: 移除了 {duplicates_removed} 筆重複資料。")
    return cleaned_map, seen_ids

# ==========================================
# 4. 策略邏輯 (保留原始詳細邏輯)
# ==========================================

def check_strategy_original(df):
    if len(df) < 250: return False, None
    close = df['Close']
    volume = df['Volume']
    low = df['Low']
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    ma240 = close.rolling(240).mean()
    vol_ma5 = volume.rolling(5).mean()
    
    curr_c = float(close.iloc[-1])
    curr_v = float(volume.iloc[-1])
    curr_l = float(low.iloc[-1])
    
    curr_ma5 = float(ma5.iloc[-1])
    curr_ma10 = float(ma10.iloc[-1])
    curr_ma20 = float(ma20.iloc[-1])
    curr_ma60 = float(ma60.iloc[-1])
    curr_ma120 = float(ma120.iloc[-1]) 
    curr_ma240 = float(ma240.iloc[-1])
    curr_vol_ma5 = float(vol_ma5.iloc[-1])
    
    prev_l = float(low.iloc[-2])

    # === 強制檢查 MA240 (嚴格過濾) ===
    if math.isnan(curr_ma240): return False, None # 資料不足，剔除
    if curr_c < curr_ma240: return False, None    # 跌破年線，剔除

    # 過濾：成交量門檻
    if curr_vol_ma5 < 500000: return False, None 

    # 1. 長線保護 (包含 MA60 與 MA120 檢查)
    if curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
    
    # 2. 多頭排列
    if not ((curr_ma10 > curr_ma20) and (curr_ma20 > curr_ma60)): return False, None
    
    # 3. 位階控制
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    
    # 4. 均線糾結
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    
    # 5. 量縮整理
    if curr_v >= curr_vol_ma5: return False, None
    # 6. 支撐確認
    if curr_c <= curr_ma10: return False, None
    
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
        "ma240": round(curr_ma240, 2)
    }

def check_strategy_vcp_pro(df):
    try:
        close = df['Close']
        volume = df['Volume']

        if len(close) < 260: return False, None

        # ===== 1. 計算指標 =====
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ma60 = close.rolling(60).mean()   # 確保計算 MA60
        ma240 = close.rolling(240).mean() # 確保計算 MA240
        
        # 布林帶 (20日, 2倍標準差)
        std20 = close.rolling(20).std()
        bb_upper = ma20 + (std20 * 2)
        bb_lower = ma20 - (std20 * 2)
        # 布林帶寬度 (Bandwidth)
        bb_width = (bb_upper - bb_lower) / ma20

        # 當前數值
        curr_c = float(close.iloc[-1])
        curr_v = float(volume.iloc[-1]) # 當天成交量

        curr_ma20 = float(ma20.iloc[-1])
        curr_ma50 = float(ma50.iloc[-1])
        curr_ma150 = float(ma150.iloc[-1])
        curr_ma200 = float(ma200.iloc[-1])
        curr_ma60 = float(ma60.iloc[-1])
        curr_ma240 = float(ma240.iloc[-1])
        curr_bb_width = float(bb_width.iloc[-1])

        # ===== 硬指標過濾 =====
        # 1. 股價必須站上 MA240 (年線)
        if math.isnan(curr_ma240) or curr_c < curr_ma240: return False, None
        
        # 2. [新增] 股價必須站上 MA60 (季線)
        if math.isnan(curr_ma60) or curr_c <= curr_ma60: return False, None
        
        # 3. 成交量 > 500 張
        if curr_v < 500000: return False, None

        # ===== 條件 1：趨勢確認 =====
        if curr_c < curr_ma200: return False, None
        if curr_ma200 <= float(ma200.iloc[-20]): return False, None
        if curr_c < curr_ma150: return False, None

        # ===== 條件 2：價格位階 (靠近 52 週新高) =====
        high_52w = close.iloc[-250:].max()
        low_52w = close.iloc[-250:].min()
        if curr_c < low_52w * 1.3: return False, None
        if curr_c < high_52w * 0.75: return False, None

        # ===== 條件 3：波動收縮 (核心 VCP) =====
        if curr_bb_width > 0.15: return False, None
        if curr_c < curr_ma20 * 0.98: return False, None

        # ===== 條件 4：量能遞減 =====
        vol_ma5 = volume.rolling(5).mean()
        vol_ma20 = volume.rolling(20).mean()
        if float(vol_ma5.iloc[-1]) > float(vol_ma20.iloc[-1]): return False, None
        if float(vol_ma5.iloc[-1]) < 300000: return False, None

        # ===== 條件 5 (新增)：回檔幅度遞減 (r1 > r2 > r3) =====
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
        "ma240": round(curr_ma240, 2),
        "bb_width": round(curr_bb_width * 100, 1)
    }

# ==========================================
# 5. 更新歷史績效 (V52 Robust + V53 Clean)
# ==========================================
def update_history_roi(history_db):
    print("===== 開始更新歷史績效 (V53 Clean & Robust) =====")
    tw_tz = pytz.timezone('Asia/Taipei')
    today_str = datetime.now(tw_tz).strftime("%Y/%m/%d")
    today_date = datetime.strptime(today_str, "%Y/%m/%d")

    # 1. 在更新股價前，先執行「去重」
    # 這樣可以避免對重複的股票多次下載股價
    history_db, tracked_ids = remove_duplicates_keep_earliest(history_db)

    if not tracked_ids: 
        print("沒有需要追蹤的股票。")
        return history_db, tracked_ids

    # 重新收集完整的 symbols (包含 .TW/.TWO)
    symbols_map = {} # id -> full_symbol
    query_list = set()
    
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            symbols_map[stock['id']] = symbol
            query_list.add(symbol)
            
    print(f"追蹤股票數量 (不重複): {len(query_list)}")
    current_data = {}
    
    try:
        # 下載最近 5 天資料
        data = yf.download(list(query_list), period="5d", auto_adjust=True, threads=True)
        
        if data.empty:
            print("⚠️ yfinance 下載回傳為空，跳過更新。")
            return history_db, tracked_ids

        close_df = data['Close'] if 'Close' in data else pd.DataFrame()
        
        # 處理單檔與多檔
        for symbol in query_list:
            try:
                series = None
                if len(query_list) == 1:
                    if isinstance(close_df, pd.DataFrame):
                        if symbol in close_df.columns: series = close_df[symbol]
                        else: series = close_df.iloc[:, 0]
                    else: series = close_df
                else:
                    if symbol in close_df.columns: series = close_df[symbol]
                
                if series is not None and not series.empty:
                    series = series.ffill().dropna()
                    if len(series) >= 2:
                        last_price = float(series.iloc[-1])
                        prev_price = float(series.iloc[-2])
                        # 存入 map，key 用 stock_id (去除 .TW/.TWO) 方便後續對應
                        stock_id = symbol.split('.')[0]
                        current_data[stock_id] = { 'price': last_price, 'prev': prev_price }
            except: pass

    except Exception as e:
        print(f"❌ 歷史資料下載錯誤: {e}")
        return history_db, tracked_ids

    # 更新 Database
    updated_count = 0
    for date_str, stocks in history_db.items():
        try:
            entry_date = datetime.strptime(date_str, "%Y/%m/%d")
        except: continue
        
        days_diff = (today_date - entry_date).days

        for stock in stocks:
            s_id = stock['id']
            if s_id in current_data:
                latest_price = current_data[s_id]['price']
                prev_price = current_data[s_id]['prev']
                buy_price = stock['buy_price']
                
                if days_diff <= 0:
                    roi = 0.0
                    daily_change = 0.0
                else:
                    roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
                    daily_change = round(((latest_price - prev_price) / prev_price) * 100, 2)
                
                stock['latest_price'] = round(latest_price, 2)
                stock['roi'] = roi
                stock['daily_change'] = daily_change
                updated_count += 1

    print(f"歷史績效更新完成，共更新 {updated_count} 筆股價。")
    return history_db, tracked_ids

# ==========================================
# 6. 主程式
# ==========================================
def run_scanner():
    tw_tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tw_tz)
    
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    # 步驟 1: 更新歷史績效 + 清洗重複 (回傳 cleaned db 和所有已存在的 ID set)
    history_db, existing_ids = update_history_roi(history_db)
    
    save_json(DB_HISTORY, history_db)
    print("盤中歷史績效 (已去重) 更新至 DB。")

    # 開始掃描今日新標的
    full_list = get_all_tickers()
    print(f"開始掃描... 時間: {now.strftime('%H:%M:%S')}")
    
    daily_results = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{len(full_list)//batch_size + 1}...")
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=True)
            
            for ticker in batch:
                try:
                    raw_code = ticker.split('.')[0]
                    
                    # 【V53 關鍵】如果這檔股票已經在歷史名單中，直接跳過計算，節省時間並避免重複
                    if raw_code in existing_ids:
                        continue

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
                        note_ma240 = round(final_info.get('ma240', 0), 2)
                        note_str = f"{tags_str} / 年線{note_ma240}"

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
        except Exception as e:
            print(f"Batch error: {e}")
            continue
        
        time.sleep(1.5)

    save_json(DB_INDUSTRY, industry_db)
    
    # 處理 output
    print(f"掃描結束，共發現 {len(daily_results)} 檔。更新 data.json...")
    data_payload = {
        "date": now.strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": daily_results
    }
    save_json(DATA_JSON, data_payload)

    # 歸檔判定
    current_time = now.time()
    market_open = dt_time(9, 0, 0)
    market_close = dt_time(13, 30, 0)
    is_market_session = market_open <= current_time <= market_close

    if is_market_session:
        print(f"⚠️ 現在是盤中時間 ({current_time.strftime('%H:%M')})，跳過 History 新增歸檔 (但已更新舊股價)。")
    else:
        if current_time > market_close:
            record_date_str = now.strftime("%Y/%m/%d")
        else:
            yesterday = now - timedelta(days=1)
            record_date_str = yesterday.strftime("%Y/%m/%d")

        print(f"✅ 盤後時段，準備將新資料歸檔至 History: {record_date_str}")
        
        if daily_results:
            # 再次檢查：如果這些結果已經在 DB 裡了 (以防萬一)，就不重複加
            if record_date_str not in history_db:
                history_db[record_date_str] = daily_results
            else:
                # 合併邏輯: 確保不重複
                existing_today_ids = {s['id'] for s in history_db[record_date_str]}
                for res in daily_results:
                    if res['id'] not in existing_today_ids and res['id'] not in existing_ids:
                         history_db[record_date_str].append(res)

            sorted_history = dict(sorted(history_db.items(), reverse=True))
            save_json(DB_HISTORY, sorted_history)
            print("History.json 新增完畢。")
        else:
            print("今日無符合策略標的，不新增 History。")

    return daily_results

if __name__ == "__main__":
    run_scanner()
