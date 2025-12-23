# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V52 Intraday ROI Update

【版本資訊】
Base Version: V51 Clean Production (User Uploaded)
Current Version: V52
Update: 盤中即時更新 ROI、盤後歷史名單去重、Python 3.10+ 支援

【保留策略說明】
1. 策略 A (拉回佈局): 
   - 長線保護 (MA240, MA120, MA60)
   - 多頭排列 (MA10 > MA20 > MA60)
   - 乖離率 < 25%, 均線糾結 < 8%, 量縮, 支撐, 底部打樁, 5日均量 > 500張
2. 策略 B (Strict VCP):
   - 硬指標 (股價 > MA240 & MA60, 量 > 500張)
   - 價格位階 (靠近 52 週新高)
   - 波動收縮 (布林頻寬 < 15%)
   - 量能遞減, 回檔收縮 (r1 > r2 > r3)
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
# 2. 產業分類解析邏輯 (保留原樣)
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
# 3. 策略邏輯 (完全保留 V51 邏輯)
# ==========================================

def check_strategy_original(df):
    """
    策略 A：拉回佈局
    """
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
    """
    策略 B：VCP 技術面 (Strict VCP)
    """
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
# 4. 關鍵功能: 更新歷史績效 (盤中即時更新)
# ==========================================
def update_history_roi(history_db):
    """
    每次執行時，優先抓取所有歷史名單的「最新股價」，
    即時計算 ROI 與 Daily Change，讓前端網頁看到最新戰況。
    """
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
        # 下載最近 5 天資料，包含盤中即時價
        data = yf.download(list(tickers_to_check), period="5d", auto_adjust=True, threads=True, progress=False)
        close_df = data['Close']
        
        # 處理單支與多支股票的資料結構差異
        if len(tickers_to_check) == 1:
             ticker = list(tickers_to_check)[0]
             if isinstance(close_df, pd.DataFrame):
                 closes = close_df[ticker].dropna().values if ticker in close_df.columns else close_df.iloc[:, 0].dropna().values
             else:
                 closes = close_df.dropna().values
             if len(closes) >= 1:
                 # 若只有一筆(當日)，prev 用同一筆或 0，避免當掉
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

    # 更新 DB 數值
    for date_str, stocks in history_db.items():
        try:
             # 如果是盤中執行，這裡計算 ROI 就會是 Based on 即時股價
             pass
        except: continue
        
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
                buy_price = float(stock['buy_price'])
                
                # 計算 ROI
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
    
    # -----------------------------------------------------
    # 步驟 1: 【盤中更新】優先更新舊名單的 ROI
    # 無論是 9:00, 10:00 還是 13:30，每次執行都會先跑這段
    # -----------------------------------------------------
    history_db = update_history_roi(history_db)
    
    # 立即存檔 history.json，確保前端網頁能看到盤中即時 ROI 變化
    save_json(DB_HISTORY, history_db)
    print("✅ history.json 已更新最新報價與 ROI。")

    # -----------------------------------------------------
    # 步驟 2: 開始掃描今日新標的 (產生 data.json 給熱力圖用)
    # -----------------------------------------------------
    full_list = get_all_tickers()
    print(f"開始掃描全市場... 時間: {now.strftime('%H:%M:%S')}")
    
    daily_results = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        # print(f"Processing batch {i//batch_size + 1}/{len(full_list)//batch_size + 1}...")
        try:
            # 取得即時報價 (auto_adjust=True)
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

                    # 執行策略
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
        except Exception: continue
        time.sleep(1.0)

    save_json(DB_INDUSTRY, industry_db)
    
    # 儲存 data.json (熱力圖與列表頁面用) - 盤中會持續更新此檔
    print(f"掃描結束，共發現 {len(daily_results)} 檔。更新 data.json...")
    data_payload = {
        "date": now.strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": daily_results
    }
    save_json(DATA_JSON, data_payload)

    # -----------------------------------------------------
    # 步驟 3: 歸檔判定 (決定是否寫入 History)
    # -----------------------------------------------------
    current_time = now.time()
    market_open = dt_time(9, 0, 0)
    market_close = dt_time(13, 30, 0)
    # 盤中定義：9:00 ~ 13:30 (可視需要微調)
    is_market_session = market_open <= current_time <= market_close

    if is_market_session:
        print(f"⚠️ 現在是盤中時間 ({current_time.strftime('%H:%M')})，跳過 History 新增歸檔。")
    else:
        # 盤後時間，準備歸檔
        if current_time > market_close:
            record_date_str = now.strftime("%Y/%m/%d")
        else:
            yesterday = now - timedelta(days=1)
            record_date_str = yesterday.strftime("%Y/%m/%d")

        print(f"✅ 盤後時段，準備將新資料歸檔至 History: {record_date_str}")
        
        if daily_results:
            # ==========================================
            # 【關鍵】去重機制 (Duplicate Check)
            # ==========================================
            
            # 1. 收集所有歷史紀錄中的股票 ID
            existing_ids = set()
            for date_key, stocks in history_db.items():
                for s in stocks:
                    existing_ids.add(s['id'])
            
            # 2. 過濾今日名單：如果 ID 已經存在於歷史中，則不加入
            unique_results = []
            for stock in daily_results:
                if stock['id'] in existing_ids:
                    print(f" ⟳ Skip duplicate in history: {stock['id']} {stock['name']}")
                else:
                    unique_results.append(stock)
            
            if unique_results:
                history_db[record_date_str] = unique_results
                # 依日期降序排列
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
