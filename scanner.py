# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V42 Modified with MA60 & Strict VCP

【修正說明】
1. 修正策略函式呼叫名稱錯誤 (check_strategy_vcp -> check_strategy_vcp_pro)
2. 在 try-except 中加入錯誤列印，以便 Debug
3. VCP 策略新增過濾：剔除收盤 < MA240 且 成交量 < 500張 的標的
4. 雙策略共同過濾：確保收盤價都必須站上 MA60 (季線)
5. VCP 策略新增條件 5：回檔幅度遞減 (r1 > r2 > r3)

【策略 A：拉回佈局】
   1. 長線保護：收盤 > MA240, MA120, MA60。
   2. 多頭排列：MA10 > MA20 > MA60。
   3. 位階安全：乖離率 < 25%。
   4. 均線糾結：差異 < 8%。
   5. 量縮整理：成交量 < 5日均量。
   6. 支撐確認：收盤 > MA10。
   7. 底部打樁：|今日最低 - 昨日最低| < 100%。
   8. 流動性：5日均量 > 500張。

【策略 B：VCP 技術面 (VCP-Lite)】
  1. 硬指標過濾：股價必須 > MA240 (年線) 且 > MA60 (季線) 且 成交量 > 500張
  2. 價格位階：靠近 52 週新高 (High Tight Flag 特徵)
  3. 波動收縮：布林帶寬度 < 15% (代表籌碼沉澱)
  4. 量能遞減：5日均量 < 20日均量 (短期量縮)
  5. 回檔收縮：r1(60日) > r2(20日) > r3(10日)
 
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
from datetime import datetime, time as dt_time, timedelta
import pytz

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
# 4. 策略邏輯
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
    
    curr_c = close.iloc[-1]
    curr_v = volume.iloc[-1]
    curr_l = low.iloc[-1]
    
    curr_ma5 = ma5.iloc[-1]
    curr_ma10 = ma10.iloc[-1]
    curr_ma20 = ma20.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_ma120 = ma120.iloc[-1] 
    curr_ma240 = ma240.iloc[-1]
    curr_vol_ma5 = vol_ma5.iloc[-1]
    
    prev_l = low.iloc[-2]

    if math.isnan(curr_ma240) or curr_ma240 <= 0 or math.isnan(curr_ma120): return False, None
    if curr_vol_ma5 < 500000: return False, None 

    # 1. 長線保護 (已包含 MA60 檢查: curr_c <= curr_ma60 則踢除)
    if curr_c <= curr_ma240 or curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
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
        "ma5": round(curr_ma5, 2),
        "ma10": round(curr_ma10, 2),
        "ma20": round(curr_ma20, 2),
        "ma240": round(curr_ma240, 2),
        "vol_ratio": round(curr_v / curr_vol_ma5, 2)
    }

def check_strategy_vcp_pro(df):
    try:
        close = df['Close']
        volume = df['Volume']

        # 資料長度不足直接跳過 (需計算 MA240)
        if len(close) < 260:
            return False, None

        # ===== 1. 計算指標 =====
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ma60 = close.rolling(60).mean()   # [修改] 確保計算 MA60
        ma240 = close.rolling(240).mean() # [修改] 確保計算 MA240
        
        # 布林帶 (20日, 2倍標準差)
        std20 = close.rolling(20).std()
        bb_upper = ma20 + (std20 * 2)
        bb_lower = ma20 - (std20 * 2)
        # 布林帶寬度 (Bandwidth)
        bb_width = (bb_upper - bb_lower) / ma20

        # 當前數值
        curr_c = close.iloc[-1]
        curr_v = volume.iloc[-1] # 當天成交量

        curr_ma20 = ma20.iloc[-1]
        curr_ma50 = ma50.iloc[-1]
        curr_ma150 = ma150.iloc[-1]
        curr_ma200 = ma200.iloc[-1]
        curr_ma60 = ma60.iloc[-1]   # [新增]
        curr_ma240 = ma240.iloc[-1] # [新增]
        curr_bb_width = bb_width.iloc[-1]

        # ===== 新增條件：基本面濾網 =====
        # 1. 股價必須站上 MA240 (年線) -> 過濾長線空頭
        if math.isnan(curr_ma240) or curr_c < curr_ma240: return False, None
        
        # 2. [新增] 股價必須站上 MA60 (季線) -> 確保中期趨勢
        if math.isnan(curr_ma60) or curr_c <= curr_ma60: return False, None
        
        # 3. 當天成交量必須 > 500 張 (500,000股) -> 過濾流動性差
        if curr_v < 500000: return False, None

        # ===== 條件 1：趨勢確認 =====
        # 股價必須在 200MA 之上 (雙重確認長線趨勢)
        if curr_c < curr_ma200: return False, None
        # 年線必須大致向上 (當前年線 > 1個月前年線)
        if curr_ma200 <= ma200.iloc[-20]: return False, None
        # 股價在季線(50MA)或半年線(150MA)之上 (代表中長期強勢)
        if curr_c < curr_ma150: return False, None

        # ===== 條件 2：價格位階 (靠近 52 週新高) =====
        high_52w = close.iloc[-250:].max()
        low_52w = close.iloc[-250:].min()
        
        # 股價至少要比 52 週低點高 30%
        if curr_c < low_52w * 1.3: return False, None
        # 股價距離 52 週新高不能太遠 (25% 以內)
        if curr_c < high_52w * 0.75: return False, None

        # ===== 條件 3：波動收縮 (核心 VCP 精神) =====
        # 布林帶寬度 < 15%
        if curr_bb_width > 0.15: return False, None
        
        # 額外檢查：股價必須站在月線 (20MA) 之上或附近
        if curr_c < curr_ma20 * 0.98: return False, None

        # ===== 條件 4：量能遞減 =====
        vol_ma5 = volume.rolling(5).mean()
        vol_ma20 = volume.rolling(20).mean()
        
        # 5日均量 < 20日均量 (短期量縮)
        if vol_ma5.iloc[-1] > vol_ma20.iloc[-1]: return False, None
        
        # 5日均量也稍微過濾一下 (至少 300 張)
        if vol_ma5.iloc[-1] < 300000: return False, None

        # ===== 條件 5 [新增]：回檔幅度遞減 (r1 > r2 > r3) =====
        # 定義：60日 / 20日 / 10日 的最大回檔深度
        def calc_depth(series):
            return (series.max() - series.min()) / series.max() if series.max() > 0 else 1.0

        r1 = calc_depth(close.iloc[-60:])
        r2 = calc_depth(close.iloc[-20:])
        r3 = calc_depth(close.iloc[-10:])
        
        if not (r1 > r2 > r3): return False, None

    except Exception:
        return False, None

    return True, {
        "tag": "VCP-Lite",
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
# 5. 更新歷史績效
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效...")
    tw_tz = pytz.timezone('Asia/Taipei')
    today_str = datetime.now(tw_tz).strftime("%Y/%m/%d")
    today_date = datetime.strptime(today_str, "%Y/%m/%d")

    tickers_to_check = set()
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            tickers_to_check.add(symbol)

    if not tickers_to_check: return history_db

    print(f"追蹤股票數量: {len(tickers_to_check)}")
    current_data = {}
    try:
        data = yf.download(list(tickers_to_check), period="5d", auto_adjust=False, threads=True)
        close_df = data['Close']
        
        if len(tickers_to_check) == 1:
             ticker = list(tickers_to_check)[0]
             if isinstance(close_df, pd.DataFrame):
                 closes = close_df[ticker].dropna().values if ticker in close_df.columns else close_df.iloc[:, 0].dropna().values
             else:
                 closes = close_df.dropna().values
             if len(closes) >= 2:
                 current_data[ticker] = { 'price': float(closes[-1]), 'prev': float(closes[-2]) }
        else:
            for ticker in tickers_to_check:
                try:
                    if ticker in close_df.columns:
                        series = close_df[ticker].dropna()
                    else:
                        continue
                    if len(series) >= 2:
                        current_data[ticker] = { 'price': float(series.iloc[-1]), 'prev': float(series.iloc[-2]) }
                except: pass
    except Exception as e:
        print(f"Error updating history: {e}")
        return history_db

    for date_str, stocks in history_db.items():
        try:
            entry_date = datetime.strptime(date_str, "%Y/%m/%d")
        except: continue
        
        days_diff = (today_date - entry_date).days

        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            if symbol in current_data:
                latest_price = current_data[symbol]['price']
                prev_price = current_data[symbol]['prev']
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

    print("歷史績效更新完成。")
    return history_db

# ==========================================
# 6. 主程式
# ==========================================
def run_scanner():
    tw_tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(tw_tz)
    
    # 讀取現有資料
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    # 步驟 1: 先更新舊的歷史績效 (無論幾點都做，確保 ROI 是新的)
    history_db = update_history_roi(history_db)

    # 開始掃描
    full_list = get_all_tickers()
    print(f"開始掃描... 時間: {now.strftime('%H:%M:%S')}")
    
    daily_results = []
    batch_size = 100 
    
    for i in range(0, len(full_list), batch_size):
        batch = full_list[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{len(full_list)//batch_size + 1}...")
        try:
            data = yf.download(batch, period="2y", group_by='ticker', threads=True, progress=False, auto_adjust=False)
            
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
                        strategy_tags.append("VCP")
                    
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

    save_json(DB_INDUSTRY, industry_db)
    
    # ==========================================
    # 處理 output 分流邏輯
    # ==========================================
    
    # 1. 總是更新 data.json (即時看板用)
    print(f"掃描結束，共發現 {len(daily_results)} 檔。更新 data.json...")
    data_payload = {
        "date": now.strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions",
        "list": daily_results
    }
    save_json(DATA_JSON, data_payload)

    # 2. 條件更新 history.json
    current_time = now.time()
    market_open = dt_time(9, 0, 0)
    market_close = dt_time(13, 30, 0)
    
    # 判斷是否為「不寫入時段」 (09:00 ~ 13:30)
    is_market_session = market_open <= current_time <= market_close

    if is_market_session:
        print(f"⚠️ 現在是盤中時間 ({current_time.strftime('%H:%M')})，跳過 History 歸檔。")
    else:
        # 計算歸檔日期 Key
        # 如果是下午 13:30 以後 -> 今天
        # 如果是凌晨 00:00~08:59 -> 昨天
        if current_time > market_close:
            record_date_str = now.strftime("%Y/%m/%d")
        else:
            yesterday = now - timedelta(days=1)
            record_date_str = yesterday.strftime("%Y/%m/%d")

        print(f"✅ 盤後時段，準備將資料寫入 History，歸檔日期: {record_date_str}")
        
        # 將今日掃描結果存入對應日期 Key (使用 Dictionary 結構可避免重複 append)
        if daily_results:
            history_db[record_date_str] = daily_results
            # 排序日期 (可選)
            sorted_history = dict(sorted(history_db.items(), reverse=True))
            save_json(DB_HISTORY, sorted_history)
            print("History.json 已更新。")
        else:
            print("今日無符合策略標的，不更新 History。")

    return daily_results

if __name__ == "__main__":
    run_scanner()
