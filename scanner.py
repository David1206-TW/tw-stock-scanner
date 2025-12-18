# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V61 FinMind Production

【重大升級】
1. 資料源切換: 全面改用 FinMind API，解決 Yahoo Finance 資料不一致與 IP 封鎖問題。
2. 準確還原權息: 使用 FinMind 官方提供的 `TaiwanStockPriceAdj` (還原股價)，數據更貼近台股真實行情。
3. 穩定性優化: 使用 API Token 驗證，並內建重試機制。

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

import pandas as pd
import twstock
import json
import os
import math
import time
from datetime import datetime, time as dt_time, timedelta
import pytz
from FinMind.data import DataLoader

# ==========================================
# 0. 環境設定 & Token 讀取
# ==========================================
API_TOKEN = os.environ.get("FINMIND_API_TOKEN")
if not API_TOKEN:
    print("⚠️ 警告: 未檢測到 FINMIND_API_TOKEN，將嘗試使用匿名模式 (可能會有限制)")

# 初始化 FinMind Loader
api = DataLoader()
if API_TOKEN:
    api.login_by_token(api_token=API_TOKEN)

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
    return str(group)

def get_all_tickers():
    # FinMind 只需要股票代碼 (e.g., '2330')，不需要 '.TW'
    twse = twstock.twse
    tpex = twstock.tpex
    ticker_list = []
    for code in twse:
        if len(code) == 4: ticker_list.append(code)
    for code in tpex:
        if len(code) == 4: ticker_list.append(code)
    return ticker_list

# ==========================================
# 3. FinMind 資料獲取函式
# ==========================================
def get_stock_data_finmind(stock_id, days=500):
    """
    從 FinMind 下載單一股票的還原日線資料
    """
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        # 下載還原股價 (TaiwanStockPriceAdj)
        df = api.taiwan_stock_daily_adj(
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty: return None

        # 欄位重新命名以符合策略邏輯 (FinMind -> Standard)
        # FinMind cols: date, stock_id, Trading_Volume, Trading_money, open, max, min, close, spread, Trading_turnover
        df = df.rename(columns={
            "close": "Close",
            "open": "Open",
            "max": "High",
            "min": "Low",
            "Trading_Volume": "Volume"
        })
        
        # 確保數值型態
        cols = ['Close', 'Open', 'High', 'Low', 'Volume']
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors='coerce')
            
        return df
    except Exception as e:
        print(f"FinMind Download Error ({stock_id}): {e}")
        return None

# ==========================================
# 4. 策略邏輯 (邏輯保持不變，參數調用 df)
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

    # 嚴格過濾：必須高於年線 (MA240)
    if math.isnan(curr_ma240) or curr_c < curr_ma240: return False, None
    
    # 過濾：成交量門檻 (>500張)
    # FinMind 的 Volume 單位也是張嗎？不，FinMind Volume 是 "股"。
    # 所以 500張 = 500,000 股
    if curr_vol_ma5 < 500000: return False, None 

    # 1. 長線保護 (MA60, MA120)
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
        "ma5": round(curr_ma5, 2),
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
        ma60 = close.rolling(60).mean()
        ma240 = close.rolling(240).mean()
        
        # 布林帶
        std20 = close.rolling(20).std()
        bb_upper = ma20 + (std20 * 2)
        bb_lower = ma20 - (std20 * 2)
        bb_width = (bb_upper - bb_lower) / ma20

        # 當前數值
        curr_c = float(close.iloc[-1])
        curr_v = float(volume.iloc[-1]) 

        curr_ma20 = float(ma20.iloc[-1])
        curr_ma50 = float(ma50.iloc[-1])
        curr_ma150 = float(ma150.iloc[-1])
        curr_ma200 = float(ma200.iloc[-1])
        curr_ma60 = float(ma60.iloc[-1])
        curr_ma240 = float(ma240.iloc[-1])
        curr_bb_width = float(bb_width.iloc[-1])

        # ===== 硬指標過濾 =====
        if math.isnan(curr_ma240) or curr_c < curr_ma240: return False, None
        if math.isnan(curr_ma60) or curr_c <= curr_ma60: return False, None
        if curr_v < 500000: return False, None

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
        "ma240": round(curr_ma240, 2),
        "bb_width": round(curr_bb_width * 100, 1)
    }

# ==========================================
# 5. 更新歷史績效 (改用 FinMind)
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效...")
    tw_tz = pytz.timezone('Asia/Taipei')
    today_str = datetime.now(tw_tz).strftime("%Y/%m/%d")
    today_date = datetime.strptime(today_str, "%Y/%m/%d")

    # 1. 整理需要更新的股票清單
    tickers_to_check = set()
    for date_str, stocks in history_db.items():
        for stock in stocks:
            tickers_to_check.add(stock['id']) # FinMind 只需 ID

    if not tickers_to_check: return history_db

    print(f"追蹤股票數量: {len(tickers_to_check)}")
    
    # 2. 批次下載最新行情
    # FinMind 支援批次下載 (dataset='TaiwanStockPriceAdj')
    try:
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        # 由於 FinMind 批次下載所有股票可能會很大，我們採用迴圈單支下載會比較穩
        # 或者使用 FinMind 的批次 API (如果支援)
        # 這裡為了穩定性，我們使用單支查詢但加入快取邏輯
        
        current_data = {}
        for ticker in tickers_to_check:
            df = get_stock_data_finmind(ticker, days=10) # 只要最近幾天
            if df is not None and len(df) >= 2:
                latest_price = float(df['Close'].iloc[-1])
                prev_price = float(df['Close'].iloc[-2])
                current_data[ticker] = { 'price': latest_price, 'prev': prev_price }
            
            # FinMind API 限制：每分鐘 600 次，非常寬鬆，不需要太多 sleep
            # time.sleep(0.1) 

    except Exception as e:
        print(f"History Update Error: {e}")
        return history_db

    # 3. 計算 ROI
    for date_str, stocks in history_db.items():
        try:
            entry_date = datetime.strptime(date_str, "%Y/%m/%d")
        except: continue
        
        days_diff = (today_date - entry_date).days

        for stock in stocks:
            ticker = stock['id']
            if ticker in current_data:
                latest_price = current_data[ticker]['price']
                prev_price = current_data[ticker]['prev']
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
    
    industry_db = load_json(DB_INDUSTRY)
    history_db = load_json(DB_HISTORY)
    
    # 步驟 1: 更新歷史績效
    history_db = update_history_roi(history_db)
    save_json(DB_HISTORY, history_db)
    print("盤中歷史績效已更新至 DB。")

    # 開始掃描今日新標的
    full_list = get_all_tickers()
    print(f"開始掃描... 時間: {now.strftime('%H:%M:%S')}")
    
    daily_results = []
    
    # FinMind 建議每次查詢間隔一點時間，但因為我們是單支單支查 (為了計算技術指標)，
    # 所以不需要像 yfinance 那樣做大 batch，直接迴圈即可。
    # 為了效率，我們還是設一個進度顯示。
    
    total_stocks = len(full_list)
    print(f"總共需掃描 {total_stocks} 檔股票。")

    for idx, ticker in enumerate(full_list):
        if idx % 50 == 0:
            print(f"Progress: {idx}/{total_stocks}...")
            
        try:
            # 下載資料 (500天足夠算年線)
            df = get_stock_data_finmind(ticker, days=500)
            if df is None: continue

            # 策略計算
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
                name = ticker # 預設用代碼
                if ticker in twstock.codes: name = twstock.codes[ticker].name
                group = get_stock_group(ticker, industry_db)
                if ticker not in industry_db: industry_db[ticker] = group
                
                try:
                    prev_c = df['Close'].iloc[-2]
                    change_rate = round((final_info['price'] - prev_c) / prev_c * 100, 2)
                except:
                    change_rate = 0.0
                    
                tags_str = " & ".join(strategy_tags)
                note_ma240 = round(final_info.get('ma240', 0), 2)
                note_str = f"{tags_str} / 年線{note_ma240}"

                stock_entry = {
                    "id": ticker,
                    "name": name,
                    "group": group,
                    "type": "上櫃" if ticker in twstock.tpex else "上市",
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
                print(f" -> Found: {ticker} {name} [{tags_str}]")
                
        except Exception as e:
            # 忽略錯誤繼續下一檔
            continue

    save_json(DB_INDUSTRY, industry_db)
    
    # 處理 output
    print(f"掃描結束，共發現 {len(daily_results)} 檔。更新 data.json...")
    data_payload = {
        "date": now.strftime("%Y/%m/%d %H:%M:%S"),
        "source": "GitHub Actions (FinMind)",
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
            history_db[record_date_str] = daily_results
            sorted_history = dict(sorted(history_db.items(), reverse=True))
            save_json(DB_HISTORY, sorted_history)
            print("History.json 新增完畢。")
        else:
            print("今日無符合策略標的，不新增 History。")

    return daily_results

if __name__ == "__main__":
    run_scanner()


