# -*- coding: utf-8 -*-
"""
台股自動掃描策略機器人 (Scanner Bot) - V59.3 純粹窒息量樞紐版

【版本資訊】
Base Version: V58.4
Update V58.5:
1. [MA定義確認] 代碼中的 MA (rolling) 本身即為 K棒定義，無需修改。
2. [ROI優化] 將歷史績效追蹤從「日曆天」改為「K棒數 (Trading Days)」。
Update V58.8:
1. [策略C極限壓縮] 距離前高的容忍值縮小至 -1% ~ +2%，只抓「貼臉前高」。
Update V58.9:
1. [位階過濾殺手鐧] 新增「歷史新高」位階過濾，近期前高必須是近兩年最高點。
2. [長天期多頭確認] 新增 MA60 > MA120 條件。
Update V59.0:
1. [防追高與量縮樞紐] 新增「不可脫離五日線太遠 (乖離<2.5%)」且「今日比昨日量縮」。
Update V59.1:
1. [極致量縮與柄部放寬] 將量縮條件加嚴為「今日成交量 <= 昨日的 50%」。
2. 將逼近前高的條件放寬至「距離前高 -9% ~ +2% 以內」。
Update V59.3 (回歸純粹型態):
1. [移除不必要條件] 移除 V59.2 的歷史 5 倍爆量條件，避免過度擬合。
2. [確立核心過濾] 嚴格執行「前高距離 -9% ~ +2%」搭配「成交量縮減 50% 以上」的純粹窒息量樞紐點。

【新增排除條件 (兩策略皆適用)】
1. 墓碑線排除：當日K線只有上引線(>0.2%)，沒有下引線(<0.1%)。
2. 破底排除：當日最低價小於前日最低價 1.5% 以上。
3. 扣抵值排除：當日收盤價 < 20交易日前收盤價 (確保趨勢向上)。

【保留策略說明】
1. 策略 A (拉回佈局): 
   1. 長線保護：收盤 > MA300, MA120, MA60。
   2. 多頭排列：MA10 > MA60 > MA120 > MA240。
   3. 位階安全：乖離率 < 25%。
   4. 均線糾結：差異 < 8%。
   5. 量縮整理：成交量 < 5日均量。
   6. 支撐確認：收盤 > MA12。
   7. K線收斂：當日振幅 < 4.5% 且 實體幅度 < 2.5%。
   8. 流動性：5日均量 > 1000張。
   9. 趨勢支撐：當日收盤 > 20日均線扣抵值。
2. 策略 B (Strict VCP):
   1. 硬指標過濾：股價 > MA300 & > MA60 & 成交量 > 1000張。
   2. 多頭排列：MA60 > MA120 > MA240。
   3. 價格位階：靠近 52 週新高。
   4. 波動收縮：布林帶寬度 < 15%。
   5. 量能遞減：5日均量 < 20日均量。
   6. 回檔收縮：r1(60日) > r2(20日) > r3(10日)。
   7. 趨勢支撐：當日收盤 > 20日均線扣抵值。
3. 策略 C (N字形上攻 - 純粹窒息量樞紐版):
   1. 長線保護：股價必須在年線之上，且 MA60 > MA120。
   2. 創高位階：尋找近30天前高，且該前高必須是「近兩年來的最高點」。
   3. 實質回檔：前高之後的低點，跌落超過 8%。
   4. 柄部起漲：今日收盤價距離前高在 -9% 到 +2% 以內。
   5. 短線轉強：今日收盤價站穩 5MA 與 10MA 之上。
   6. 窒息量防追高：今日成交量 <= 昨日成交量的 50%，且距離 5MA 乖離不超過 2.5%。
"""

import yfinance as yf
import pandas as pd
import twstock
import json
import os
import math
import numpy as np
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
# 3. 策略邏輯 (V59.3)
# ==========================================

def check_strategy_original(df):
    """
    策略 A：拉回佈局 (含交易日扣抵值過濾)
    """
    if len(df) < 310: return False, None
    
    close = df['Close']
    open_p = df['Open']
    high = df['High'] 
    volume = df['Volume']
    low = df['Low']
    
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma12 = close.rolling(12).mean()
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    ma120 = close.rolling(120).mean()
    ma240 = close.rolling(240).mean()
    ma300 = close.rolling(300).mean()
    
    vol_ma5 = volume.rolling(5).mean()
    
    curr_c = float(close.iloc[-1])
    curr_o = float(open_p.iloc[-1])
    curr_h = float(high.iloc[-1])
    curr_v = float(volume.iloc[-1])
    curr_l = float(low.iloc[-1])
    
    prev_c = float(close.iloc[-2])
    prev_l = float(low.iloc[-2])
    
    curr_ma5 = float(ma5.iloc[-1])
    curr_ma10 = float(ma10.iloc[-1])
    curr_ma12 = float(ma12.iloc[-1])
    curr_ma20 = float(ma20.iloc[-1])
    curr_ma60 = float(ma60.iloc[-1])
    curr_ma120 = float(ma120.iloc[-1]) 
    curr_ma240 = float(ma240.iloc[-1])
    curr_ma300 = float(ma300.iloc[-1])
    
    curr_vol_ma5 = float(vol_ma5.iloc[-1])

    # === 0. 風控排除 ===
    upper_shadow = curr_h - max(curr_c, curr_o)
    lower_shadow = min(curr_c, curr_o) - curr_l
    if (upper_shadow / curr_c > 0.002) and (lower_shadow / curr_c < 0.001): return False, None
    if prev_l > 0 and (prev_l - curr_l) / prev_l > 0.015: return False, None
    deduction_20 = float(close.iloc[-20])
    if curr_c < deduction_20: return False, None

    # === 1. 基本過濾 ===
    if math.isnan(curr_ma300): return False, None 
    if curr_c < curr_ma300: return False, None    
    if curr_vol_ma5 < 1000000: return False, None 

    # === 2. 策略核心 ===
    if curr_c <= curr_ma120 or curr_c <= curr_ma60: return False, None
    if math.isnan(curr_ma240): return False, None
    if not (curr_ma10 > curr_ma60 > curr_ma120 > curr_ma240): return False, None
    
    bias_ma60 = (curr_c - curr_ma60) / curr_ma60
    if bias_ma60 >= 0.25: return False, None
    
    mas = [curr_ma5, curr_ma10, curr_ma20]
    ma_divergence = (max(mas) - min(mas)) / min(mas)
    if ma_divergence >= 0.08: return False, None
    
    if curr_v >= curr_vol_ma5: return False, None
    if curr_c <= curr_ma12: return False, None
    
    daily_range_pct = (curr_h - curr_l) / prev_c
    if daily_range_pct >= 0.045: return False, None
    entity_pct = abs(curr_c - curr_o) / prev_c
    if entity_pct >= 0.025: return False, None

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
    策略 B：Strict VCP
    """
    try:
        close = df['Close']
        open_p = df['Open']
        high = df['High']
        low = df['Low']
        volume = df['Volume']

        if len(close) < 310: return False, None

        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ma60 = close.rolling(60).mean()
        ma300 = close.rolling(300).mean()
        
        ma120 = close.rolling(120).mean()
        ma240 = close.rolling(240).mean()
        
        std20 = close.rolling(20).std()
        bb_upper = ma20 + (std20 * 2)
        bb_lower = ma20 - (std20 * 2)
        bb_width = (bb_upper - bb_lower) / ma20

        curr_c = float(close.iloc[-1])
        curr_o = float(open_p.iloc[-1])
        curr_h = float(high.iloc[-1])
        curr_l = float(low.iloc[-1])
        curr_v = float(volume.iloc[-1])

        prev_l = float(low.iloc[-2])

        curr_ma20 = float(ma20.iloc[-1])
        curr_ma50 = float(ma50.iloc[-1])
        curr_ma150 = float(ma150.iloc[-1])
        curr_ma200 = float(ma200.iloc[-1])
        curr_ma60 = float(ma60.iloc[-1])
        curr_ma300 = float(ma300.iloc[-1])
        
        curr_ma120 = float(ma120.iloc[-1])
        curr_ma240 = float(ma240.iloc[-1])
        
        curr_bb_width = float(bb_width.iloc[-1])

        # === 0. 風控排除 ===
        upper_shadow = curr_h - max(curr_c, curr_o)
        lower_shadow = min(curr_c, curr_o) - curr_l
        if (upper_shadow / curr_c > 0.002) and (lower_shadow / curr_c < 0.001): return False, None
        if prev_l > 0 and (prev_l - curr_l) / prev_l > 0.015: return False, None
        deduction_20 = float(close.iloc[-20])
        if curr_c < deduction_20: return False, None

        if math.isnan(curr_ma300) or curr_c < curr_ma300: return False, None
        if math.isnan(curr_ma60) or curr_c <= curr_ma60: return False, None
        if math.isnan(curr_ma120) or math.isnan(curr_ma240): return False, None
        if not (curr_ma60 > curr_ma120 > curr_ma240): return False, None
        if curr_v < 1000000: return False, None

        if curr_c < curr_ma200: return False, None
        if curr_ma200 <= float(ma200.iloc[-20]): return False, None
        if curr_c < curr_ma150: return False, None

        high_52w = close.iloc[-250:].max()
        low_52w = close.iloc[-250:].min()
        if curr_c < low_52w * 1.3: return False, None
        if curr_c < high_52w * 0.75: return False, None

        if curr_bb_width > 0.15: return False, None
        if curr_c < curr_ma20 * 0.98: return False, None

        vol_ma5 = volume.rolling(5).mean()
        vol_ma20 = volume.rolling(20).mean()
        if float(vol_ma5.iloc[-1]) > float(vol_ma20.iloc[-1]): return False, None
        if float(vol_ma5.iloc[-1]) < 300000: return False, None

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

def check_strategy_n_shape(df):
    """
    策略 C：N字形上攻 (V59.3 純粹窒息量樞紐版)
    專抓收盤價在兩年新高下緣 (放寬至9%)，且今日量縮一半以上、未大幅偏離 5MA 的蓄勢極品
    """
    try:
        if len(df) < 250: return False, None

        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume']

        curr_c = float(close.iloc[-1])
        curr_v = float(volume.iloc[-1])
        prev_v = float(volume.iloc[-2]) # 取得昨日成交量
        
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()
        ma240 = close.rolling(240).mean()
        ma300 = close.rolling(300).mean()

        curr_ma5 = float(ma5.iloc[-1])
        curr_ma10 = float(ma10.iloc[-1])
        curr_ma20 = float(ma20.iloc[-1])
        curr_ma60 = float(ma60.iloc[-1])
        curr_ma120 = float(ma120.iloc[-1])
        curr_ma240 = float(ma240.iloc[-1])
        curr_ma300 = float(ma300.iloc[-1])

        # ==========================================
        # 🛡️ 條件零：絕對多頭排列
        # ==========================================
        if math.isnan(curr_ma240) or curr_c < curr_ma240: return False, None
        if not (curr_ma60 > curr_ma120): return False, None

        # ==========================================
        # 🎯 條件一：找出 N 字形的「左側高點 (近期前高)」與「底部回檔」
        # ==========================================
        highs_window = high.iloc[-30:-3]
        if len(highs_window) == 0: return False, None
        
        peak_high = float(highs_window.max())
        peak_pos_in_slice = np.argmax(highs_window.values)
        peak_abs_pos = len(df) - 30 + peak_pos_in_slice

        pullback_zone = low.iloc[peak_abs_pos : -1]
        if len(pullback_zone) < 2: return False, None
        pullback_low = float(pullback_zone.min())

        # ==========================================
        # ⭐ 條件二：創歷史新高位階確認
        # ==========================================
        historical_high = float(high.max())
        if peak_high < historical_high * 0.97: return False, None

        # ==========================================
        # 🎯 條件三：有實質洗盤回檔 (高低點落差至少大於 8%)
        # ==========================================
        if pullback_low <= 0: return False, None
        if peak_high / pullback_low < 1.08: return False, None

        # ==========================================
        # 🎯 條件四：柄部起漲 (放寬距離至 9%)
        # ==========================================
        # 今天的收盤價，必須距離前高在 -9% 到 +2% 以內，涵蓋了洗盤剛站上均線的甜蜜點
        near_peak = (curr_c >= peak_high * 0.91) and (curr_c <= peak_high * 1.02)

        # ==========================================
        # 🎯 條件五：短線重回多頭
        # ==========================================
        short_trend_up = (curr_c > curr_ma5) and (curr_c > curr_ma10)

        # ==========================================
        # ⭐ 條件六：極致窒息量與防追高樞紐
        # ==========================================
        # 1. 乖離率 <= 2.5% (緊貼五日線)
        ma5_bias = (curr_c - curr_ma5) / curr_ma5 if curr_ma5 > 0 else 1.0
        not_overextended = ma5_bias <= 0.025
        
        # 2. 今日成交量必須 <= 昨日成交量的一半 (縮量 50% 以上)
        volume_contraction = curr_v <= (prev_v * 0.5)

        # ==========================================
        # 🎯 條件七：成交量基礎過濾
        # ==========================================
        vol_ma5 = float(volume.rolling(5).mean().iloc[-1])
        if vol_ma5 < 800000: return False, None

        # 綜合判定
        if near_peak and short_trend_up and not_overextended and volume_contraction:
            return True, {
                "tag": "N字形",
                "price": round(curr_c, 2),
                "ma5": round(curr_ma5, 2),
                "ma10": round(curr_ma10, 2),
                "ma20": round(curr_ma20, 2),
                "ma300": round(curr_ma300, 2) if not math.isnan(curr_ma300) else 0.0
            }

        return False, None
    except Exception:
        return False, None


# ==========================================
# 4. 更新歷史績效 (改為 K棒數計算)
# ==========================================
def update_history_roi(history_db):
    print("正在更新歷史名單績效 (K-Bar ROI Tracking)...")
    tickers_to_check = set()
    
    # 這裡只需要下載資料，不需要算今天日期 (因為是看 K 棒相對位置)
    for date_str, stocks in history_db.items():
        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            tickers_to_check.add(symbol)

    if not tickers_to_check: return history_db

    print(f"追蹤股票數量: {len(tickers_to_check)}，下載 2 年歷史資料...")
    
    close_df = None
    try:
        data = yf.download(list(tickers_to_check), period="2y", auto_adjust=True, threads=True, progress=False)
        
        if isinstance(data, pd.DataFrame):
            if 'Close' in data.columns and isinstance(data.columns, pd.MultiIndex):
                close_df = data['Close']
            elif 'Close' in data.columns:
                if len(tickers_to_check) == 1:
                    ticker = list(tickers_to_check)[0]
                    close_df = pd.DataFrame({ticker: data['Close']})
                else:
                    close_df = data['Close']
            else:
                 close_df = data
        
        if close_df is not None and close_df.index.tz is not None:
            close_df.index = close_df.index.tz_localize(None)
            
    except Exception as e:
        print(f"Error downloading history data: {e}")
        return history_db

    if close_df is None or close_df.empty:
        print("⚠️ 無法取得歷史股價資料，跳過 ROI 更新。")
        return history_db

    # Helper: 取得該股票的 Series
    def get_stock_series(ticker_symbol, dataframe):
        try:
            target_col = None
            if ticker_symbol in dataframe.columns:
                target_col = ticker_symbol
            elif ticker_symbol.split('.')[0] in dataframe.columns:
                target_col = ticker_symbol.split('.')[0]
            else:
                simple_code = ticker_symbol.split('.')[0]
                for col in dataframe.columns:
                    if simple_code == str(col).split('.')[0]:
                        target_col = col
                        break
            
            if not target_col: return None
            return dataframe[target_col].dropna()
        except: return None

    # Helper: 解析日期
    def parse_record_date(date_str):
        formats = ["%Y/%m/%d", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]
        for fmt in formats:
            try: return datetime.strptime(date_str, fmt).date()
            except ValueError: continue
        return None

    # 開始遍歷歷史紀錄
    for date_str, stocks in history_db.items():
        record_date_obj = parse_record_date(date_str)
        if not record_date_obj: continue
        
        # 將 datetime.date 轉為 pd.Timestamp 以便比對 Index
        record_ts = pd.Timestamp(record_date_obj)

        for stock in stocks:
            symbol = stock['id'] + ('.TW' if stock['type'] == '上市' else '.TWO')
            buy_price = float(stock['buy_price'])
            
            series = get_stock_series(symbol, close_df)
            if series is None or series.empty: continue

            # 1. 找到進場日在 series 中的位置 (Index Location)
            try:
                start_idx = series.index.searchsorted(record_ts)
                
                if start_idx >= len(series): continue
                
                found_date = series.index[start_idx]
                if (found_date - record_ts).days > 7: pass

            except Exception: continue

            # 2. 計算目前持有幾根 K 棒
            current_idx = len(series) - 1
            bars_held = current_idx - start_idx
            
            # 存回 stock 物件，方便前端參考
            stock['days_held'] = int(bars_held) 

            # 3. 更新最新報價與 ROI
            latest_price = float(series.iloc[-1])
            roi = round(((latest_price - buy_price) / buy_price) * 100, 2)
            
            stock['latest_price'] = round(latest_price, 2)
            stock['roi'] = roi
            
            if len(series) >= 2:
                prev_price = float(series.iloc[-2])
                stock['daily_change'] = round(((latest_price - prev_price) / prev_price) * 100, 2)

            # 4. 里程碑鎖定 (基於 K 棒數)
            targets = [
                (1, 'perf_1d'),
                (5, 'perf_5d'),
                (10, 'perf_10d'),
                (20, 'perf_20d'),
                (60, 'perf_60d'),
                (120, 'perf_120d')
            ]

            for bar_threshold, field_name in targets:
                if bars_held >= bar_threshold:
                    target_idx = start_idx + bar_threshold
                    
                    if target_idx < len(series):
                        lock_price = float(series.iloc[target_idx])
                        lock_roi = round(((lock_price - buy_price) / buy_price) * 100, 2)
                        stock[field_name] = lock_roi

    print("歷史績效更新完成 (K-Bar Based)。")
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

                    required_cols = ['Close', 'Volume', 'Low', 'High', 'Open']
                    if not all(col in df.columns for col in required_cols): continue

                    is_match_1, info_1 = check_strategy_original(df)
                    is_match_2, info_2 = check_strategy_vcp_pro(df)
                    is_match_3, info_3 = check_strategy_n_shape(df)
                    
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
                    if is_match_3:
                        final_match = True
                        if not final_info: final_info = info_3
                        strategy_tags.append("N字形")
                    
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
                            "daily_change": change_rate,
                            "perf_1d": None, "perf_5d": None, "perf_10d": None,
                            "perf_20d": None, "perf_30d": None, "perf_60d": None, "perf_120d": None
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
