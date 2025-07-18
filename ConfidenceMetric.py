import pandas as pd
import numpy as np

#test data
df = pd.read_csv(r"C:\work\eup_ts_fuel_detection\merged_refuel_theft.csv")
print(df.head())

# Metric 1: Δsignal 與真實油量評分
# 計算每輛車的 k 值 (Δsignal / 真實油量)
def k_value(df, min_amount=30, default_k=40):
    
    df["voltage_change"] = df["voltage_change"].abs()
    #篩選出加油事件
    refuel_k = (df["event_type"] == "refuel") & (df["amount"] > min_amount)
    theft_k = (df["event_type"] == "theft")
    event_k = df[(refuel_k) | (theft_k)].copy()
    
    #計算k值(取每輛車中位數)
    k_map = (
        event_k.groupby("unicode")
        .apply(lambda x: np.median(x["voltage_change"] / x["amount"]))
        .replace([np.nan, np.inf], default_k)
        .fillna(default_k)
        .to_dict()
    ) 
    return k_map

# Δsignal 與真實油量評分
def fuel_signal_score(amount, voltage_change, k_value, tiny = 1e-6):
    if amount <= 0 or voltage_change <= 0:
        return 0.0
    ratio = amount / (voltage_change / k_value + tiny)
    score = np.exp(-abs(np.log2(max(ratio, tiny))))
    return score


# Metric 2: 事件持續時間評分
def duration_score(duration_min: pd.Series, 
                   event_series: pd.Series) -> pd.Series:
    low_dict = {"refuel": 1.0, "theft": 5.0}    #設定 Refuel 區間 1–30 min，中心 15 min
    high_dict = {"refuel": 30.0, "theft": 180.0} #設定 Withdraw 區間 5–180 min，中心 92.5 min
    
    low = event_series.map(low_dict)
    high = event_series.map(high_dict)
    mid = (low + high) / 2
    
    inside = (duration_min >= low) & (duration_min <= high)
    score = np.zeros_like(duration_min, dtype=float)
    score[inside] = 1 - np.abs(duration_min[inside] - mid[inside]) / (mid[inside] - low[inside])
    
    score[~inside] = np.maximum(
        0 , 1 - np.abs(duration_min[~inside] - low[~inside]) / high[~inside]
    )
    return score


# Metric 3: 加油/偷油量變化速率評分
def fuel_rate_score(amount, duration_min, event_series):
    # 計算油量變化速率
    rate = amount / np.clip(duration_min, 1e-6, None)
    score = np.empty(len(rate), dtype=float)
    
    events_map = {
        "refuel": np.array([0, 30, 60, 150], dtype=float),
        "theft": np.array([0, 1, 5, 20], dtype=float)
    }
    score_map = {
        "refuel": np.array([0.1 , 0.3, 0.6, 1], dtype=float),
        "theft": np.array([0.1, 0.3, 0.6, 1], dtype=float)
    }
    refuel_mask = (event_series == "refuel")
    theft_mask = (event_series == "theft")
    
    if refuel_mask.any():
    score[refuel_mask] = np.interp(refuel_mask, 
                                   events_map["refuel"], 
                                   score_map["refuel"],
                                   left = score_map["refuel"][0], 
                                   right = score_map["refuel"][-1])
    if theft_mask.any():
    score[theft_mask] = np.interp(theft_mask, 
                                  events_map["theft"], 
                                  score_map["theft"],
                                  left = score_map["theft"][0], 
                                  right = score_map["theft"][-1])
    
    return score

# Metric 4: 油量變化合理性
def fuel_change_score(amount, duration_min, event_series, capacity):
    
    if event_series == "refuel":
        low = 0.20 * capacity
        high = capacity
    else: #theft
        low = 0.05 * capacity
        high = capacity
    
    if low <= amount <= high:
        return 1.0
    elif amount < low:
        return max(0.2, (amount / low) * 0.8)
    else:
        return 0.0

# Metric 5: location score(靜止程度)
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c
    
def location_score(df, speed, start_lat, start_lon, end_lat, end_lon, speed):
    
    # 靜止程度
    speed_level = df["speed"].fillna(0)
    same_point = ()




# 計算所有事件的評分
k_map = k_value(df, min_amount=30, default_k=40)
print(k_map)
df_scored = df.copy()

df_scored["score"] = df_scored.apply(
    lambda row: fuel_signal_score(
        row["amount"], 
        row["voltage_change"], 
        k_map.get(row['unicode'], 40)
    ), 
    axis=1
)

# 分別獲取加油和偷油事件
refuel_df = df_scored[df_scored["event_type"] == "refuel"]
theft_df = df_scored[df_scored["event_type"] == "theft"]

print("加油事件評分:")
print(refuel_df[["unicode", "amount", "voltage_change", "score"]].head())
print("\n偷油事件評分:")
print(theft_df[["unicode", "amount", "voltage_change", "score"]].head())







