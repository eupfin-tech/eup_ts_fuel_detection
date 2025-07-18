
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
def analyze_unicode_ranking(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        return None
    unicode_stats = df.groupby('Unicode').agg({
        'Match_Rate': 'mean',
        'Unicode': 'count',
        'RO_RefuelAmount': ['mean', 'sum', 'std', 'count']
    }).rename(columns={'Unicode': 'Event_Count'})
    
    # 扁平化列名
    unicode_stats.columns = ['Match_Rate', 'Event_Count', 'RO_RefuelAmount_Mean', 'RO_RefuelAmount_Sum', 'RO_RefuelAmount_Std', 'RO_RefuelAmount_Count']
    unicode_stats = unicode_stats.reset_index()
    
    unicode_ranking = unicode_stats.sort_values('Match_Rate', ascending=False)
    unicode_ranking['Rank'] = range(1, len(unicode_ranking) + 1)
    result = unicode_ranking[['Rank', 'Unicode', 'Match_Rate', 'Event_Count', 'RO_RefuelAmount_Mean', 'RO_RefuelAmount_Sum', 'RO_RefuelAmount_Std']].copy()
    result['Match_Rate'] = result['Match_Rate'].round(4)
    result['RO_RefuelAmount_Mean'] = result['RO_RefuelAmount_Mean'].round(2)
    result['RO_RefuelAmount_Sum'] = result['RO_RefuelAmount_Sum'].round(2)
    result['RO_RefuelAmount_Std'] = result['RO_RefuelAmount_Std'].round(2)
    return result

def filter_unicode_by_fuel_sensor(csv_file_path, fuel_sensor_product=None):
    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        return None
    
    if fuel_sensor_product:
        fuel_sensor_product = fuel_sensor_product.upper()
        filtered_df = df[df['FuelSensorName'].str.upper() == fuel_sensor_product]
        
        if filtered_df.empty:
            return None
        
        result_df = filtered_df[['Unicode', 'FuelSensorName', 'FuelSensorId', 'DeviceProduct', 'capacity']].copy()
        result_df = result_df.drop_duplicates(subset=['Unicode'])
        result_df = result_df.sort_values('Unicode')
        
        return result_df
    else:
        fuel_sensor_stats = df.groupby('FuelSensorName').agg({
            'Unicode': 'nunique',
            'FuelSensorName': 'count'
        }).rename(columns={'Unicode': 'Unique_Unicode_Count', 'FuelSensorName': 'Total_Records'})
        
        fuel_sensor_stats = fuel_sensor_stats.reset_index()
        fuel_sensor_stats = fuel_sensor_stats.sort_values('Unique_Unicode_Count', ascending=False)
        
        return fuel_sensor_stats

def get_available_fuel_sensor_types(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        unique_types = df['FuelSensorName'].unique()
        return sorted(unique_types)
    except Exception as e:
        return []

def top_match_rate(result_df):
    if result_df is None:
        return
    top_events = result_df.nlargest(1000000, 'Match_Rate')[['Unicode', 'Match_Rate', 'Event_Count', 'RO_RefuelAmount_Mean']]
    return top_events
    
def save_results(result_df, output_file='unicode_ranking_results.csv'):
    if result_df is None:
        return
    try:
        result_df.to_csv(output_file, index=False, encoding='utf-8')
    except Exception as e:
        pass

def get_specific_accuracy_by_unicode(ranking_result, unicode):
    # 提供Unicode，然後顯示該Unicode的ranking_result
    return ranking_result[ranking_result['Unicode'] == unicode]

def overall_statistics(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        return None
    
    overall_stats = {
        'Total_Events': len(df),
        'Overall_Match_Rate': df['Match_Rate'].mean(),
        'Match_Rate_Std': df['Match_Rate'].std(),
        'Unique_Unicode_Count': df['Unicode'].nunique(),
        'Overall_RO_RefuelAmount_Mean': df['RO_RefuelAmount'].mean(),
        'Overall_RO_RefuelAmount_Sum': df['RO_RefuelAmount'].sum(),
        'Overall_RO_RefuelAmount_Std': df['RO_RefuelAmount'].std()
    }
    
    return overall_stats

def print_overall_statistics(stats):
    if stats is None:
        print("無法獲取統計資料")
        return
    
    print("=== 整體統計資料 ===")
    print(f"總事件數: {stats['Total_Events']:,}")
    print(f"整體平均命中率: {stats['Overall_Match_Rate']:.4f}")
    print(f"命中率標準差: {stats['Match_Rate_Std']:.4f}")
    print(f"唯一 Unicode 數量: {stats['Unique_Unicode_Count']:,}")
    print(f"平均加油量: {stats['Overall_RO_RefuelAmount_Mean']:.2f}")
    print(f"總加油量: {stats['Overall_RO_RefuelAmount_Sum']:.2f}")
    print(f"加油量標準差: {stats['Overall_RO_RefuelAmount_Std']:.2f}")
    print("=" * 25)

def plot_match_rate_by_refuel_amount(csv_file_path, bin_size=30):
    try:
        df = pd.read_csv(csv_file_path)
    except Exception as e:
        return None
    
   #df = df[df['RO_RefuelAmount'] >= 30]
    
    bins = list(range(30, 500, bin_size)) + [500, float('inf')]
    bin_labels = [f"{i}-{min(i+bin_size, 500)}" for i in range(30, 500, bin_size)] + ["500+"]
    
    df['RefuelAmount_Group'] = pd.cut(df['RO_RefuelAmount'], bins=bins, labels=bin_labels, include_lowest=True)
    
    grouped_stats = df.groupby('RefuelAmount_Group').agg({
        'Match_Rate': ['mean', 'std', 'count'],
        'RO_RefuelAmount': 'mean'
    }).round(4)
    
    grouped_stats.columns = ['Match_Rate_Mean', 'Match_Rate_Std', 'Event_Count', 'RefuelAmount_Mean']
    grouped_stats = grouped_stats.reset_index()
    grouped_stats = grouped_stats[grouped_stats['Event_Count'] > 0]
    
    plt.figure(figsize=(12, 6))
    x_pos = range(len(grouped_stats))
    
    bars = plt.bar(x_pos, grouped_stats['Match_Rate_Mean'], 
                   alpha=0.7, color='skyblue')
    
    plt.xlabel('Refuel Amount Range (Liters)')
    plt.ylabel('Average Match Rate')
    plt.title(f'Match Rate Analysis by Refuel Amount Range (Every {bin_size} Liters)')
    plt.xticks(x_pos, grouped_stats['RefuelAmount_Group'], rotation=45)
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.show()
    
    return grouped_stats

if __name__ == "__main__":
    car_file = "my_car.csv"
    ro_file = r"C:\Users\ken-liao\rebit_fuel_comparison_march_april.csv"
    ranking_result = analyze_unicode_ranking(ro_file)
    #tradition_result = filter_unicode_by_fuel_sensor(car_file, 'TRADITION')
    
    overall_stats = overall_statistics(ro_file)
    print_overall_statistics(overall_stats)
    
    print(top_match_rate(ranking_result))
    
    refuel_analysis = plot_match_rate_by_refuel_amount(ro_file, bin_size=30)
    if refuel_analysis is not None:
        print(refuel_analysis.to_string(index=False))
