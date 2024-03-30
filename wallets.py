import pandas as pd
import json
import numpy as np
import socket 
from datetime import datetime

df = pd.read_csv('transactions_for_claster.csv')

wallets = {}

def check_blacklist(btc_address):
    try:
        hostname = f"{btc_address}.bl.btcblack.it"
        ip_address = socket.gethostbyname(hostname)
        return ip_address == "127.0.0.2"
    except socket.gaierror:
        return False

# Функция для обновления агрегированных данных кошелька
def update_wallet(wallet, amount, is_input, inputs, outputs, row):
    timestamp = datetime.fromtimestamp(row['Timestamp'])
    if wallet not in wallets:
        wallets[wallet] = {'total_sent': 0, 'total_received': 0, 'tx_amounts': [],
                           'tx_timestamps': [], 'unique_addresses': set(),
                           'total_inputs': 0, 'total_outputs': 0, 'tx_counts': 0,
                           'blacklisted': check_blacklist(wallet)}
    
    if is_input:
        wallets[wallet]['total_received'] += amount
    else:
        wallets[wallet]['total_sent'] += amount

    wallets[wallet]['tx_amounts'].append(amount)
    wallets[wallet]['tx_timestamps'].append(timestamp)
    wallets[wallet]['unique_addresses'].update(inputs.keys())
    wallets[wallet]['unique_addresses'].update(outputs.keys())
    wallets[wallet]['total_inputs'] += row['TotalInputs']
    wallets[wallet]['total_outputs'] += row['TotalOutputs']
    wallets[wallet]['tx_counts'] += 1

# Обработка каждой строки в датасете
for index, row in df.iterrows():
    if index % 100 == 0:  # Вывод информации каждые 100 строк
        print(f"Обрабатывается строка {index} из {len(df)}")


    inputs = json.loads(row['Inputs'])
    outputs = json.loads(row['Outputs'])
    total_amount = row['TotalAmount']
    
    for wallet in inputs:
        update_wallet(wallet, total_amount, True, inputs, outputs, row)

    for wallet in outputs:
        update_wallet(wallet, total_amount, False, inputs, outputs, row)

# Расчет интервалов между транзакциями
def calculate_intervals(timestamps):
    timestamps.sort()
    intervals = [(timestamps[i] - timestamps[i-1]).total_seconds() for i in range(1, len(timestamps))]
    return intervals if intervals else [0]

# Преобразование словаря в DataFrame и расчет дополнительных признаков
wallets_df = pd.DataFrame.from_dict(wallets, orient='index')
wallets_df['wallet'] = wallets_df.index
wallets_df['unique_address_count'] = wallets_df['unique_addresses'].apply(len)
wallets_df['avg_interval_between_tx'] = wallets_df['tx_timestamps'].apply(calculate_intervals).apply(np.mean)
wallets_df['avg_tx_amount'] = wallets_df['tx_amounts'].apply(np.mean)
wallets_df['max_tx_amount'] = wallets_df['tx_amounts'].apply(max)
wallets_df['min_tx_amount'] = wallets_df['tx_amounts'].apply(min)
wallets_df['avg_inputs'] = wallets_df['total_inputs'] / wallets_df['tx_counts']
wallets_df['avg_outputs'] = wallets_df['total_outputs'] / wallets_df['tx_counts']

# Очистка от временных колонок
wallets_df.drop(columns=['tx_amounts', 'tx_timestamps', 'unique_addresses', 'total_inputs', 'total_outputs', 'tx_counts'], inplace=True)

# Сохранение обработанного датасета в файл CSV
wallets_df.to_csv('wallets.csv', index=False)

print("Датасет кошельков сохранен в файл 'wallets.csv'.")
