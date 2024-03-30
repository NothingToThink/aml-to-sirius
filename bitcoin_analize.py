import requests
import csv
import json
import socket 
from datetime import datetime

def get_block_info(block_hash):
    try:
        response = requests.get(f"https://blockchain.info/rawblock/{block_hash}")
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        print(f"Ошибка HTTP: {e}")
        return None
    except Exception as e:
        print(f"Ошибка: {e}")
        return None

def check_blacklist(btc_address):
    try:
        hostname = f"{btc_address}.bl.btcblack.it"
        ip_address = socket.gethostbyname(hostname)
        return ip_address == "127.0.0.2"
    except socket.gaierror:
        return False

def is_suspicious(addreses):
    for adress in addreses:
        if check_blacklist(adress):
            return True, adress
    return False, None

def process_transactions(transactions, csv_writer):
    for tx in transactions:
        inputs = {inp["prev_out"]["addr"]: inp["prev_out"]["value"] / 100000000 for inp in tx["inputs"] if "prev_out" in inp and "addr" in inp["prev_out"]}
        outputs = {out["addr"]: out["value"] / 100000000 for out in tx["out"] if "addr" in out}
        total_amount = sum(outputs.values())
        suspicious, address = is_suspicious(list(inputs.keys()) + list(outputs.keys()))
        if suspicious:
            print(f"\nОбнаружена транзакция: {tx['hash']}")
            print(f"Подозрительный адрес: {address}")
            print(f"Количество входов: {len(inputs)}, Количество выходов: {len(outputs)}, Total Amount: {total_amount} BTC")
        tx_details = {
            "Timestamp": (tx["time"]),
            "TotalAmount": total_amount,
            "TotalInputs": len(inputs),
            "TotalOutputs": len(outputs),
            "Inputs": json.dumps(inputs),
            "Outputs": json.dumps(outputs),
            "Suspicius": suspicious,
        }
        csv_writer.writerow(tx_details)

def fetch_transactions_and_write_csv(last_n_blocks=80, csv_filename="transactions.csv"):
    try:
        latest_block_hash = requests.get("https://blockchain.info/latestblock").json()["hash"]

        with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = ["Timestamp", "TotalAmount", "TotalInputs", "TotalOutputs", "Inputs", "Outputs", "Suspicius"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for _ in range(last_n_blocks):
                block_data = get_block_info(latest_block_hash)
                if block_data:
                    print(f"\n\n=== Анализ блока #{block_data['height']}, количество транзакций: {len(block_data['tx'])} ===\n\n")
                    process_transactions(block_data["tx"], writer)
                    latest_block_hash = block_data["prev_block"]
                else:
                    print("Ошибка получения данных о блоке.")
                    break
    except Exception as e:
        print(f"Ошибка: {e}")

fetch_transactions_and_write_csv()
