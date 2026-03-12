# test_network_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import socket
import requests
import subprocess

def test_network_connectivity():
    """Тестирование сетевого подключения"""
    
    # check DNS
    try:
        ip = socket.gethostbyname('api.spacexdata.com')
        print(f"DNS resolution: api.spacexdata.com -> {ip}")
    except socket.gaierror as e:
        print(f"DNS resolution failed: {e}")
    
    # check curl
    try:
        result = subprocess.run(
            ['curl', '-I', 'https://api.spacexdata.com/v4/starlink'],
            capture_output=True,
            text=True,
            timeout=10
        )
        print(f"curl result: {result.stdout}")
    except Exception as e:
        print(f"curl failed: {e}")
    
    # 3. check requests 
    for timeout in [5, 10, 30]:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br', 
            'Connection': 'keep-alive'
}
        try:
            print(f"\ncheck with timeout {timeout}с...")
            response = requests.get(
                'https://api.spacexdata.com/v4/starlink',
                headers=headers,
                timeout=(10, 90),
                stream=True
            )
            print(f"status: {response.status_code}")
            print(f"heasders: {dict(response.headers)}")
            break
        except requests.exceptions.Timeout:
            print(f"timeout {timeout}с")
        except requests.exceptions.ConnectionError as e:
            print(f"Fail connection: {e}")
        except Exception as e:
            print(f"error: {e}")

with DAG(
    'test_network_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_network',
        python_callable=test_network_connectivity
    )