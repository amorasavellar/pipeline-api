from tinydb import TinyDB
from datetime import datetime
import requests
import time

def extract():
    
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    
    return response.json()

def transform(dados_json):
    
    valor = dados_json['data']['amount']
    criptomoeda = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    
    dados_tratados = {
        "valor":valor,
        "criptomoeda":criptomoeda,
        "moeda":moeda,
        "timestamp": datetime.now().isoformat()
    }
    
    return dados_tratados

def load(dados_tratados):
    db = TinyDB('db.json')
    db.insert(dados_tratados)
    
    print("Dados foram salvos com sucesso!")

if __name__ == "__main__":
    
    while True:
        dados_json = extract()
        dados_tratados = transform(dados_json)
        load(dados_tratados)
        time.sleep(5)
    