from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from dotenv import load_dotenv
import requests
import time
import os

load_dotenv()

#Configuração do banco SQL
DATABASE_URL = os.getenv("DATABASE_KEY")

#Criação da engine e sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

#Definição do modelo de dados:
class BitcoinDados(Base):
    __tablename__ = "bitcoin_dados"
    
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

#Criar a tabela se não existir
Base.metadata.create_all(engine)


def extract():
    
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    
    return response.json()

def transform(dados_json):
    
    valor = float(dados_json['data']['amount'])
    criptomoeda = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    
    dados_tratados = BitcoinDados(
        valor = valor,
        criptomoeda = criptomoeda,
        moeda = moeda,
        timestamp = datetime.now()
    )
    
    # dados_tratados = {
    #     "valor":valor,
    #     "criptomoeda":criptomoeda,
    #     "moeda":moeda,
    #     "timestamp": datetime.now().isoformat()
    # }
    
    return dados_tratados

def load(dados_tratados):
    
    with Session() as session:
        session.add(dados_tratados)
        session.commit()
    
        print("Dados foram salvos com sucesso!")

if __name__ == "__main__":
    
    while True:
        #Extração e tratamento dos dados
        dados_json = extract()
        dados_tratados = transform(dados_json)
        
        #Mostrar os dados tratados
        print("Dados Tratados:")
        
        #Salvar no PostgreSQL
        load(dados_tratados)
        
        #Pausa por 15 segundos
        print("Aguardando 15 segundos...")
        time.sleep(15)
    