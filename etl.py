from abstract_etl import AbstractETL
import pandas as pd
import json
from main import usuario, host, senha, banco_de_dados

class ETL(AbstractETL):
  def __init__(self, source, target):
    self.source = source
    self.target = target
  
  def extract(self):
    with open(self.source, 'r') as arquivo:
        dados_arquivo = json.load(arquivo)
        self.extracted = dados_arquivo
  
  def transform(self):
    dicionario_datas = {}
    for dicionario in self.extracted:
      # print(dicionario)
      nome_tabela=dicionario['tipo']
      atributos = dicionario['atributos']
      df = pd.DataFrame(atributos)
      dicionario_datas[nome_tabela]=df
    self.transformed = dicionario_datas

# Iterar sobre a lista principal

  def load(self):
    engine = create_engine(f"mssql+pymssql://{usuario}:{senha}@{host}/{banco_de_dados}")

    Session = sessionmaker(bind=engine)
    session = Session()
    
    pass
