from abstract_etl import AbstractETL
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import json
from classes import *

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
    engine = create_engine(self.target)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    df = self.transformed['UNIDADE_PRODUCAO']
    lista_unidadePd = []
    for indice, linha in df.iterrows():
      unidade_pd = Unidade_Producao(numero=linha['numero'], peca_hora_nominal=linha['peca_hora_nominal'])
      lista_unidadePd.append(unidade_pd)
    print(lista_unidadePd)
    
    
    
    session.add_all(lista_unidadePd)
    session.commit()
      
      
    

