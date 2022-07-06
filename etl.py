# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 20:06:54 2021

@author: junior
"""

import pandas as pd

class Covid:
  def __init__(self):
    self.srag_cov = pd.read_csv('srag_cov.csv', sep = ';')
    self.srag_cov_columns = pd.read_csv('srag_cov_columns.csv', sep = ';')

  def etl(self):
    srag_cov_reg_columns = list(self.srag_cov_columns['Colunas REG'])
    srag_cov_class_columns = list(self.srag_cov_columns['Colunas CLASS'])
    srag_cov_class_columns = [x for x in srag_cov_class_columns if pd.isnull(x) == False]
    columns_drop_class = list(self.srag_cov.columns)
    columns_drop_reg = list(self.srag_cov.columns)
    
    def tratamento(srag_cov):
      srag_cov = srag_cov.loc[(srag_cov['ID_PAIS'] == 'BRASIL')]
      srag_cov['CLASSI_FIN'] = srag_cov['CLASSI_FIN'].replace(to_replace = [2, 3, 4], value = [1, 1, 1])
      srag_cov['CS_SEXO'] = srag_cov['CS_SEXO'].replace(to_replace = ['M', 'F', 'I'], value = ['1', '2', '9'])
      srag_cov['CLASSI_FIN'].fillna(1, inplace = True)
      srag_cov.fillna(9, inplace = True)
      
      
      return srag_cov
    
    
    def remov_columns(dataset, colunas, drop_):
      for i in colunas:
        if i in drop_:
          drop_.remove(i)
      
      for i in drop_:
        dataset.drop(i, axis = 1, inplace = True)
      
      return dataset
    
    def split_join(x):
      if str(x).find(','):
          a = str(x).split(',')
          return '.'.join(a)
      else:
        return x
    
    self.srag_cov = tratamento(self.srag_cov)
    srag_cov_reg = remov_columns(self.srag_cov.copy(), srag_cov_reg_columns, columns_drop_reg)
    srag_cov_class = remov_columns(self.srag_cov.copy(), srag_cov_class_columns, columns_drop_class)
    
    
    srag_cov_reg = srag_cov_reg[srag_cov_reg_columns]
    srag_cov_class = srag_cov_class[srag_cov_class_columns]
    
    srag_cov_class['OBES_IMC'] = srag_cov_class['OBES_IMC'].apply(split_join).astype(float) 
    
    srag_cov_reg.to_csv('srag_cov_reg.csv', index = False)
    srag_cov_class.to_csv('srag_cov_class.csv', index = False)
    

if __name__ == '__main__':
  execut_class = Covid()
  execut_etl = execut_class.etl()


