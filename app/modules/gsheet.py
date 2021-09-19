#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import inspect
import gspread as gs
from gspread_dataframe import set_with_dataframe

def obtem_chave_api():
    diretorio = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
    nome_arquivo = glob.glob(diretorio + '/static/chave-api.json')
    return nome_arquivo[0]

def grava_dados(key, df):
    gc = gs.service_account(filename=obtem_chave_api())
    sh = gc.open_by_key(key)
    worksheet = sh.get_worksheet(0) 

    # Apagar dados da tabela antes de gravar 
    # Este procedimento é necessário porque alguns retornos ficam menores do que o volume de dados que existia antes
    nome_tabela = worksheet.title
    sh.values_clear(nome_tabela)

    set_with_dataframe(worksheet, df)
    return None