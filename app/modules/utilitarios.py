#!/usr/bin/env python
# -*- coding: cp1252 -*-

import os
import glob
import inspect
from pandas import read_parquet

def diretorio_dados():
    diretorio = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
    nome_dir = glob.glob(diretorio + '/data/')
    return str(nome_dir[0])

def grava_dados_dir(df, nome_arquivo):
    nome_arquivo = diretorio_dados() + nome_arquivo
    df.to_parquet(nome_arquivo)
    return None

def recupera_dados_dir(nome_arquivo):
    nome_arquivo = diretorio_dados() + nome_arquivo
    df = read_parquet(nome_arquivo)
    return df