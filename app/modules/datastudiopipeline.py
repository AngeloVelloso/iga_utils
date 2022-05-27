#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gsheet
import igacloud
import utilitarios
import pandas as pd
import re


def pipeline_cobranca():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_cobranca()
    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(dados, 'cobranca.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1PWvPUsJEE56oS_sQ8nnbsi9mcMpECNyChbG5gOwAzXw', dados.drop(columns=['Curso']))

def pipeline_dividas():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_dividas()
    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(dados, 'dividas.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1zFAKnCJzzVqUb2CtgGQc21C1kQPoWZMTCVlY89mT_Qo', dados)

def pipeline_cobros():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_cobros()
    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(dados, 'cobros.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1nnrFpz7JwvXppq7kFXF3vT3Y91NWiGX6FLVwM0OcKgI', dados)

def pipeline_det_alunos_curso():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_det_alunos_curso()
    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(dados, 'det_alunos_curso.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('163DqyBt05H5WBOgxrBFcmOTkmnT_J1-D6VDZrfCLEcQ', dados)
    
    #Sumarização 1 - Gráfico Matriculas x Baixas x Alunos Ativos
    matriculas_sum = dados.groupby(['DataMatricula', 'Unidade', 'Curso']).agg({ 'Matricula': 'count' }
                    ).reset_index().rename(columns= {'DataMatricula': 'Data', 'Matricula': 'Matriculas'})
    baixas_sum = dados.groupby(['DataBaixa', 'Unidade', 'Curso']).agg({ 'Matricula': 'count' }
                    ).reset_index().rename(columns= {'DataBaixa': 'Data', 'Matricula': 'Baixas'})
    df_sum = matriculas_sum.merge(baixas_sum, how='left', on=['Data', 'Unidade', 'Curso']).fillna(0)
    df_sum['AlunosAtivos'] = df_sum['Matriculas'] - df_sum['Baixas']

    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(df_sum, 'matr_baixas_ativos.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1XZFc8p8LGTSAyDjhrh9Tc9Fh1HiQ0XHgtJ57zTpi3Ao',df_sum)

def pipeline_acad_frequencia():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_acad_frequencia()
    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(dados, 'acad_frequencia.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1itMP5kVOqVW5rH_PUii7JdsBGwjPcm6feuMdy2r_rvI', dados)

    #Sumarização 1 - Alunos x Última Presença Cozinha x Presencas e Ausências 
    ultima_freq_lancada = dados['DataAula'].max()
    filtro_sala = dados['SalaAula'].str.contains('Cozinha')
    filtro_presente = dados['Presente'].str.contains('Presente')
    ultima_presenca_sum = dados[filtro_sala & filtro_presente].groupby(['Unidade', 'CodigoAluno', 'NomeTurma']
                          ).agg({'DataAula': 'max'}).reset_index().rename(columns={'DataAula': 'UltimaPresenca'})
    ultima_presenca_sum['UltimaFreqLancada'] = ultima_freq_lancada

    presencas_totais = dados[filtro_sala].groupby(['Unidade', 'CodigoAluno', 'NomeTurma']
                        )['Presente'].value_counts().unstack().fillna(0)
    df_freq_sum = ultima_presenca_sum.merge(presencas_totais, how='outer', on=['Unidade', 'CodigoAluno', 'NomeTurma'])
    
    trinta_dias = ultima_freq_lancada - pd.Timedelta(days=30)
    filtro_data = dados['DataAula'] >= trinta_dias
    presencas_30_dias = dados[filtro_sala & filtro_data].groupby(['Unidade', 'CodigoAluno', 'NomeTurma']
                        )['Presente'].value_counts().unstack().fillna(0)
    presencas_30_dias.rename(columns={'Ausente': 'Ausente_30', 'Justificado': 'Jutif_30', 'Presente': 'Presente_30'}, inplace=True)
    df_freq_sum = df_freq_sum.merge(presencas_30_dias, how='outer', on=['Unidade', 'CodigoAluno', 'NomeTurma'])

    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(df_freq_sum, 'frequencia_sum.parquet')
    gsheet.grava_dados('1wbQN5j_vOx5Js3ndedwMD44LxcFhAXcKT1z6m4wy2qI',df_freq_sum)

def pipeline_resumo_alunos_turma():
    df_freq = utilitarios.recupera_dados_dir('frequencia_sum.parquet')

    df_cobr = utilitarios.recupera_dados_dir('cobranca.parquet')
    filtro_estado = df_cobr['Estado'] == 'confirmado'
    df_cobr_sum = df_cobr[filtro_estado].groupby(by=['Unidade', 'CodigoAluno']
                ).agg({'Valor': 'sum', 'CodigoCobranca': 'count', 'DataPagamento': 'max'}
                ).reset_index().rename(columns= {
                    'Valor': 'Valor_Pago',
                    'CodigoCobranca': 'Qt_Pagamentos',
                    'DataPagamento': 'Data_Ultimo_Pago'
                })

    df_divi = utilitarios.recupera_dados_dir('dividas.parquet')
    df_divi['ValorDivida']= df_divi['ValorDivida'].astype('float64', copy=False)
    df_divi_sum = df_divi.groupby(by=['Unidade', 'CodigoAluno']
                ).agg({'ValorDivida': 'sum', 'Parcela': 'count', 'DataVencimento': 'min'}
                ).reset_index().rename(columns= {
                    'ValorDivida': 'Valor_Vencido',
                    'Parcela': 'Qt_Parcelas_Vencidas',
                    'DataVencimento': 'Data_Primeiro_Vencimento'
                })

    df_esti = utilitarios.recupera_dados_dir('cobros.parquet')  
    filtro_vencfuturo = df_esti['DataVencimento'] > df_divi['DataVencimento'].max(skipna=True)
    df_esti['Saldo']= df_esti['Saldo'].astype('float64', copy=False)
    df_esti['Parcela']= df_esti['Parcela'].astype('float64', copy=False)    
    df_esti_sum = df_esti[filtro_vencfuturo].groupby(by=['Unidade', 'CodigoAluno']
                ).agg({'Saldo': ['sum', lambda x: pd.Series.mode(x)[0]], 
                'Parcela': ['count', 'max'], 'DataVencimento': 'min'}
                ).reset_index()
                
    df_esti_sum.columns = ['Unidade', 'CodigoAluno', 'Valor_a_Pagar', 'Parcela_Contrato',
                           'Qt_Parcelas_A_Vencer', 'Qt_Total_Parcelas', 'Data_Proximo_Vencimento']

    df_matr = utilitarios.recupera_dados_dir('det_alunos_curso.parquet') 
    df_nomes = df_matr.filter(['Unidade', 'CodigoAluno', 'Nome']).drop_duplicates()
    filtro_habilitado = df_matr['Estado'].str.contains('^hab', 
                                                       flags=re.IGNORECASE, 
                                                       regex=True)
    filtro_curso_longo = df_matr['Curso'].str.contains('(gastronomia|cozinheiro|confeitaria.pro)', 
                                                       flags=re.IGNORECASE, 
                                                       regex=True)
    df_ativos = df_matr[filtro_habilitado & filtro_curso_longo].groupby(['Unidade', 'CodigoAluno']
                ).agg({'Matricula': 'count'}
                ).reset_index().rename(columns= {
                    'Matricula': 'Contratos_ativos'
                })
    
    df_resumo = df_nomes.merge(df_ativos, how='outer', on=['Unidade', 'CodigoAluno'])
    df_resumo = df_resumo.merge(df_freq, how='outer', on=['Unidade', 'CodigoAluno'])
    df_resumo = df_resumo.merge(df_cobr_sum, how='outer', on=['Unidade', 'CodigoAluno'])
    df_resumo = df_resumo.merge(df_divi_sum, how='outer', on=['Unidade', 'CodigoAluno'])
    df_resumo = df_resumo.merge(df_esti_sum, how='outer', on=['Unidade', 'CodigoAluno'])
    df_resumo = df_resumo.astype({
            'Qt_Pagamentos': 'float64', 
            'Qt_Parcelas_Vencidas': 'float64',  
            'Qt_Parcelas_A_Vencer': 'float64' }, copy=False, errors='ignore')

    #Grava os dados em diretorio
    utilitarios.grava_dados_dir(df_resumo, 'resumo_alunos_turma.parquet')
    #Grava os dados no googlesheet
    gsheet.grava_dados('1bfnu1XVWlA08T5z_dqD_ggZ9CmpTdsSqYO4uIG--ytI', df_resumo)

if __name__ == '__main__':
    pipeline_cobranca()
    pipeline_dividas()
    pipeline_cobros()
    pipeline_det_alunos_curso()
    pipeline_acad_frequencia()
    pipeline_resumo_alunos_turma()