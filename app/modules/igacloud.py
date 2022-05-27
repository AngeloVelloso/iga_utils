#!/usr/bin/env python
# -*- coding: cp1252 -*-

import os
import glob
import inspect
import re
import pandas as pd
from json import loads, load
from mechanize import Browser, Request

class IgaCloud:

    def __init__(self): 
        self.br = Browser()
        url = 'https://sistema.igacloud.net/login/validaLogin'
        usuario, senha = self.obtem_senha()
        data= { 'usuario': usuario, 
                'pass': senha }

        rqst = Request(url=url, data=data, method='POST')
        self.br.open(rqst)
        return None

    def obtem_senha(self):
        diretorio = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
        nome_arquivo = glob.glob(diretorio + '/static/*.senha')
        json_data = load(open(nome_arquivo[0], 'r'))    
        return json_data['usuario'], json_data['pass']

    def definir_filial_iga_cloud(self, nav, codigo_filial):  
        url = 'https://sistema.igacloud.net/usuarios/setFilial'
        data = { 'filial': codigo_filial }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)
        return r.code

# Administra��o -> Cobran�a
    def get_adm_cobranca(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/cobros/listar'
        data= { 'iSortCol_0': 0, 
                'sSortDir_0': 'DESC', 
                'iDisplayLength': -1,
                'iDisplayStart': 0,
                'fecha_desde_t': '01/04/2000',
                'fecha_hasta_t': '01/04/2030',
                'selectEstado': -1,
                'medio_pago': -1,
                'cod_autorizacion': -1,
                'caja': -1,
                'saldo': -1               }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())
        colunas = ['CodigoCobranca', 'Nome', 'Documento', 'Valor_R$', 'Imputado_R$', 'Saldo_R$', 
                    'FormaPagamento', 'Conta', 'DataPagamento', 'Periodo', 
                    'DataVencimento', 'Estado', 'Valor', 'Imputado', 'Saldo']

        df_data = pd.DataFrame(json_data['aaData'], columns=colunas)
        df_data['DataPagamento'] = pd.to_datetime(df_data['DataPagamento'], format='%d/%m/%Y')
        df_data['DataVencimento'] = pd.to_datetime(df_data['DataVencimento'], format='%d/%m/%Y', errors='coerce')
        df_data['Conta'] = df_data['Conta'].map(lambda x: 'Recebimentos - PagSeguro' if x is None else x)
        df_data['Valor'] = df_data['Valor'].astype('float64')

        # Reordenar colunas
        colunas = ['CodigoCobranca', 'Nome', 'Documento', 'Valor', 'Imputado', 'Saldo', 
                    'FormaPagamento', 'Conta', 'DataPagamento', 'Periodo', 
                    'DataVencimento', 'Estado']
        df_data = df_data[colunas]

        # Inclui informação da unidade e chave para merge
        df_data['Unidade'] = flag_unidade
        df_data['chave_unica'] = df_data['Unidade'] + '_' + df_data['CodigoCobranca']

        return df_data

# Relat�rios -> Administra��o -> Cobran�a
    def get_rel_cobranca(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/reportes/getReporte'
        data= { 'iPaginationLength': -1, 
                'iCurrentPage': 1, 
                'report_name': 'cobros',
                'iFieldView[]': 'cod_alumno',
                'iFieldView[]': 'cod_matricula',
                'iFieldView[]': 'nombre_alumno',
                'iFieldView[]': 'codigo',
                'iFieldView[]': 'concepto',
                'iFieldView[]': 'curso_nombre',
                'iFieldView[]': 'nrocuota',
                'iFieldView[]': 'importe',
                'iFieldView[]': 'medio',
                'iFieldView[]': 'nombre_usuario',
                'iFieldView[]': 'fecha',
                'iFieldView[]': 'estado',
                'iFieldView[]': 'documento',
                'filters[0][field]': 'fecha',
                'filters[0][field]': 'entre',
                'filters[0][value1]': '01/04/2000',
                'filters[0][value2]': '01/04/2030',
                'filters[0][dataType]': 'date' }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())

        df_data = pd.DataFrame(json_data['aaData'])
        df_data.columns = ['CodigoAluno', 'Matricula', 'Nome', 'CodigoCobranca',
                    'Email', 'Conceito', 'Curso', 'Parcela', 'Valor', 'FormaPagamento', 
                    'Usuario', 'Data', 'Estado', 'Documento', 'Endereco', 'Bairro',
                    'CEP', 'Cidade']

        df_data['Data'] = pd.to_datetime(df_data['Data'], format='%d/%m/%Y', errors='coerce')

        # Inclui informação da unidade e chave para merge
        df_data['Unidade'] = flag_unidade
        df_data['chave_unica'] = df_data['Unidade'] + '_' + df_data['CodigoCobranca']

        return df_data

    def juntar_cobranca(self, adm_file,rel_file):
        # Sele��o das colunas do 'Relat�rios'>Administra��o>Cobran�a e elimina��o de duplicidades
        # Esse relat�rio � menos preciso, todavia possui informações que faltam no 'Administração>Cobrança', 
        #      com isso, selecionamos os dados e complementamos as informações naquele relatório.
        rel_filtro = rel_file.filter(items=['chave_unica', 'CodigoAluno', 
                                            'Matricula', 'Usuario', 'Data', 'Curso']
                                    ).drop_duplicates(subset='chave_unica', 
                                                        keep='first')

        # Junção de ambos relatórios
        merge_da_cobranca = adm_file.merge(rel_filtro, 
                                                    how='left', 
                                                    on=['chave_unica'], 
                                                    suffixes=['_adm', '_rel'], 
                                                    indicator=True)
        
        # Preparação de Campos e Tratamento de Dimensões
        # .. Alteração nome de colunas de data para ficar mais claro do que se trata
        merge_da_cobranca.rename(columns={ 'DataPagamento': 'DataRegistro', 'Data': 'DataPagamento'}, inplace=True)
        # .. Extração Ano, Mês e Dia para melhorar filtro
        merge_da_cobranca['Ano'] = merge_da_cobranca['DataPagamento'].map(lambda x: str(x.year))
        merge_da_cobranca['M�s'] = merge_da_cobranca['DataPagamento'].map(lambda x: str(x.month))
        merge_da_cobranca['Dia'] = merge_da_cobranca['DataPagamento'].map(lambda x: str(x.day))
        # .. Deleta coluna chave_unica
        del(merge_da_cobranca['chave_unica'])

        return merge_da_cobranca  

    def faz_rel_cobranca(self):  
        adm_cob = self.get_adm_cobranca(491, 'Porto Velho')
        rel_cob = self.get_rel_cobranca(491, 'Porto Velho')
         
        return self.juntar_cobranca(adm_cob,rel_cob)

# Relatórios -> Administração -> Detalhe das Dívidas do Aluno
    def get_rel_dividas(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/reportes/getReporte'
        data= { 'iPaginationLength': -1, 
                'iCurrentPage': 1, 
                'report_name': 'ctacte_pendientes',
                'iFieldView[]': 'codigo',
                'iFieldView[]': 'nombre_alumno',
                'iFieldView[]': 'email',
                'iFieldView[]': 'telefono',
                'iFieldView[]': 'fecha_vencimiento',
                'iFieldView[]': 'nombre_concepto',
                'iFieldView[]': 'nrocuota',
                'iFieldView[]': 'saldo',
                'iFieldView[]': 'mora',
                'iFieldView[]': 'importe_total',
                'iFieldView[]': 'deuda_alumno',
                'iFieldView[]': 'nombre_curso',
                'iFieldView[]': 'camision', }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())

        df_data = pd.DataFrame(json_data['aaData'])
        df_data.columns = ['CodigoAluno', 'Nome', 'Email', 'Telefones',
                    'DataVencimento', 'Conceito', 'Parcela', 'ValorDivida', 'Juro', 
                    'TotalDivida', 'TipoDivida', 'Curso', 'Turma']

        df_data['DataVencimento'] = pd.to_datetime(df_data['DataVencimento'], format='%d/%m/%Y', errors='coerce')

        # Inclui informação da unidade e chave
        df_data['Unidade'] = flag_unidade

        return df_data

    def faz_rel_dividas(self):  
        rel_div = self.get_rel_dividas(491, 'Porto Velho')
         
        return rel_div[rel_div['Conceito'] != 'Juro']

# Reportes -> Cobros Estimados
    def get_rel_cobros(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/reportes/getReporte'
        data= { 'iPaginationLength': -1, 
                'iCurrentPage': 1, 
                'report_name': 'cobros_estimados',
                'iFieldView[]': 'codigo',
                'iFieldView[]': 'nombre_alumno',
                'iFieldView[]': 'telefono',
                'iFieldView[]': 'fecha_vencimiento',
                'iFieldView[]': 'nombre_concepto',
                'iFieldView[]': 'nrocuota',
                'iFieldView[]': 'saldo',
                'iFieldView[]': 'mora',
                'iFieldView[]': 'importe_total',
                'iFieldView[]': 'deuda_alumno',
                'iFieldView[]': 'nombre_curso' }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())

        df_data = pd.DataFrame(json_data['aaData'])
        df_data.columns = ['CodigoAluno', 'Nome', 'Telefones', 'DataVencimento', 
                            'Conceito', 'Parcela', 'Saldo', 'Juro', 'Valor',
                            'TipoDivida', 'Curso', 'Turma'] 

        df_data['DataVencimento'] = pd.to_datetime(df_data['DataVencimento'], format='%d/%m/%Y', errors='coerce')

        # Inclui informação da unidade e chave
        df_data['Unidade'] = flag_unidade

        return df_data

    def faz_rel_cobros(self):  
        rel_cobros = self.get_rel_cobros(491, 'Porto Velho')
         
        return rel_cobros[rel_cobros['Conceito'] != 'Juro']

# Relatórios -> Acadêmicos -> Detalhes de Alunos por Curso
    def get_rel_det_alunos_curso(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/reportes/getReporte'
        data= { 	'iPaginationLength': '-1',
                    'iCurrentPage': '1',
                    'report_name': 'inscripciones',
                    'iSortDir': '',
                    'iSortCol': '',
                    'iFieldView[]': [
                        'cod_alumno',
                        'codigo',
                        'alumno_nombre',
                        'curso_nombre',
                        'comision',
                        'ciclo',
                        'fecha_matricula',
                        'estado',
                        'telefono_alumno',
                        'fecha_baja'
                    ] }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())

        df_data = pd.DataFrame(json_data['aaData'])
        df_data.columns = ['CodigoAluno', 'Matricula', 'Nome', 'Curso', 
                            'Turma', 'Ciclo', 'DataMatricula', 'Estado',
                            'Telefone', 'DataBaixa' ] 

        df_data['DataMatricula'] = pd.to_datetime(df_data['DataMatricula'], format='%d/%m/%Y', errors='coerce')
        df_data['DataBaixa'] = pd.to_datetime(df_data['DataBaixa'], format='%d/%m/%Y', errors='coerce')

        # Inclui informação da unidade
        df_data['Unidade'] = flag_unidade

        return df_data

    def faz_rel_det_alunos_curso(self):  
        rel_det_alunos_curso = self.get_rel_det_alunos_curso(491, 'Porto Velho')
         
        return rel_det_alunos_curso

# Relatórios -> Acadêmicos -> Acad. Frequências
    def get_rel_frequencias(self, codigo_filial, flag_unidade):
        nav = self.br
        self.definir_filial_iga_cloud(nav, codigo_filial)

        url = 'https://sistema.igacloud.net/reportes/getReporte'
        data= { 
                "iPaginationLength": "-1",
                "iCurrentPage": "1",
                "report_name": "asistencia",
                "iSortDir": "",
                "iSortCol": "",
                "iFieldView[]": [
                    "cod_alumno",
                    "alumno_nombre",
                    "alumno_documento",
                    "alumno_telefono",
                    "materia_nombre",
                    "cod_comision",
                    "comision_nombre",
                    "ciclo_comision",
                    "dia_cursado",
                    "horas",
                    "salon",
                    "profesor_nombre",
                    "asistencia"
                ] }
        rqst = Request(url=url, data=data, method='POST')
        r = nav.open(rqst)

        json_data = loads(r.read())

        df_data = pd.DataFrame(json_data['aaData'])
        df_data.columns = ['CodigoAluno', 'Nome', 'Documento', 'Telefone', 
                           'Disciplina', 'CodigoTurma', 'CodigoCiclo',
                           'NomeTurma', 'DataAula', 'HorarioAula', 
                           'SalaAula', 'Professor', 'Presente'] 

        df_data['DataAula'] = pd.to_datetime(df_data['DataAula'], format='%d/%m/%Y', errors='coerce')

        # Define o turno a partir da hora inicial
        def info_turno(x):
            hora_inicio = int(re.findall('^\d{2}',x)[0])
            return 'Manha' if hora_inicio in range(0,12) else 'Tarde' if hora_inicio in range(12,18) else 'Noite'
        df_data['Turno'] = df_data['HorarioAula'].apply(info_turno)

        # Inclui informação da unidade
        df_data['Unidade'] = flag_unidade

        return df_data

    def faz_rel_acad_frequencia(self):  
        rel_acad_frequencia = self.get_rel_frequencias(491, 'Porto Velho')
         
        return rel_acad_frequencia

if __name__ == '__main__':
    pass