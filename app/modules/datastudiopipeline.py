#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gsheet
import igacloud


def pipeline_cobranca():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_cobranca()
    #Grava os dados no googlesheet
    gsheet.grava_dados('1PWvPUsJEE56oS_sQ8nnbsi9mcMpECNyChbG5gOwAzXw', dados.drop(columns=['Curso']))

def pipeline_dividas():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_dividas()
    #Grava os dados no googlesheet
    gsheet.grava_dados('1zFAKnCJzzVqUb2CtgGQc21C1kQPoWZMTCVlY89mT_Qo', dados)

def pipeline_cobros():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_cobros()
    #Grava os dados no googlesheet
    gsheet.grava_dados('1nnrFpz7JwvXppq7kFXF3vT3Y91NWiGX6FLVwM0OcKgI', dados)

def pipeline_det_alunos_curso():
    #Faz login no IGA Cloud
    iga = igacloud.IgaCloud()
    #Captura dados no IGA Cloud
    dados = iga.faz_rel_det_alunos_curso()
    #Grava os dados no googlesheet
    gsheet.grava_dados('163DqyBt05H5WBOgxrBFcmOTkmnT_J1-D6VDZrfCLEcQ', dados)

if __name__ == '__main__':
    pipeline_cobranca()
    pipeline_dividas()
    pipeline_cobros()
    pipeline_det_alunos_curso()