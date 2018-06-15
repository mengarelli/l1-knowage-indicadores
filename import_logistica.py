#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 18 14:27:44 2018

@author: menga
"""

import xlrd
import log_geral
from sql_command import SQLCommand
import datetime
from config import TAB_META_GERAL, TAB_META_PLATAFORMA, TAB_LOGISTICA_INDICADOR, \
    TAB_PLATAFORMA

TEXTO_CUMPRIMENTO = "% Cumprimento"
TEXTO_ADERENCIA = "% Aderência"

def importar_plataformas(plan):
    log_geral.log("Importando plataformas...")
    sheet = plan.sheet_by_name("Plataforma")
    conta = 0
    # Primeira linha é o cabeçalho
    for r in range(1, sheet.nrows):
        cod = sheet.cell(r, 0).value
        de_para = sheet.cell(r, 1).value
        plataforma = sheet.cell(r, 2).value
        db_plataforma = SQLCommand.get_plataforma(cod)
        if not db_plataforma:
            conta = conta + SQLCommand.insert_plataforma(cod, plataforma)
        conta = conta + SQLCommand.insert_de_para_plata(cod, de_para)
    log_geral.log("Linhas inseridas: {}".format(conta))

def importar_transportadoras(plan):
    log_geral.log("Importando transportadoras...")
    sheet = plan.sheet_by_name("Transportadora")
    conta = 0
    # Primeira linha é o cabeçalho
    for r in range(1, sheet.nrows):
        cod = sheet.cell(r, 0).value
        de_para = sheet.cell(r, 1).value
        plataforma = sheet.cell(r, 2).value
        db_plataforma = SQLCommand.get_transportadora(cod)
        if not db_plataforma:
            conta = conta + SQLCommand.insert_transportadora(cod, plataforma)
        conta = conta + SQLCommand.insert_de_para_trans(cod, de_para)
    log_geral.log("Linhas inseridas: {}".format(conta))

def obter_linha_por_valor(valor, coluna, sheet, de=0, ate=None):
    valor = valor.lower()
    if not ate:
        ate = sheet.nrows
    for r in range(de, ate):
        if str(sheet.cell(r, coluna).value).lower() == valor:
            return r
    return None

def importar_roteirizacao(plan):
    log_geral.log("Importando roteirização...")
    sheet = plan.sheet_by_name("Roteirização")
    conta = 0
    # % Cumprimento
    row_cumprimento = obter_linha_por_valor(TEXTO_CUMPRIMENTO, 0, sheet)
    if not row_cumprimento:
        raise Exception("Valor não encontrado na planilha: {}".format(TEXTO_CUMPRIMENTO))
    # % Aderência
    row_aderencia = obter_linha_por_valor(TEXTO_ADERENCIA, 0, sheet)
    if not row_aderencia:
        raise Exception("Valor não encontrado na planilha: {}".format(TEXTO_ADERENCIA))
    
    # Primeira linha é o cabeçalho
    for r in range(row_cumprimento + 1, row_aderencia - 1):
        plataforma = sheet.cell(r, 0).value
        if plataforma:
            db_plat = SQLCommand.get_plataforma_por_nome(plataforma)
            if not db_plat:
                raise Exception("Plataforma não encontrada: '{}'".format(plataforma))
            id_plataforma = db_plat[0]
            row = obter_linha_por_valor(plataforma, 0, sheet, de=row_aderencia)
            if not row:
                raise Exception("Aderência não encontrada para plataforma: '{}'".format(plataforma))
            col = 1
            data_base = sheet.cell(0, col).value
            data_base = datetime.datetime(*xlrd.xldate_as_tuple(data_base, plan.datemode))
            while (data_base):
                perc_cump = sheet.cell(r, col).value
                perc_cump = float(perc_cump) if perc_cump else 0
                perc_ader = sheet.cell(row, col).value
                perc_ader = float(perc_ader) if perc_ader else 0
#                print(type(data_base), data_base, plataforma, id_plataforma, perc_cump, perc_ader)
                conta = conta + SQLCommand.ins_or_upd_roteirizacao(data_base, id_plataforma, perc_cump, perc_ader)
                col = col + 1
                if (col < sheet.ncols):
                    data_base = sheet.cell(0, col).value
                    data_base = datetime.datetime(*xlrd.xldate_as_tuple(data_base, plan.datemode))
                else:
                    data_base = None

def importar_meta_geral(plan):
    log_geral.log("Importando metas gerais...")
    sheet = plan.sheet_by_name("Meta Geral")
    conta = 0
    to_insert = []
    # Primeira linha é o cabeçalho
    for r in range(1, sheet.nrows):
        values = {}
        values["data_base"] = datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r, 0).value, plan.datemode))
        values["perc_cumprimento"] = sheet.cell(r, 1).value
        values["perc_aderencia"] = sheet.cell(r, 2).value
        check_exist = SQLCommand.select_one(TAB_META_GERAL, "WHERE data_base = %s", [values["data_base"]])
        if not check_exist:
            to_insert.append(values)
    if (to_insert):
        conta = conta + SQLCommand.insert(TAB_META_GERAL, to_insert)
    log_geral.log("Linhas inseridas: {}".format(conta))

def importar_meta_plataforma(plan):
    log_geral.log("Importando metas para plataformas...")
    sheet = plan.sheet_by_name("Metas Plataforma")
    conta = 0
    to_insert = []
    # Primeira linha é o cabeçalho
    for r in range(1, sheet.nrows):
        values = {}
        values["data_base"] = datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r, 0).value, plan.datemode))
        db_plat = SQLCommand.get_plataforma_por_nome(sheet.cell(r, 1).value)
        if (db_plat):
            values["id_plataforma"] = db_plat[0]
            values["real_litro"] = sheet.cell(r, 2).value or 0
            values["volume"] = sheet.cell(r, 3).value or 0
            values["km"] = sheet.cell(r, 4).value or 0
            values["custo_reais"] = sheet.cell(r, 5).value or 0
            values["num_veiculos"] = sheet.cell(r, 6).value or 0
            values["volume_km"] = sheet.cell(r, 7).value or 0
            values["volume_caminhao"] = sheet.cell(r, 8).value or 0
            check_exist = SQLCommand.select_one(TAB_META_PLATAFORMA, \
                "WHERE data_base = %s AND id_plataforma = %s", \
                [values["data_base"], values["id_plataforma"]])
            if not check_exist:
                to_insert.append(values)
        else:
            raise Exception("Plataforma não encontrada: '{}'".format(sheet.cell(r, 1).value))
            
    if (to_insert):
        conta = conta + SQLCommand.insert(TAB_META_PLATAFORMA, to_insert)
    log_geral.log("Linhas inseridas: {}".format(conta))


def importar_indicadores(plan):
    log_geral.log("Importando indicadores de logística...")
    sheet = plan.sheet_by_name("Indicadores")
    conta = 0
    to_insert = []
    min_base = None
    max_base = None
    # Primeira linha é o cabeçalho
    print()
    plataformas = SQLCommand.load_dict(TAB_PLATAFORMA, "nome_plataforma", "id_plataforma", None, None)
    for r in range(1, sheet.nrows):
        if (r % 100 == 0):
            print('.', end='', flush=True)
        values = {}
        data_base = datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r, 0).value, plan.datemode))
        values["data_base"] = SQLCommand.convertDateToSQL(data_base)
        min_base = data_base if (not min_base) or (min_base > data_base) else min_base
        max_base = data_base if (not max_base) or (max_base < data_base) else max_base
#        db_plat = SQLCommand.get_plataforma_por_nome(sheet.cell(r, 3).value)
#        if (db_plat):
#            values["id_plataforma"] = db_plat[0]
        db_plat = plataformas[sheet.cell(r, 3).value.upper()]
        if (db_plat):
            values["id_plataforma"] = db_plat
            values["cod_pagamento"] = sheet.cell(r, 1).value or 0
            values["local_entrega"] = sheet.cell(r, 2).value
            values["custo_reais"] = sheet.cell(r, 4).value or 0
            values["volume"] = sheet.cell(r, 5).value or 0
            values["volume_dia"] = sheet.cell(r, 6).value or 0
            values["km"] = sheet.cell(r, 7).value or 0
            values["num_veiculos"] = sheet.cell(r, 8).value or 0
            to_insert.append(values)
        else:
            raise Exception("Plataforma não encontrada: '{}'".format(sheet.cell(r, 1).value))
    apagar = SQLCommand.delete(TAB_LOGISTICA_INDICADOR, \
        "WHERE data_base >= %s AND data_base <= %s", \
        [min_base, max_base])
    log_geral.log("Linhas apagadas: {}".format(apagar))
    
    if (to_insert):
        conta = conta + SQLCommand.insert(TAB_LOGISTICA_INDICADOR, to_insert)
    log_geral.log("Linhas inseridas: {}".format(conta))


def importar(arquivo_origem):
    log_geral.log("Abrindo arquivo origem:", arquivo_origem)
    plan = xlrd.open_workbook(arquivo_origem)
    importar_plataformas(plan)
    importar_transportadoras(plan)
    importar_roteirizacao(plan)
    importar_meta_geral(plan)
    importar_meta_plataforma(plan)
    importar_indicadores(plan)


EXCEL_FILE = "/Users/menga/volumes/onedrive/BI/projetos/Itambe/dados_importacao.xlsx"
importar(EXCEL_FILE)
log_geral.close()
