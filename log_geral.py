#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 14:58:28 2018

@author: menga
"""
from config import EXTENSAO_LOG

global log_file

try:
    log_file
except:
    arquivo_log = "log-geral" + EXTENSAO_LOG
    print("Criando arquivo de log:", arquivo_log)
    log_file = open(arquivo_log, "w")

def log(*ss):
    global log_file
    for s in ss:
        print(s)
        log_file.write(s)
    log_file.write("\n")

def close():
    global log_file
    log_file.flush()
    log_file.close()
