#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 18 14:44:56 2018

@author: menga
"""

from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

from config import CONNECTION_STRING
from datetime import datetime

from config import TAB_PLATAFORMA, TAB_DE_PARA_PLATA, TAB_TRANSPORTADORA, \
    TAB_DE_PARA_TRANS, TAB_PERC_ROTEIRIZACAO
#conn = psycopg2.connect(CONNECTION_STRING)
#cursor = conn.cursor()
#print("Connected!")

#@contextmanager
#def tag(name):
#    print("<%s>" % name)
#    yield 1
#    print("</%s>" % name)
#
#with tag("h1") as val:
#    print("foo")
#    print(val)

# Implementado seguindo Singleton Pattern
class SQLCommand:
    instance = None

    class __SQLCommand:
        def __init__(self):
            # pool define with 10 live connections
            self.connectionpool = SimpleConnectionPool(1,10,dsn=CONNECTION_STRING)
        
        @contextmanager
        def getcursor(self):
            con = self.connectionpool.getconn()
            try:
                con.autocommit = True
                yield con.cursor()
            finally:
                self.connectionpool.putconn(con)

        @contextmanager
        def getconnection(self, autocommit = True):
            con = self.connectionpool.getconn()
            try:
                con.autocommit = autocommit
                yield con
            finally:
                self.connectionpool.putconn(con)

    @staticmethod
    def convertDateToSQL(value):
        return datetime.strftime(value, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def select_one(table, where, values):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + table + " " + where, values)
            result = cursor.fetchone()
        return result

    @staticmethod
    def select(table, where, values):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + table + " " + where, values)
            result = cursor.fetchall()
        return result

    @staticmethod
    def delete(table, where, values):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("DELETE FROM " + table + " " + where, values)
            result = cursor.rowcount
        return result

    # https://nelsonslog.wordpress.com/2015/04/27/inserting-lots-of-data-into-a-remote-postgres-efficiently/
    # https://stackoverflow.com/questions/29461933/insert-python-dictionary-using-psycopg2
    @staticmethod
    def insert(table, rows):
        result = 0
        if len(rows) > 0:
            if type(rows) == type(dict()):
                rows = [rows]
            firstrow = rows[0]
            columns = firstrow.keys()
            values = ["%(" + column + ")s" for column in columns]
            insert_statement = 'insert into ' + table + ' ({}) values ({})'.format(', '.join(columns), ', '.join(values))
            with SQLCommand().getcursor() as cursor:
                cursor.executemany(insert_statement, rows)
                result = cursor.rowcount
        return result

    @staticmethod
    def load_dict(table, fieldKey, fieldValue, where, values):
        result = {}
        with SQLCommand().getcursor() as cursor:
            query = "SELECT " + fieldKey + ", " + fieldValue + " FROM " + table
            if where:
                query = query + " " + where
            cursor.execute(query, values)
            for row in cursor:
                key = row[0]
                if (type(row[0]) == type(str())):
                    key = row[0].upper()
                result[key] = row[1]
        return result
        
    @staticmethod
    def get_plataforma(id_plataforma):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_PLATAFORMA + " WHERE id_plataforma = %s", [id_plataforma])
            result = cursor.fetchone()
        return result

    @staticmethod
    def get_plataforma_por_nome(nome):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_PLATAFORMA + " WHERE LOWER(nome_plataforma) = %s", [nome.lower()])
            result = cursor.fetchone()
        return result

    @staticmethod
    def insert_plataforma(id_plataforma, nome):
        result = 0
        with SQLCommand().getcursor() as cursor:
            cursor.execute("INSERT INTO " + TAB_PLATAFORMA + " VALUES (%s, %s)", [id_plataforma, nome])
            result = cursor.rowcount
        return result

    @staticmethod
    def insert_de_para_plata(id_plataforma, nome_de):
        result = 0
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_DE_PARA_PLATA + " WHERE nome_de = %s", [nome_de])
            if not cursor.fetchone():
                cursor.execute("INSERT INTO " + TAB_DE_PARA_PLATA + " VALUES (%s, %s)", [nome_de, id_plataforma])
                result = cursor.rowcount
        return result

    @staticmethod
    def get_transportadora(id_transportadora):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_TRANSPORTADORA + " WHERE id_transportadora = %s", [id_transportadora])
            result = cursor.fetchone()
        return result

    @staticmethod
    def insert_transportadora(id_transportadora, nome):
        result = 0
        with SQLCommand().getcursor() as cursor:
            cursor.execute("INSERT INTO " + TAB_TRANSPORTADORA + " VALUES (%s, %s)", [id_transportadora, nome])
            result = cursor.rowcount
        return result

    @staticmethod
    def insert_de_para_trans(id_transportadora, nome_de):
        result = 0
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_DE_PARA_TRANS + " WHERE nome_de = %s", [nome_de])
            if not cursor.fetchone():
                cursor.execute("INSERT INTO " + TAB_DE_PARA_TRANS + " VALUES (%s, %s)", [nome_de, id_transportadora])
                result = cursor.rowcount
        return result

    @staticmethod
    def get_roteirizacao(data_base, id_plataforma):
        result = None
        with SQLCommand().getcursor() as cursor:
            cursor.execute("SELECT * FROM " + TAB_PERC_ROTEIRIZACAO + " WHERE id_plataforma = %s" + \
                           " AND data_base = %s", [id_plataforma, data_base])
            result = cursor.fetchone()
        return result

    @staticmethod
    def ins_or_upd_roteirizacao(data_base, id_plataforma, perc_cump, perc_ader):
        result = 0
        data_base = SQLCommand.convertDateToSQL(data_base)
        db_rot = SQLCommand.get_roteirizacao(data_base, id_plataforma)
        if db_rot:
            with SQLCommand().getcursor() as cursor:
                cursor.execute("UPDATE " + TAB_PERC_ROTEIRIZACAO + " SET perc_cumprimento = %s, " \
                               "perc_aderencia = %s WHERE data_base = %s "
                               "AND id_plataforma = %s", [perc_cump, perc_ader, data_base, id_plataforma])
                result = cursor.rowcount
        else:
            with SQLCommand().getcursor() as cursor:
                cursor.execute("INSERT INTO " + TAB_PERC_ROTEIRIZACAO + " VALUES (%s, %s, %s, %s)", [data_base, id_plataforma, perc_cump, perc_ader])
                result = cursor.rowcount
        return result


    def __init__(self):
        if not SQLCommand.instance:
            SQLCommand.instance = SQLCommand.__SQLCommand()

    def __getattr__(self, name):
        return getattr(self.instance, name)
# Exemplo de uso:
#with SQLCommand().getcursor() as cursor:
#    cursor.execute("SELECT current_timestamp")
#    print(cursor.fetchall())

SQLCommand.get_plataforma(1)
