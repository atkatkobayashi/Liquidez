from icecream import ic
import pandas as pd
import sys
import mysql.connector
import datetime
import os
from os import path

sys.path.insert(0, r'R:\Quant')
#sys.path.insert(0, r'C:\Users\ATK\OneDrive\Rafter')

import lib_rafter
import lib_liquidez_passivo

if __name__ == "__main__":
    # Abre conexao SQL
    conn = lib_rafter.open_sql_conn()
    cursor = conn.cursor(buffered=True)

    data_base = input("Data Base (aaaa-mm-dd): ")
    #data_base = '2021-08-17'
    data_base = datetime.datetime.strptime(data_base, '%Y-%m-%d').date()

    list_fundo = ['13151', '13152', '16778', '19019', 'PAVA', '14586', '16029', '16749']
    #list_fundo = ['13152']

    for i in range(0, len(list_fundo)):
        fundo_id = list_fundo[i]

        print(fundo_id)
        
        lib_liquidez_passivo.ResgateMedioFundo(cursor, data_base, fundo_id)
        lib_liquidez_passivo.MediaPercentualResgate(cursor, data_base, fundo_id)
        
    # Fecha conexao SQL
    conn.commit()
    lib_rafter.close_mysql_conn(conn)

    print("")
    input("Importacao Finalizada...")