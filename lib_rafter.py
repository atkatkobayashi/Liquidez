#from icecream import ic
import pandas as pd
import sys
import math
import mysql.connector
import datetime
from dateutil.relativedelta import relativedelta
import time
from scipy import interpolate

## FUNCOES GERAIS
def open_sql_conn():
    return mysql.connector.connect(host='rafterinvestimentos.cifxhn60hqcu.sa-east-1.rds.amazonaws.com',database='rafter_investimentos',user='rafter',password='rafter!2024!')
    
def close_mysql_conn(conn):
    conn.close()

def load_fundo_cota(cursor, fundo_id, data_base):
    cursor.execute("SELECT valor_cota FROM tbl_rafter_nav WHERE fundo_id = %s AND data_base = %s;", (fundo_id, data_base))
    return cursor.fetchone()[0]

def load_fundo_pl(cursor, fundo_id, data_base):
    cursor.execute("SELECT pl FROM tbl_rafter_nav WHERE fundo_id = %s AND data_base = %s;", (fundo_id, data_base))
    
    try:
        pl_fundo = cursor.fetchone()[0]
    except:
        print("PL do fundo (" + str(fundo_id) + ") nao atualizado no Banco de Dados para esta data.")
        input("")
        sys.exit()

    return pl_fundo

def load_fundo_historico_pl_cota(cursor, fundo_id):
    cursor.execute("SELECT data_base, valor_cota, pl FROM tbl_rafter_nav WHERE fundo_id = %s ORDER BY data_base;", (fundo_id, ))
    return cursor.fetchall()

def load_fundo_nome(cursor, fundo_id):
    cursor.execute("SELECT nome FROM tbl_funds_info WHERE fundo_id = %s;", (fundo_id,))
    return cursor.fetchone()[0]

def load_fundo_id(cursor, nome):
    cursor.execute("SELECT fundo_id FROM tbl_funds_info WHERE nome = %s;", (nome,))
    return cursor.fetchone()[0]

def load_hist_indice(cursor, indice):
    cursor.execute("SELECT data_base, indice FROM tbl_hist_indices WHERE ativo = %s ORDER BY data_base;", (indice,))
    return cursor.fetchall()

def fn_list_last_day_of_year(list_data_base):
    list_last_day_of_year = []
    cur_date = list_data_base[0]
    last_year = cur_date.year
    last_date = cur_date
    list_last_day_of_year.append(last_date)
    
    for i in range(1, len(list_data_base)):
        cur_date = list_data_base[i]
        cur_year = cur_date.year
        if (cur_year != last_year):
        
            list_last_day_of_year.append(last_date)
            last_year = cur_year
        last_date = cur_date
        
    return list_last_day_of_year

def fn_list_last_day_of_month(list_data_base):
    list_last_day_of_month = []
    cur_date = list_data_base[0]
    last_month = cur_date.month
    last_date = cur_date
    list_last_day_of_month.append(last_date)
    
    for i in range(1, len(list_data_base)):
        cur_date = list_data_base[i]
        cur_month = cur_date.month
        if (cur_month != last_month):
        
            list_last_day_of_month.append(last_date)
            last_month = cur_month
        last_date = cur_date
        
    return list_last_day_of_month

def get_Lista_Dias_Uteis(cursor):
    cursor.execute("SELECT data_base FROM tbl_dias_uteis ORDER BY data_base;")

    return pd.DataFrame(cursor.fetchall(), columns =['Data Base'])


def calc_cdi(cursor, data_base):
        cursor.execute("select indice from tbl_hist_indices where ativo='cdi acumulado' and data_base <= %s order by data_base desc limit 0, 2;", (data_base, ))
        list_cdi_dia = cursor.fetchall()
        cdi_dia = list_cdi_dia[0][0] / list_cdi_dia[1][0] - 1

        return cdi_dia

def calc_cdi_datas(cursor, data_base_inicio, data_base_fim):
    cursor.execute(f"select indice from tbl_hist_indices where ativo = 'cdi acumulado' and data_base <= '{ data_base_inicio }' order by data_base desc limit 0, 1;")
    cdi_inicio = cursor.fetchone()[0]

    cursor.execute(f"select indice from tbl_hist_indices where ativo = 'cdi acumulado' and data_base <= '{ data_base_fim }' order by data_base desc limit 0, 1;")
    cdi_fim = cursor.fetchone()[0]

    return cdi_fim / cdi_inicio - 1

def load_cdi_dia(cursor, data_base):
    cursor.execute(f"select indice from tbl_hist_indices where ativo = 'cdi acumulado' and data_base <= '{ data_base }' order by data_base desc limit 0, 1;")
    cdi_dia = cursor.fetchone()[0]

    return cdi_dia

def get_CDI_Acumulado(cursor):
    cursor.execute("select data_base, indice from tbl_hist_indices where ativo='cdi acumulado' order by data_base;")

    return pd.DataFrame(cursor.fetchall(), columns =['Data Base', 'CDI Acumulado'])

def get_Curva_DI(cursor, data_base, Lista_Dias_Uteis):

    cursor.execute("select TckrSymb, truncate(AdjstdQtTax, 2) from tbl_b3_bvbg_086_01 where left(TckrSymb, 3) = 'DI1' and data_base = (select data_base from tbl_b3_bvbg_086_01 where left(TckrSymb, 3) = 'DI1' and data_base <= %s group by data_base order by data_base desc limit 0, 1);", (data_base, ))

    df_Curva_DI = pd.DataFrame(cursor.fetchall(), columns =['Ticker', 'LastPric'])

    df_Curva_DI['Vencimento'] = df_Curva_DI['Ticker'].apply(ConverteCodigoFuturo)
    df_Curva_DI = df_Curva_DI.sort_values(by = ['Vencimento'])

    df_Curva_DI['DU'] = df_Curva_DI.apply(lambda row: get_Count_WorkingDays(cursor,  data_base, row['Vencimento'], Lista_Dias_Uteis), axis = 1)

    return df_Curva_DI

def get_Curva_DAP(cursor, data_base, Lista_Dias_Uteis):

    cursor.execute("select TckrSymb, truncate(AdjstdQtTax, 2) from tbl_b3_bvbg_086_01 where left(TckrSymb, 3) = 'DAP' and data_base = (select data_base from tbl_b3_bvbg_086_01 where left(TckrSymb, 3) = 'DAP' and data_base <= %s group by data_base order by data_base desc limit 0, 1);", (data_base, ))

    df_Curva_DAP = pd.DataFrame(cursor.fetchall(), columns =['Ticker', 'LastPric'])

    df_Curva_DAP['Vencimento'] = df_Curva_DAP['Ticker'].apply(ConverteCodigoFuturo)
    df_Curva_DAP = df_Curva_DAP.sort_values(by = ['Vencimento'])

    df_Curva_DAP['DU'] = df_Curva_DAP.apply(lambda row: get_Count_WorkingDays(cursor,  data_base, row['Vencimento'], Lista_Dias_Uteis), axis = 1)

    return df_Curva_DAP

def get_Curva_DI_Svensson(cursor, data_base, DU):
    # Calcula a curva DI pelo modelo proposto por Svensson - Parametros calculados pela ANBIMA
    # https://www.anbima.com.br/data/files/9A/F4/E3/1F/4805B710B0F024B7882BA2A8/est-termo_metodologia_v2021.pdf
    

    cursor.execute(f"select beta_1, beta_2, beta_3, beta_4, lambda_1, lambda_2 from tbl_anbima_ettj_param_curva where data_base = '{data_base}' and tipo='PREFIXADOS';")
    df_parametros_curva = pd.DataFrame(cursor.fetchall(), columns =['beta_1', 'beta_2', 'beta_3', 'beta_4', 'lambda_1', 'lambda_2'])
    
    tau = DU / 252
    beta_1 = df_parametros_curva['beta_1'].values
    beta_2 = df_parametros_curva['beta_2'].values
    beta_3 = df_parametros_curva['beta_3'].values
    beta_4 = df_parametros_curva['beta_4'].values
    lambda_1 = df_parametros_curva['lambda_1'].values
    lambda_2 = df_parametros_curva['lambda_1'].values

    termo_1 = beta_1
    termo_2 = beta_2 * ((1 - math.exp(-lambda_1 * tau)) / (lambda_1 * tau))
    termo_3 = beta_3 * ((1 - math.exp(-lambda_1 * tau)) / (lambda_1 * tau) - math.exp(-lambda_1 * tau))
    termo_4 = beta_4 * ((1 - math.exp(-lambda_2 * tau)) / (lambda_2 * tau) - math.exp(-lambda_2 * tau))

    r_t = termo_1 + termo_2 + termo_3 + termo_4

    return r_t

def ConverteCodigoFuturo(ticker):
    ativo = ticker[0:3]
    codigo_mes = ticker[3:4]
    codigo_ano = ticker[4:6]
    if (codigo_mes == 'F'):
        mes_data = 1
    elif (codigo_mes == 'G'):
        mes_data = 2                
    elif (codigo_mes == 'H'):
        mes_data = 3 
    elif (codigo_mes == 'J'):
        mes_data = 4    
    elif (codigo_mes == 'K'):
        mes_data = 5                
    elif (codigo_mes == 'M'):
        mes_data = 6 
    elif (codigo_mes == 'N'):
        mes_data = 7
    elif (codigo_mes == 'Q'):
        mes_data = 8                
    elif (codigo_mes == 'U'):
        mes_data = 9 
    elif (codigo_mes == 'V'):
        mes_data = 10            
    elif (codigo_mes == 'X'):
        mes_data = 11
    elif (codigo_mes == 'Z'):
        mes_data = 12
    
    
    if (ativo =='DI1'):
        vencimento = datetime.date(2000 + int(codigo_ano), mes_data, 1)
    elif (ativo =='DAP'):
        vencimento = datetime.date(2000 + int(codigo_ano), mes_data, 15)

    return vencimento

def InterpolarDIFuturo_old(DU, Curva_DI):
    # Interpola utilizado Cubic Spline

    tck = interpolate.splrep(Curva_DI['DU'], Curva_DI['LastPric'])
    taxa_interpol = interpolate.splev(DU, tck) / 100

    taxa_interpol = truncate(taxa_interpol, 4)
   
    return taxa_interpol

def InterpolarDIFuturo(DU, Curva_DI):
    # Interpola utilizado Exponencial
    if (DU < 0):
        taxa_interpol = 0
        return taxa_interpol
    
    DU_Anterior = Curva_DI.loc[Curva_DI['DU'] <= DU, 'DU'].tail(1).values
    DU_Posterior = Curva_DI.loc[Curva_DI['DU'] >= DU, 'DU'].head(1).values
    i_Anterior = Curva_DI.loc[Curva_DI['DU'] <= DU, 'LastPric'].tail(1).values / 100
    i_Posterior = Curva_DI.loc[Curva_DI['DU'] >= DU, 'LastPric'].head(1).values / 100
    
    # Verifica se DU_Anterior está vazio
    if len(DU_Anterior) == 0:
        taxa_interpol = i_Posterior
        taxa_interpol = truncate(taxa_interpol[0], 4)
        return taxa_interpol
    
    # Verifica se DU_Posterior está vazio
    if len(DU_Posterior) == 0:
        taxa_interpol = i_Anterior
        taxa_interpol = truncate(taxa_interpol[0], 4)
        return taxa_interpol
    
    # Verifica se DU é igual ao DU_Anterior
    if DU == DU_Anterior[0]:
        taxa_interpol = i_Anterior
        taxa_interpol = truncate(taxa_interpol[0], 4)
        return taxa_interpol

    taxa_interpol_1 = (1 + i_Anterior) 
    taxa_interpol_2 = ((1 + i_Posterior) / (1 + i_Anterior))
    taxa_interpol_3 = (DU - DU_Anterior) / (DU_Posterior - DU_Anterior)
    taxa_interpol_4 = taxa_interpol_2 ** taxa_interpol_3
    taxa_interpol_5 = taxa_interpol_1 * taxa_interpol_4
    taxa_interpol = taxa_interpol_5 - 1
    
    taxa_interpol = truncate(taxa_interpol[0], 4)
  
    return taxa_interpol 

def InterpolarDAPFuturo(DU, Curva_DAP):
    # Interpola utilizado Exponencial
    if (DU < 0):
        taxa_interpol = 0
        return taxa_interpol

    DU_Anterior = Curva_DAP.loc[Curva_DI['DU'] <= DU, 'DU'].tail(1).values
    DU_Posterior = Curva_DAP.loc[Curva_DI['DU'] >= DU, 'DU'].head(1).values
    i_Anterior = Curva_DAP.loc[Curva_DI['DU'] <= DU, 'LastPric'].tail(1).values / 100
    i_Posterior = Curva_DAP.loc[Curva_DI['DU'] >= DU, 'LastPric'].head(1).values / 100

    if (DU == DU_Anterior):
        taxa_interpol = i_Anterior
        taxa_interpol = truncate(taxa_interpol[0], 4)
  
        return taxa_interpol

    if (len(DU_Anterior) == 0):
        taxa_interpol = i_Posterior
        taxa_interpol = truncate(taxa_interpol[0], 4)
  
        return taxa_interpol

    taxa_interpol_1 = (1 + i_Anterior) 
    taxa_interpol_2 = ((1 + i_Posterior) / (1 + i_Anterior))
    taxa_interpol_3 = (DU - DU_Anterior) / (DU_Posterior - DU_Anterior)
    taxa_interpol_4 = taxa_interpol_2 ** taxa_interpol_3
    taxa_interpol_5 = taxa_interpol_1 * taxa_interpol_4
    taxa_interpol = taxa_interpol_5 - 1
    
    taxa_interpol = truncate(taxa_interpol[0], 4)
  
    return taxa_interpol

def truncate(number, decimals=0):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)
    
    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor
    
def get_Count_WorkingDays(cursor, data_inicial, data_final, Lista_Dias_Uteis):

    # Exclusive Data Inicial e Inclusive Data Final
    
    data_final = Lista_Dias_Uteis.loc[Lista_Dias_Uteis['Data Base']>= data_final, 'Data Base'].head(1).item()
    
    return Lista_Dias_Uteis[(Lista_Dias_Uteis['Data Base']>= data_inicial) & (Lista_Dias_Uteis['Data Base'] <= data_final)].count()['Data Base'] - 1

def FatorCorrecaoDAP(cursor, data_base, data_base_d_1):

    # CDI - D1 e D0
    cursor.execute(f"select indice from tbl_hist_indices where ativo='CDI Acumulado' and data_base <= '{data_base}' order by data_base desc limit 0, 2;")
    cdi_d0_d1 = cursor.fetchall()
    ret_cdi_d0 = cdi_d0_d1[0][0] / cdi_d0_d1[1][0] - 1

    # Projecao IPCA - D-0 e D-1
    df_IPCA_HIST = pd.read_excel ("R:\\Quant\\DailyRoutines\\PrecificacaoRF\\VNA_IPCA.xlsm", sheet_name = "HIST_IPCA")
    df_IPCA_HIST['Data Base'] = df_IPCA_HIST['Data Base'].dt.strftime("%Y-%m-%d")

    try:
        PROJ_D0 = df_IPCA_HIST.loc[df_IPCA_HIST['Data Base'] == str(data_base), 'IPCA'].item() * 100
    except:
        print("IPCA não cadastrado no arquivo (R:\\Quant\\DailyRoutines\\PrecificacaoRF\\VNA_IPCA.xlsm) para esta data.")
        input("")
        quit()
    PROJ_D1 = df_IPCA_HIST.loc[df_IPCA_HIST['Data Base'] == str(data_base_d_1), 'IPCA'].item() * 100

    # Dados Para Calculo do Ajuste DAP
    M = 0.00025
    cursor.execute(f"SELECT indice FROM tbl_hist_indices WHERE observacao = 'BZPIIPCA Index' and data_base <= '{data_base}' order by data_base desc limit 0, 1;")
    try:
        IPCA = cursor.fetchone()[0]
    except:
        IPCA = 0
    cursor.execute(f"SELECT indice FROM tbl_hist_indices WHERE observacao = 'BZCLVLUE Index' AND data_base = '{data_base}';")
    try:
        IPCA_fechado = cursor.fetchone()[0]
    except:
        IPCA_fechado = 0

    # Datas IPCA Anterior e IPCA Proximo - D0
    data_base_aux = datetime.datetime.strptime(data_base, '%Y-%m-%d')
    dia_atual = data_base_aux.day
    mes_atual = data_base_aux.month
    ano_atual = data_base_aux.year
    if (dia_atual >= 15):
        if(mes_atual == 1):
            mes_anterior = 1
            mes_proximo = mes_atual + 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual
        elif(mes_atual == 12):
            mes_anterior = 12
            mes_proximo = 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual + 1
        else:
            mes_anterior = mes_atual
            mes_proximo = mes_atual + 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual
    else:
        if(mes_atual == 1):
            mes_anterior = 12 
            mes_proximo = mes_atual
            ano_anterior = ano_atual - 1
            ano_proximo = ano_atual
        elif(mes_atual == 12):
            mes_anterior = 11 
            mes_proximo = 12
            ano_anterior = ano_atual
            ano_proximo = ano_atual
        else:
            mes_anterior = mes_atual - 1
            mes_proximo = mes_atual
            ano_anterior = ano_atual
            ano_proximo = ano_atual            
    
    data_ipca_fechado_anterior = datetime.date(ano_anterior, mes_anterior, 15)
    data_ipca_fechado_proximo = datetime.date(ano_proximo, mes_proximo, 15)
    
    # Datas IPCA Anterior e IPCA Proximo - D0
    data_base_aux = data_base_d_1
    dia_atual = data_base_aux.day
    mes_atual = data_base_aux.month
    ano_atual = data_base_aux.year

    if (dia_atual >= 15):
        if(mes_atual == 1):
            mes_anterior = 1
            mes_proximo = mes_atual + 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual
        elif(mes_atual == 12):
            mes_anterior = 12 
            mes_proximo = 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual + 1            
        else:
            mes_anterior = mes_atual
            mes_proximo = mes_atual + 1
            ano_anterior = ano_atual
            ano_proximo = ano_atual
    else:
        if(mes_atual == 1):
            mes_anterior = 12 
            mes_proximo = mes_atual
            ano_anterior = ano_atual - 1
            ano_proximo = ano_atual
        
        else:        
            mes_anterior = mes_atual - 1
            mes_proximo = mes_atual
            ano_anterior = ano_atual
            ano_proximo = ano_atual        
    
    data_ipca_fechado_anterior_d1 = datetime.date(ano_anterior, mes_anterior, 15)
    data_ipca_fechado_proximo_d1 = datetime.date(ano_proximo, mes_proximo, 15)
    
    # Calcula datas para a data base atual
    cursor.execute(f"select data_base from tbl_dias_uteis where data_base >= '{data_ipca_fechado_anterior}' order by data_base asc limit 0, 1;")
    data_ipca_fechado_anterior = cursor.fetchone()[0]
        
    cursor.execute(f"select data_base from tbl_dias_uteis where data_base >= '{data_ipca_fechado_proximo}' order by data_base limit 0, 1;")
    data_ipca_fechado_proximo = cursor.fetchone()[0]

    # Calcula datas para a data base um dia util antes
    cursor.execute(f"select data_base from tbl_dias_uteis where data_base >= '{data_ipca_fechado_anterior_d1}' order by data_base asc limit 0, 1;")
    data_ipca_fechado_anterior_d1 = cursor.fetchone()[0]
        
    cursor.execute(f"select data_base from tbl_dias_uteis where data_base >= '{data_ipca_fechado_proximo_d1}' order by data_base limit 0, 1;")
    data_ipca_fechado_proximo_d1 = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT count(data_base) from tbl_dias_feriados where tipo='BOVESPA' and data_base = '{data_base_d_1}';")
    check_feriado_bovespa = cursor.fetchone()[0]

    if (check_feriado_bovespa == 0):
        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s ;", (data_ipca_fechado_anterior_d1, data_base_d_1))
        dias_passados_ipca_anterior_d1 = cursor.fetchone()[0] 

        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s ;", (data_ipca_fechado_anterior, data_base))
        dias_passados_ipca_anterior_d0 = cursor.fetchone()[0] 

        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s ;", (data_ipca_fechado_anterior, data_ipca_fechado_proximo))
        dias_ipca_fechado_periodo = cursor.fetchone()[0] 

        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s;", (data_ipca_fechado_anterior_d1, data_ipca_fechado_proximo_d1))
        dias_ipca_fechado_periodo_d1 = cursor.fetchone()[0]
    else:
        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s;", (data_ipca_fechado_anterior_d1, data_base_d_1))
        dias_passados_ipca_anterior_d1 = cursor.fetchone()[0] - 11
        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s;", (data_ipca_fechado_anterior, data_base))
        dias_passados_ipca_anterior_d0 = cursor.fetchone()[0] - 11
        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s;", (data_ipca_fechado_anterior, data_ipca_fechado_proximo))
        dias_ipca_fechado_periodo = cursor.fetchone()[0] - 11
        cursor.execute("select count(data_base) from tbl_dias_uteis where data_base > %s and data_base <=%s;", (data_ipca_fechado_anterior_d1, data_ipca_fechado_proximo_d1))
        dias_ipca_fechado_periodo_d1 = cursor.fetchone()[0] - 11


    

    acum_mes_d0 = pow((1 + PROJ_D0 / 100), (dias_passados_ipca_anterior_d0 / (dias_ipca_fechado_periodo)))
    acum_mes_d1 = pow((1 + PROJ_D1 / 100), ((dias_passados_ipca_anterior_d1) / (dias_ipca_fechado_periodo_d1)))   

    if (dias_passados_ipca_anterior_d0 != 0):
        NI_Pro_Rata_D1 = IPCA * acum_mes_d1
    else:
        NI_Pro_Rata_D1 = IPCA / (pow((1 + PROJ_D1 / 100), (1/dias_ipca_fechado_periodo_d1)))
    
    NI_Pro_Rata_D0 = IPCA * acum_mes_d0

    RS_PU = NI_Pro_Rata_D0 * M

    if (check_feriado_bovespa == 1):
        Fator_CDI = (ret_cdi_d0 + 1) * (ret_cdi_d0 + 1)
    else:
        Fator_CDI = (ret_cdi_d0 + 1)

    try:
        Fator_IPCA = NI_Pro_Rata_D0 / NI_Pro_Rata_D1
    except:
        Fator_IPCA = 0

    try:
        Fator_correcao = Fator_CDI / Fator_IPCA
    except:
        Fator_correcao = 0

    return Fator_correcao, RS_PU


def FatorCorrecaoDDI(cursor, data_base, data_base_d_1):

    # CDI - D1 e D0
    cursor.execute(f"select indice from tbl_hist_indices where ativo='CDI Acumulado' and data_base < '{data_base}' order by data_base desc limit 0, 2;")
    cdi_d_1_d_2 = cursor.fetchall()
    ret_cdi_d_1 = cdi_d_1_d_2[0][0] / cdi_d_1_d_2[1][0] - 1

    # PTAX-V - D1 e D0
    cursor.execute(f"select indice from tbl_hist_indices where ativo='PTAX-V USD' and data_base < '{data_base}' order by data_base desc limit 0, 2;")
    ptax_d_1_d_2 = cursor.fetchall()
    ptax_d_1 = ptax_d_1_d_2[0][0]
    ptax_d_2 = ptax_d_1_d_2[1][0]

    try:
        Fator_correcao = (1 + ret_cdi_d_1) / (ptax_d_1 / ptax_d_2)
    except:
        Fator_correcao = 1
    

    return Fator_correcao