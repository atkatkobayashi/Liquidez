from datetime import datetime
from dateutil.relativedelta import relativedelta
from django.http import JsonResponse
from icecream import ic
from numpy import empty
import pandas as pd
import sys
import mysql.connector
import datetime
import os
from os import path

from pandas.core.indexes import period

sys.path.insert(0, r'R:\Quant')
#sys.path.insert(0, r'C:\Users\ATK\OneDrive\Rafter')

import lib_rafter

def CheckTotalCotasPassivoFundo(cursor, data_base, fundo_id):
    cursor.execute("SELECT SUM(qtde_cotas) FROM tbl_passivo_posicao_fundo_cotista WHERE data_base = %s AND fundo_id = %s;", (data_base, fundo_id))
    total_cotas_passivo = cursor.fetchone()[0]
    
    cursor.execute("SELECT qtde_cotas FROM tbl_rafter_nav WHERE data_base = %s AND fundo_id = %s;", (data_base, fundo_id))
    total_cotas_ativo = cursor.fetchone()[0]

    if(abs(total_cotas_passivo - total_cotas_ativo) > 0.1):
        
        print("Total Cotas no Passivo: " + str(total_cotas_passivo))
        print("Total Cotas no Ativo: " + str(total_cotas_ativo))
        return False
    else:
        return True

def ListaParticipacaoCotistaFundo(cursor, data_base, fundo_id):

    cursor.execute("SELECT cotista, sum(qtde_cotas) FROM tbl_passivo_posicao_fundo_cotista WHERE data_base = %s AND fundo_id = %s GROUP BY cotista order by sum(qtde_cotas);" , (data_base, fundo_id))
    ParticipacaoCotistaFundo = pd.DataFrame(cursor.fetchall(), columns=['cotista', 'qtde_cotas'])
    ParticipacaoCotistaFundo['part_perc'] = ParticipacaoCotistaFundo['qtde_cotas'] / ParticipacaoCotistaFundo['qtde_cotas'].sum()
    
    return ParticipacaoCotistaFundo

def ListaResgateFuturos(cursor, data_base, fundo_id):
    cursor.execute("select data_cotizacao, data_liquidacao, cotista, sum(vl_bruto), tipo_movimento from  tbl_passivo_movimentacao where data_cotizacao > %s and fundo_id = %s AND tipo_movimento in ('NT', 'NL', 'RL', 'RT') \
        GROUP BY data_cotizacao, cotista, tipo_movimento order by cotista, data_cotizacao, tipo_movimento;", (data_base, fundo_id))
    ResgatesFuturos = pd.DataFrame(cursor.fetchall(), columns = ['data_cotizacao', 'data_liquidacao', 'cotista', 'vl_bruto', 'tipo_movimento'])
    
    for i, row in ResgatesFuturos.iterrows():
        
        cotista = row['cotista']
        vl_bruto = row['vl_bruto']
        tipo_movimento = row['tipo_movimento']

        if (tipo_movimento == 'RT'):
            
            ResgateTotalCotista = ResgatesFuturos.loc[(ResgatesFuturos['cotista'] == str(cotista)) & (ResgatesFuturos['tipo_movimento'] == 'RL'), 'vl_bruto'].sum().item()
            ResgatesFuturos.loc[i, 'vl_bruto'] = QtdeCotasCotistaFundo(cursor, data_base, fundo_id, cotista) * lib_rafter.load_fundo_cota(cursor, fundo_id, data_base) - ResgateTotalCotista
        
    return ResgatesFuturos.sort_values(by=['data_cotizacao'])

def QtdeCotasCotistaFundo(cursor, data_base, fundo_id, cotista):

    cursor.execute("SELECT SUM(qtde_cotas) FROM tbl_passivo_posicao_fundo_cotista WHERE data_base = %s AND fundo_id = %s AND cotista = %s;", (data_base, fundo_id, cotista))
    return cursor.fetchone()[0]

def ResgateMedioFundo(cursor, data_base, fundo_id):
    ResgateMedio = 0
    periodo = [1, 2, 3, 4, 5, 10, 21, 42, 63]

    cursor.execute("DELETE FROM tbl_passivo_perc_resgate WHERE data_base = %s AND fundo_id = %s;", (data_base, fundo_id))

    for i in range(0, len(periodo)):
        cursor.execute("SELECT data_base FROM tbl_rafter_nav where fundo_id = %s and data_base < %s order by data_base desc limit 0, 1;", (fundo_id, data_base))
        try:
            d_1 = cursor.fetchone()[0]
        except:
            return 0

        cursor.execute("SELECT data_base FROM tbl_rafter_nav where fundo_id = %s and data_base <= %s order by data_base desc limit %s, 1;", (fundo_id, data_base, periodo[i]))
        try:
            d_periodo = cursor.fetchone()[0]
        except:
            return 0

        cursor.execute("SELECT avg(pl) FROM tbl_rafter_nav where fundo_id = %s and (data_base between %s and %s);", (fundo_id, d_periodo, d_1))
        pl_medio = cursor.fetchone()[0]

        resgates_cotizados_periodo = CalculaResgatesCotizadosPeriodo(cursor, d_periodo, d_1, fundo_id)
        
        ResgateMedio = (resgates_cotizados_periodo / pl_medio)

        cursor.execute("INSERT INTO tbl_passivo_perc_resgate (data_base, fundo_id, janela, perc_resgate) VALUES (%s, %s, %s, %s);", (data_base, fundo_id, periodo[i], ResgateMedio))

    return 0

def CalculaResgatesCotizadosPeriodo(cursor, data_inicial, data_final, fundo_id):
    ResgatesCotizadosPeriodo = 0

    cursor.execute("select data_cotizacao, cotista, sum(vl_bruto), tipo_movimento from  tbl_passivo_movimentacao where (data_cotizacao between %s and %s)  and fundo_id = %s AND tipo_movimento in ('NL', 'RL', 'RT') \
        GROUP BY data_cotizacao, cotista, tipo_movimento order by cotista, data_cotizacao, tipo_movimento;", (data_inicial, data_final, fundo_id))
    df_ResgatesCotizadosPeriodo = pd.DataFrame(cursor.fetchall(), columns = ['data_cotizacao', 'cotista', 'vl_bruto', 'tipo_movimento'])

    for i, row in df_ResgatesCotizadosPeriodo.iterrows():
        data_cotizacao = row['data_cotizacao']
        tipo_movimento = row['tipo_movimento']
        vl_bruto = row['vl_bruto']
        cotista = row['cotista']

        if(tipo_movimento == 'RL' or tipo_movimento == 'NL'):
            ResgatesCotizadosPeriodo = ResgatesCotizadosPeriodo + vl_bruto
        elif(tipo_movimento == 'RT'):
            cursor.execute("SELECT data_base FROM tbl_rafter_nav where fundo_id = %s and data_base < %s order by data_base desc limit 0, 1;", (fundo_id, data_cotizacao))
            d_1 = cursor.fetchone()[0]

            if (QtdeCotasCotistaFundo(cursor, d_1, fundo_id, cotista)):
                ResgatesCotizadosPeriodo = ResgatesCotizadosPeriodo + QtdeCotasCotistaFundo(cursor, d_1, fundo_id, cotista) * lib_rafter.load_fundo_cota(cursor, fundo_id, data_cotizacao)
            else:
                ResgatesCotizadosPeriodo = ResgatesCotizadosPeriodo + vl_bruto

    return ResgatesCotizadosPeriodo

def MediaPercentualResgate(cursor, data_base, fundo_id):
    periodo = [1, 2, 3, 4, 5, 10, 21, 42, 63]

    cursor.execute("SELECT data_base FROM tbl_rafter_nav where fundo_id = %s and data_base <= %s order by data_base desc limit 125, 1;", (fundo_id, data_base))
    try:
        d_126 = cursor.fetchone()[0] # Conforme definido no Manual da Anbima
    except:
        return 0

    cursor.execute("DELETE FROM tbl_passivo_perc_medio_resgate WHERE data_base = %s AND fundo_id = %s;", (data_base, fundo_id))

    for i in range(0, len(periodo)):

        cursor.execute("SELECT avg(perc_resgate) FROM rafter_investimentos.tbl_passivo_perc_resgate WHERE fundo_id = %s and janela = %s and data_base between %s and %s;", (fundo_id, periodo[i], d_126, data_base))
        perc_medio_resgate = cursor.fetchone()[0]

        cursor.execute("INSERT INTO tbl_passivo_perc_medio_resgate (data_base, fundo_id, janela, perc_medio_resgate) VALUES (%s, %s, %s, %s);", (data_base, fundo_id, periodo[i], perc_medio_resgate))

    return 0

def ConcentracaoPassivo(cursor, data_base, fundo_id):
    
    print(fundo_id)
    cursor.execute("select cotista, sum(qtde_cotas) from tbl_passivo_posicao_fundo_cotista where data_base = %s and fundo_id = %s group by cotista order by sum(qtde_cotas) desc;", (data_base, fundo_id))
    ListaPassivo = pd.DataFrame(cursor.fetchall(), columns=['cotista', 'qtde_cotas'])
    TotalCotas = ListaPassivo['qtde_cotas'].sum().item()

    QtdeCotistas = len(ListaPassivo)
    Part1MaiorCotista = ListaPassivo[:1]['qtde_cotas'].item() / TotalCotas
    Part3MaiorCotista = ListaPassivo[:3]['qtde_cotas'].sum().item() / TotalCotas
    Part5MaiorCotista = ListaPassivo[:5]['qtde_cotas'].sum().item() / TotalCotas
    Part10MaiorCotista = ListaPassivo[:10]['qtde_cotas'].sum().item() / TotalCotas
    Part25MaiorCotista = ListaPassivo[:25]['qtde_cotas'].sum().item() / TotalCotas

    InfoCotistasFundo = {}
    InfoCotistasFundo['QtdeCotistas'] = QtdeCotistas
    InfoCotistasFundo['Part1MaiorCotista'] = Part1MaiorCotista
    InfoCotistasFundo['Part3MaiorCotista'] = Part3MaiorCotista
    InfoCotistasFundo['Part5MaiorCotista'] = Part5MaiorCotista
    InfoCotistasFundo['Part10MaiorCotista'] = Part10MaiorCotista
    InfoCotistasFundo['Part25MaiorCotista'] = Part25MaiorCotista

    return InfoCotistasFundo

def CotistasParticipacaoRelevante(cursor, data_base, fundo_id, limite_maximo_cotista):
    cursor.execute("select cotista, sum(qtde_cotas) from tbl_passivo_posicao_fundo_cotista where data_base = %s and fundo_id = %s group by cotista order by sum(qtde_cotas) desc;", (data_base, fundo_id))
    ListaPassivo = pd.DataFrame(cursor.fetchall(), columns=['cotista', 'qtde_cotas'])
    TotalCotas = ListaPassivo['qtde_cotas'].sum().item()

    ListaPassivo['part_perc'] = ListaPassivo['qtde_cotas'] / TotalCotas
    ListaPassivo['limite'] = limite_maximo_cotista
    ListaPassivo = ListaPassivo[ListaPassivo['part_perc'] >= limite_maximo_cotista]
    ListaPassivo['excesso_limite'] = ListaPassivo['part_perc'] - ListaPassivo['limite']

    return ListaPassivo

def PercMedioResgateHist(cursor, fundo_id, janela):

    cursor.execute("select data_base, perc_medio_resgate from tbl_passivo_perc_medio_resgate where fundo_id = %s and janela = %s order by data_base", (fundo_id, janela))
    ListaPercMedioResgate = pd.DataFrame(cursor.fetchall(), columns = ['data_base', 'perc_medio_resgate'])

    return ListaPercMedioResgate

def AgregarPercMedioResgateHist(cursor, fundo_id, JanelasAnalisePassivo):
    
    JanelaLista = []

    for i in range(0, len(JanelasAnalisePassivo)):
        JanelaAtual = JanelasAnalisePassivo[i]

        JanelaAtualList = PercMedioResgateHist(cursor, fundo_id, JanelaAtual)
        JanelaAtualList.columns=['data_base', JanelaAtual]

        if (len(JanelaLista) == 0):
            JanelaLista = JanelaAtualList
        else:
            JanelaLista = JanelaLista.merge(JanelaAtualList, on='data_base', how='left')

    return JanelaLista

def ConvertePercMedioResgateHist_JSON(JanelaLista):
    # TRANSFORMA PARA FORMATO JSON
    JanelaLista['data_base'] = pd.to_datetime(JanelaLista['data_base'])
    JanelaLista['data_base'] = JanelaLista['data_base'].dt.strftime('%d/%m/%Y')
    JanelaLista = pd.DataFrame(JanelaLista).to_json(orient='records',date_format='iso')
    
    json_data = {'JanelaLista': JanelaLista}
    
       
    return json_data

def PercMedioResgateData(cursor, fundo_id, data_base, janela):

    cursor.execute("select janela, perc_medio_resgate from tbl_passivo_perc_medio_resgate where fundo_id = %s and janela like %s and data_base = %s order by data_base", (fundo_id, janela, data_base))
    PercMedioResgate = pd.DataFrame(cursor.fetchall(), columns =['janela', 'perc_medio_resgate'])

    return PercMedioResgate

def MaiorResgate1Dia(cursor, data_base, fundo_id):

    cursor.execute("select data_cotizacao, max(resg_max) from (select t1.data_cotizacao, (sum(t1.vl_bruto) / t2.pl) as resg_max \
        from tbl_passivo_movimentacao t1 \
        left join tbl_rafter_nav t2 on t1.data_base = t2.data_base and t2.fundo_id = t1.fundo_id \
        where t1.data_base >= %s and t1.data_base <= %s and t1.fundo_id = %s and t1.tipo_movimento in ('NL', 'RL', 'RT') \
        group by t1.data_cotizacao) tbl_aux;", ((data_base - relativedelta(months=36)), data_base, fundo_id))
    
    return cursor.fetchall()