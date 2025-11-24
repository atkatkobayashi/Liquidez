from icecream import ic
import math
import pandas as pd
import sys
import mysql.connector
import datetime
from dateutil.relativedelta import relativedelta
import os
from os import path

sys.path.insert(0, r'R:\Quant')
#sys.path.insert(0, r'C:\Users\ATK\OneDrive\Rafter')

import lib_rafter
import lib_pricing_fixed_income

def CriaDataFrameTodosAtivosFundo(cursor, data_base, fundo_id, cdi_dia, df_dias_uteis, CDI_Acumulado, Curva_DI, dados_fundo, Lista_Dias_Uteis):
    df_Todos_Fluxos = pd.DataFrame([])

    df_Fluxo_RFPriv = FluxoRFPriv(cursor, data_base, fundo_id, cdi_dia, df_dias_uteis, CDI_Acumulado, Curva_DI, dados_fundo, Lista_Dias_Uteis)
    df_Fluxo_FIC = FluxoFIC(cursor, data_base, fundo_id)
    df_Fluxo_RFPub = FluxoRFPub(cursor, data_base, fundo_id, df_dias_uteis)
    df_Fluxo_Over = FluxoOver(cursor, data_base, fundo_id, dados_fundo)
    df_Fluxo_Caixa = FluxoCaixa(cursor, data_base, fundo_id, dados_fundo)
    df_Fluxo_Acoes = FluxoAcoes(cursor, data_base, fundo_id)
    df_Fluxo_Opcoes = FluxoOpcoes(cursor, data_base, fundo_id)
    df_Fluxo_CPR = FluxoCPR(cursor, data_base, fundo_id, dados_fundo)

    if(len(df_Fluxo_RFPriv) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_RFPriv])
    if(len(df_Fluxo_FIC) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_FIC])
    if(len(df_Fluxo_RFPub) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_RFPub])
    if(len(df_Fluxo_Over) != 0):
        if (df_Fluxo_Over['posicao_qtde'][0] != 0):
            df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_Over])
    if(len(df_Fluxo_Caixa) != 0):
        if (df_Fluxo_Caixa['posicao_qtde'][0] != 0):
            df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_Caixa])
    if(len(df_Fluxo_Acoes) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_Acoes])
    if(len(df_Fluxo_Opcoes) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_Opcoes])
    if(len(df_Fluxo_CPR) != 0):
        df_Todos_Fluxos = pd.concat([df_Todos_Fluxos, df_Fluxo_CPR])
        
    df_Todos_Fluxos['vl_financeiro'] = df_Todos_Fluxos['VP'] * df_Todos_Fluxos['posicao_qtde']
    df_Todos_Fluxos['perc_pl'] = df_Todos_Fluxos['vl_financeiro'] / dados_fundo['pl_fundo']
    df_Todos_Fluxos['dias_liquidar'] = df_Todos_Fluxos['DU']

    df_Todos_Fluxos = df_Todos_Fluxos.reset_index(drop=True)

    # Ajusta a quantidade de dias para liquidar
    df_Todos_Fluxos['perc_fluxo_ativo'] = 0.0

    for i, row in df_Todos_Fluxos.iterrows():
        veiculo = row['veiculo']
        emissor = row['emissor']
        ticker= row['nome']
        
        if (veiculo =='DEBENTURE'):
            
            posicao_qtde= row['posicao_qtde']
            if (getTaxaDebAnbima(cursor, data_base, ticker) == 100):
                dias_liquidar = 126
            else:
                dias_liquidar = CalculaDiasLiquidarDebentures(cursor, ticker, posicao_qtde)

            if (dias_liquidar <= row['DU']):
                df_Todos_Fluxos.loc[i, 'dias_liquidar'] = dias_liquidar
            
            if (df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum() != 0):
                df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
            else:
                df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = 0
        elif ((veiculo == 'NTN-B') or (veiculo == 'LFT') or (veiculo == 'LTN')):
            df_Todos_Fluxos.loc[i, 'dias_liquidar'] = 0
            df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
        elif ((veiculo == 'CDB') or (veiculo == 'LF')):
            df_Todos_Fluxos.loc[i, 'dias_liquidar'] = 21
            df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
        elif ((veiculo == 'CDB - SUBORDINADA') or (veiculo == 'LF - SUBORDINADA')):
            df_Todos_Fluxos.loc[i, 'dias_liquidar'] = 42
            df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
        elif ((veiculo == 'CDB - PERPETUA') or (veiculo == 'LF - PERPETUA')):
            df_Todos_Fluxos.loc[i, 'dias_liquidar'] = 126
            df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
        elif ((veiculo == 'DPGE')):
            df_Todos_Fluxos.loc[i, 'dias_liquidar'] = 42
            df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
        elif ((veiculo == 'OPCAO')):
            if "Dólar" in emissor:
                df_Todos_Fluxos.loc[i, 'vl_financeiro'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] * 50
                df_Todos_Fluxos.loc[i, 'perc_fluxo_ativo'] = df_Todos_Fluxos.loc[i, 'vl_financeiro'] / df_Todos_Fluxos[df_Todos_Fluxos['nome'] == ticker]['vl_financeiro'].sum()
    
        
    
    df_Todos_Fluxos['data_base'] = data_base

    # Cria Arquivo Excel
    #df_Todos_Fluxos.to_excel("Fluxo_" + str(fundo_id) + "_" + str(data_base).replace("-", "" ) +  ".xlsx", index=False)

    return df_Todos_Fluxos

def LoadPosicaoRFPriv(cursor, fundo_id, data_base):
    cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.vencimento, t3.liquidez, t3.veiculo, t3.ticker, t4.vne, t4.indexador_ref, t3.subordinacao \
    FROM tbl_posicao_rf_priv t1 \
    left join tbl_precos_cust_rf_priv t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia = (select custodia from tbl_funds_info where fundo_id=t1.fundo_id) \
    left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
    left join tbl_anbima_caracteristicas_debentures t4 on t3.ticker = t4.ticker  \
    where t1.fundo_id = %s and t1.data_base = %s  and t3.moeda = 'BRL' \
    group by t1.ativo \
    order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

    posicao_rf = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'vencimento', 'liquidez', 'veiculo', 'ticker', 'vne', 'indexador', 'subordinacao'])

    return posicao_rf

def LoadPosicaoFIC(cursor, fundo_id, data_base):
    cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.liquidez, t3.veiculo, t3.cotizacao_resgate, t3.cotizacao_resgate_du_dc \
        FROM tbl_posicao_fic t1 \
        left join tbl_precos_cust_fic t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia = (select custodia from tbl_funds_info where fundo_id=t1.fundo_id) \
        left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
        where t1.fundo_id = %s and t1.data_base = %s \
        group by t1.ativo \
        order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

    posicao_fic = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'cotizacao_resgate', 'cotizacao_resgate_du_dc'])

    return posicao_fic

def LoadPosicaoRFPub(cursor, fundo_id, data_base):
    cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.liquidez, t3.veiculo, t3.vencimento \
        FROM tbl_posicao_rf_pub t1 \
        left join tbl_precos_cust_rf_pub t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia = (select custodia from tbl_funds_info where fundo_id=t1.fundo_id) \
        left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
        where t1.fundo_id = %s and t1.data_base = %s \
        group by t3.emissor, t3.vencimento \
        order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

    posicao_rf_pub = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'vencimento'])

    return posicao_rf_pub

def LoadPosicaoAcoes(cursor, fundo_id, data_base):
    cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.liquidez, t3.veiculo, t3.vencimento \
        FROM tbl_posicao_acoes t1 \
        left join tbl_precos_cust_acoes t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia = (select custodia from tbl_funds_info where fundo_id=t1.fundo_id) \
        left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
        where t1.fundo_id = %s and t1.data_base = %s \
        group by t3.emissor, t3.vencimento \
        order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

    try:
        posicao_acoes = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'vencimento'])
    except:
        posicao_acoes = []
    
    return posicao_acoes

def LoadPosicaoOpcoes(cursor, fundo_id, data_base):
    cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.liquidez, t3.veiculo, t3.vencimento \
        FROM tbl_posicao_opcoes t1 \
        left join tbl_precos_cust_opcoes t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia = (select custodia from tbl_funds_info where fundo_id=t1.fundo_id) \
        left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
        where t1.fundo_id = %s and t1.data_base = %s \
        group by t3.emissor, t3.vencimento, t3.nome \
        order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

    posicao_opcoes = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'vencimento'])

    return posicao_opcoes

def FluxoRFPriv(cursor, data_base, fundo_id, cdi_dia, df_dias_uteis, CDI_Acumulado, Curva_DI, dados_fundo, Lista_Dias_Uteis):
    df_Fluxo_RFPriv_Fundo = pd.DataFrame()
    cotizacao_fundo = dados_fundo['cotizacao_fundo_du']

    PosicaoRFPriv = LoadPosicaoRFPriv(cursor, fundo_id, data_base)

    for i, row in PosicaoRFPriv.iterrows():
        veiculo = row['veiculo']
        liquidez = row['liquidez']
        posicao_qtde = row['posicao_qtde']
        emissor = row['emissor']

        if (veiculo == 'DEBENTURE'):
            indexador = row['indexador'].replace(" ", "")
            ticker = row['ticker']
            pu = row['preco']
            taxa = getTaxaDebAnbima(cursor, data_base, ticker)
            
            if (taxa == 100):
                cursor.execute("SELECT indexador_taxa FROM tbl_anbima_caracteristicas_debentures WHERE ticker = %s;", (ticker, ))
                taxa = cursor.fetchone()[0] / 100
            else:
                cursor.execute("SELECT pu FROM tbl_anbima_taxas_tit_privados WHERE ticker = %s and data_base = %s;", (ticker,  data_base))
                preco_anbima = cursor.fetchone()[0]
       
            if (indexador == 'IPCA+'):
                if (abs(preco_anbima - pu) > 0.1):
                    #print("Estimando taxa: " + str(ticker))
                    taxa = lib_pricing_fixed_income.get_Taxa_Debenture_IPCA_Mais(cursor, data_base, taxa, pu, ticker, Lista_Dias_Uteis)
                    #print("Taxa Estimada: " + str(taxa))

                VNA_Atual = lib_pricing_fixed_income.get_VNA_Atual_Debenture_IPCA_Mais(cursor, data_base, ticker, df_dias_uteis)
                df_Fluxo_Debenture = lib_pricing_fixed_income.get_Fluxo_Debenture(cursor, ticker, data_base, VNA_Atual, taxa, df_dias_uteis)
                
            elif (indexador == 'DI+'):
                if (abs(preco_anbima - pu) > 0.1):
                    #print("Estimando taxa: " + str(ticker))
                    taxa = lib_pricing_fixed_income.get_Taxa_Debenture_DI_Mais(cursor, data_base, taxa, pu, ticker, Lista_Dias_Uteis, CDI_Acumulado, Curva_DI)
                    #print("Taxa Estimada: " + str(taxa))

                df_Fluxo_Debenture = lib_pricing_fixed_income.get_Fluxo_Debenture_DI_Mais(cursor, ticker, data_base, taxa, df_dias_uteis, CDI_Acumulado, Curva_DI)

            elif (indexador == '%DI'):
                if (abs(preco_anbima - pu) > 0.1):
                    #print("Estimando taxa: " + str(ticker))
                    taxa = lib_pricing_fixed_income.get_Taxa_Debenture_Perc_DI(cursor, data_base, taxa, pu, ticker, Lista_Dias_Uteis, CDI_Acumulado, Curva_DI)
                    #print("Taxa Estimada: " + str(taxa))

                df_Fluxo_Debenture = lib_pricing_fixed_income.get_Fluxo_Debenture_Perc_DI(cursor, ticker, data_base, taxa, df_dias_uteis, CDI_Acumulado, Curva_DI)

            df_Fluxo_Debenture['ticker'] = ticker
            df_Fluxo_Debenture['fundo_id'] = fundo_id
            df_Fluxo_Debenture['liquidez'] = liquidez
            df_Fluxo_Debenture['posicao_qtde'] = posicao_qtde
            df_Fluxo_Debenture['emissor'] = emissor
            df_Fluxo_Debenture['veiculo'] = veiculo
            df_Fluxo_Debenture = df_Fluxo_Debenture[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'ticker', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]
            #df_Fluxo_Debenture = df_Fluxo_Debenture[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'ticker', 'fundo_id', 'liquidez', 'emissor', 'veiculo']]
            df_Fluxo_RFPriv_Fundo = pd.concat([df_Fluxo_RFPriv_Fundo, df_Fluxo_Debenture])
            df_Fluxo_RFPriv_Fundo.loc[df_Fluxo_RFPriv_Fundo['DU'] <= cotizacao_fundo, 'liquidez'] = 'Líquido'
        
        else:
            vencimento = row['vencimento']
            preco = row['preco']
            subordinacao = row['subordinacao']
            if (subordinacao != ''):
                veiculo = veiculo + ' - ' + subordinacao

            DU = lib_rafter.get_Count_WorkingDays(cursor, data_base, vencimento, df_dias_uteis)
            RF_Priv_Bullet = pd.DataFrame([[vencimento, 'VENCIMENTO', DU, preco, posicao_qtde, veiculo + ' - ' + emissor, fundo_id, liquidez, emissor, veiculo, 0, 0]])
            RF_Priv_Bullet.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'ticker', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
            df_Fluxo_RFPriv_Fundo = pd.concat([df_Fluxo_RFPriv_Fundo, RF_Priv_Bullet])

    # Remove as linhas dos fluxos já vencidos
    if (len(df_Fluxo_RFPriv_Fundo) > 0):
        df_Fluxo_RFPriv_Fundo = df_Fluxo_RFPriv_Fundo[df_Fluxo_RFPriv_Fundo['DU'] >= 0]

        df_Fluxo_RFPriv_Fundo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
        #df_Fluxo_RFPriv_Fundo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo']
    #print(df_Fluxo_RFPriv_Fundo)

    return df_Fluxo_RFPriv_Fundo

def FluxoFIC(cursor, data_base, fundo_id):
    
    df_FluxoFIC = LoadPosicaoFIC(cursor, fundo_id, data_base)
    df_FluxoFIC['data_fluxo'] = datetime.datetime.strptime('3100-01-01', '%Y-%m-%d').date()
    df_FluxoFIC['tipo_fluxo'] = 'VENCIMENTO'
    df_FluxoFIC['DU_DC'] = df_FluxoFIC['cotizacao_resgate_du_dc']
    df_FluxoFIC['DU'] = df_FluxoFIC['cotizacao_resgate']
    df_FluxoFIC['fundo_id'] = fundo_id
    df_FluxoFIC['VP'] = df_FluxoFIC['preco']
    

    for i, row in df_FluxoFIC.iterrows():
        
        if (row['DU_DC'] == 'DC'):
            df_FluxoFIC.loc[i, 'DU'] = Converte_DC_DU(df_FluxoFIC.loc[i, 'DU'])
    
    df_FluxoFIC['VF'] = 0
    df_FluxoFIC['PU_PAR'] = 0

    df_FluxoFIC = df_FluxoFIC[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]

    return df_FluxoFIC

def FluxoRFPub(cursor, data_base, fundo_id, df_dias_uteis):
    
    df_Fluxo_RFPub_Fundo = pd.DataFrame()

    df_FluxoRFPub = LoadPosicaoRFPub(cursor, fundo_id, data_base)

    for i, row in df_FluxoRFPub.iterrows():
        
        posicao_qtde = row['posicao_qtde']
        nome = row['nome']
        liquidez = row['liquidez']
        emissor = row['emissor']
        veiculo = row['veiculo']

        if (veiculo == 'NTN-B'):
            VNA_Atual = lib_pricing_fixed_income.get_VNA_Atual_NTNB(cursor, data_base)
            Data_Proximo_IPCA = lib_pricing_fixed_income.get_Data_Proximo_IPCA(cursor, data_base)
            Data_Anterior_IPCA = lib_pricing_fixed_income.get_Data_Anterior_IPCA(cursor, data_base)

            Dias_Uteis_Periodo = lib_rafter.get_Count_WorkingDays(cursor, Data_Anterior_IPCA, Data_Proximo_IPCA, df_dias_uteis)
            Dias_Corridos_Periodo = lib_rafter.get_Count_WorkingDays(cursor, Data_Anterior_IPCA, data_base, df_dias_uteis)

            Projecao_IPCA_Mes = lib_pricing_fixed_income.get_Projecao_IPCA_Mes(cursor, data_base)
            
            cursor.execute("SELECT taxa_indicativa FROM tbl_anbima_taxas_tit_publicos WHERE titulo = 'ntn-b' and data_base = %s and data_vencimento = %s;", (data_base, row['vencimento']))
            taxa = cursor.fetchone()[0] / 100

            if(Dias_Uteis_Periodo != 0):
                VNA_DataBase = float((VNA_Atual * pow((1 + Projecao_IPCA_Mes / 100), (Dias_Corridos_Periodo / Dias_Uteis_Periodo))))
            else:
                VNA_DataBase = float((VNA_Atual * pow((1 + Projecao_IPCA_Mes / 100), (0))))
            
            #VNA_DataBase = 3911.712972

            RF_Pub_Fluxo = lib_pricing_fixed_income.get_Fluxo_NTNB(cursor, data_base, row['vencimento'], VNA_DataBase, df_dias_uteis, taxa)
       
            RF_Pub_Fluxo['tipo_fluxo'] = 'JUROS'
            RF_Pub_Fluxo['posicao_qtde'] = posicao_qtde
            RF_Pub_Fluxo['nome'] = nome
            RF_Pub_Fluxo['fundo_id'] = fundo_id
            RF_Pub_Fluxo['liquidez'] = liquidez
            RF_Pub_Fluxo['emissor'] = emissor
            RF_Pub_Fluxo['veiculo'] = veiculo
            RF_Pub_Fluxo['PU_PAR'] = 0

            RF_Pub_Fluxo = RF_Pub_Fluxo[['Data_Fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]
            RF_Pub_Fluxo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
            df_Fluxo_RFPub_Fundo = pd.concat([df_Fluxo_RFPub_Fundo, RF_Pub_Fluxo])

        else:
            vencimento = row['vencimento']
            DU = lib_rafter.get_Count_WorkingDays(cursor, data_base, vencimento, df_dias_uteis)
            preco = row['preco']
            
            RF_Pub_Bullet = pd.DataFrame([[vencimento, 'VENCIMENTO', DU, preco, posicao_qtde, nome, fundo_id, liquidez, emissor, veiculo, 0, 0]])
            RF_Pub_Bullet.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
            df_Fluxo_RFPub_Fundo = pd.concat([df_Fluxo_RFPub_Fundo, RF_Pub_Bullet])
    

    return df_Fluxo_RFPub_Fundo

def FluxoAcoes(cursor, data_base, fundo_id):
    
    df_FluxoAcoes = LoadPosicaoAcoes(cursor, fundo_id, data_base)

    # Checa se é feriado na BOVESPA (Feriados SP) em D - 1
    cursor.execute(f"SELECT COUNT(data_base) FROM tbl_dias_feriados WHERE data_base = '{data_base}' AND tipo='BOVESPA';")
    feriado_bovespa_d1 = cursor.fetchone()[0]

    if(len(df_FluxoAcoes) == 0):
        
        return df_FluxoAcoes

    df_FluxoAcoes['data_fluxo'] = data_base
    df_FluxoAcoes['tipo_fluxo'] = 'VENCIMENTO'
    df_FluxoAcoes['DU'] = 2
    df_FluxoAcoes['fundo_id'] = fundo_id
    df_FluxoAcoes['VP'] = df_FluxoAcoes['preco']
    
    list_FluxoAcoes = []

    for i, row in df_FluxoAcoes.iterrows():
        ticker = row['emissor']
        vl_financeiro = row['VP'] * row ['posicao_qtde']
        PU = row['VP']
        ADTV = ADTV_Acoes(cursor, ticker, data_base)
        try:
            ADTV_Part = ADTV / 4
        except:
            ADTV_Part = 0

        if(vl_financeiro > ADTV_Part):
            if (ADTV_Part != 0):
                nro_dias_liquidar = math.floor(vl_financeiro / ADTV_Part)
            else:
                nro_dias_liquidar = 0

            for j in range(0, nro_dias_liquidar):
                df_FluxoAcoes.loc[i, 'posicao_qtde'] = (ADTV_Part / PU)
                df_FluxoAcoes.loc[i, 'DU'] = (j + 2)
                
                list_FluxoAcoes.append(df_FluxoAcoes.loc[i])
            
            if (ADTV_Part != 0):
                df_FluxoAcoes.loc[i, 'posicao_qtde'] = row ['posicao_qtde'] - (ADTV_Part / PU) * (j + 1)
                df_FluxoAcoes.loc[i, 'DU'] = (j + 3)
            
            list_FluxoAcoes.append(df_FluxoAcoes.loc[i])
        else:
            list_FluxoAcoes.append(row)

    df_FluxoAcoes = pd.DataFrame(list_FluxoAcoes)
    df_FluxoAcoes['VF'] = 0
    df_FluxoAcoes['PU_PAR'] = 0
    df_FluxoAcoes = df_FluxoAcoes[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]

    return df_FluxoAcoes

def FluxoOpcoes(cursor, data_base, fundo_id):
    
    df_FluxoOpcoes = LoadPosicaoOpcoes(cursor, fundo_id, data_base)
    df_FluxoOpcoes['data_fluxo'] = datetime.datetime.strptime('3100-01-01', '%Y-%m-%d').date()
    df_FluxoOpcoes['tipo_fluxo'] = 'VENCIMENTO'
    df_FluxoOpcoes['DU'] = 1
    df_FluxoOpcoes['fundo_id'] = fundo_id
    df_FluxoOpcoes['VP'] = df_FluxoOpcoes['preco']

    df_FluxoOpcoes['VF'] = 0
    df_FluxoOpcoes['PU_PAR'] = 0

    df_FluxoOpcoes = df_FluxoOpcoes[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]

    return df_FluxoOpcoes

def FluxoOver(cursor, data_base, fundo_id, dados_fundo):
    pl_fundo = dados_fundo['pl_fundo']
    cursor.execute("SELECT t1.ativo, 1, ifnull(sum(t1.posicao_perc) * %s, 0), t2.nome, t2.emissor, t2.liquidez, t2.veiculo, t2.cotizacao_resgate \
        FROM tbl_posicao_over t1 \
        left join tbl_info_ativos t2 on t1.ativo = t2.ativo \
        where t1.fundo_id= %s and t1.data_base = %s;", (pl_fundo, fundo_id, data_base))

    posicao_over = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'cotizacao_resgate'])

    posicao_over['data_fluxo'] = data_base
    posicao_over['vencimento'] = 'VENCIMENTO'
    posicao_over['fundo_id'] = fundo_id
    posicao_over['DU'] = 0

    posicao_over['VF'] = 0
    posicao_over['PU_PAR'] = 0

    Over_Fluxo = posicao_over[['data_fluxo', 'vencimento', 'DU', 'preco', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]
    Over_Fluxo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
   
    return Over_Fluxo    


def FluxoCaixa(cursor, data_base, fundo_id, dados_fundo):
    pl_fundo = dados_fundo['pl_fundo']
    cursor.execute("SELECT t1.ativo, 1, ifnull(sum(t1.posicao_perc) * %s, 0), t2.nome, t2.emissor, t2.liquidez, t2.veiculo, t2.cotizacao_resgate \
        FROM tbl_posicao_caixa_cust t1 \
        left join tbl_info_ativos t2 on t1.ativo = t2.ativo \
        where t1.fundo_id= %s and t1.data_base = %s;", (pl_fundo, fundo_id, data_base))

    posicao_caixa = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'cotizacao_resgate'])

    posicao_caixa['data_fluxo'] = data_base
    posicao_caixa['vencimento'] = 'VENCIMENTO'
    posicao_caixa['fundo_id'] = fundo_id
    posicao_caixa['DU'] = 0
    posicao_caixa['VF'] = 0
    posicao_caixa['PU_PAR'] = 0
    
    Caixa_Fluxo = posicao_caixa[['data_fluxo', 'vencimento', 'DU', 'preco', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]
    Caixa_Fluxo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo' , 'VF', 'PU_PAR']
   
    return Caixa_Fluxo    

def FluxoCPR(cursor, data_base, fundo_id, dados_fundo):
    pl_fundo = dados_fundo['pl_fundo']
    cursor.execute("SELECT t1.ativo, 1, sum(t1.posicao_perc) * %s, t2.nome, t2.emissor, t2.liquidez, t2.veiculo, t2.cotizacao_resgate \
        FROM tbl_posicao_cpr_cust t1 \
        left join tbl_info_ativos t2 on t1.ativo = t2.ativo \
        where t1.fundo_id= %s and t1.data_base = %s;", (pl_fundo, fundo_id, data_base))

    posicao_cpr = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'cotizacao_resgate'])
    posicao_cpr = posicao_cpr.dropna(subset=['ativo'], inplace=True)
    
    if(posicao_cpr == None):
        return pd.DataFrame(columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'liquidez', 'veiculo', 'cotizacao_resgate'])

    posicao_cpr['data_fluxo'] = data_base
    posicao_cpr['vencimento'] = 'VENCIMENTO'
    posicao_cpr['fundo_id'] = fundo_id
    posicao_cpr['DU'] = 0
    posicao_cpr['VF'] = 0
    posicao_cpr['PU_PAR'] = 0
    
    Caixa_Fluxo = posicao_cpr[['data_fluxo', 'vencimento', 'DU', 'preco', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']]
    Caixa_Fluxo.columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP','posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo', 'VF', 'PU_PAR']
   
    return Caixa_Fluxo    

def getTaxaDebAnbima(cursor, data_base, ticker):
    cursor.execute("SELECT taxa_indicativa FROM tbl_anbima_taxas_tit_privados WHERE data_base = %s AND ticker = %s;", (data_base, ticker))
    try:
        taxa_anbima = cursor.fetchone()[0] / 100
    except:
        taxa_anbima = 100

    return taxa_anbima

def getCotizacaoFundo(cursor, fundo_id):
    cursor.execute("SELECT cotizacao_resgate, cotizacao_resgate_du_dc FROM tbl_funds_info WHERE fundo_id = %s;", (fundo_id,))
    dados_cotizacao = cursor.fetchone()
    cotizacao_resgate = dados_cotizacao[0]
    du_dc = dados_cotizacao[1]

    if(du_dc == 'dc'):
        if(cotizacao_resgate == 30):
            cotizacao_resgate_du = 21
            cotizacao_resgate_dc = cotizacao_resgate
        elif(cotizacao_resgate == 29):
            cotizacao_resgate_du = 21
            cotizacao_resgate_dc = cotizacao_resgate
        elif(cotizacao_resgate == 0):
            cotizacao_resgate_du = 0
            cotizacao_resgate_dc = cotizacao_resgate
        elif(cotizacao_resgate == 1):
            cotizacao_resgate_du = 1
            cotizacao_resgate_dc = cotizacao_resgate            
        elif(cotizacao_resgate == 15):
            cotizacao_resgate_du = 10
            cotizacao_resgate_dc = cotizacao_resgate
        elif(cotizacao_resgate == 10):
            cotizacao_resgate_du = 5
            cotizacao_resgate_dc = cotizacao_resgate
        elif(cotizacao_resgate == 3):
            cotizacao_resgate_du = 3
            cotizacao_resgate_dc = cotizacao_resgate  
        elif(cotizacao_resgate == 60 or cotizacao_resgate == 59):
            cotizacao_resgate_du = 42
            cotizacao_resgate_dc = cotizacao_resgate            
    elif(du_dc == 'du'):
        cotizacao_resgate_du = cotizacao_resgate
        cotizacao_resgate_dc = cotizacao_resgate / 0.7

    return math.ceil(cotizacao_resgate_dc), math.ceil(cotizacao_resgate_du)

def Converte_DC_DU(DC):

    if (DC == 0):
        return 0
    elif ((DC == 1)):
        return 1
    elif ((DC == 10)):
        return 10
    elif (DC>= 25 and DC <= 45):
        return 21
    elif ((DC == 57) or (DC == 58) or (DC == 59) or (DC == 60)):
        return 42
    elif (DC == 90):
        return 63
    elif (DC == 540):
        return 365*2
    else:
        print("Dias Corridos não cadastrados no codigo - lib_liquidez_ativo.py | Função Converte_DC_DU.")

def getLiquidacaoFundo(cursor, fundo_id):
    cursor.execute("SELECT liquidacao_resgate FROM tbl_funds_info WHERE fundo_id = %s;", (fundo_id,))

    return cursor.fetchone()[0]

def CalculaDiasLiquidarDebentures(cursor, ticker, qtde_fundo):

    cursor.execute("select vne, quantidade_emissao, registro_cvm_400_476 from tbl_anbima_caracteristicas_debentures where ticker = %s;", (ticker, ))
    debenture_info = pd.DataFrame(cursor.fetchall(), columns = ['vne', 'qtde_emissao', 'emissao_400_476']) 
    
    vne = debenture_info['vne'].item()
    qtde_emissao = debenture_info['qtde_emissao'].item()
    emissao_400_476 = debenture_info['emissao_400_476'].item()
    fin_emissao = vne * qtde_emissao
    perc_emissao_fundo = qtde_fundo / qtde_emissao
    
    if (emissao_400_476 == 476):
        fator_tipo_emissao = 6
    elif (emissao_400_476 == 400):
        fator_tipo_emissao = 6.5
    else:
        ic(emissao_400_476)
        print("Tipo Emissao 400/476 nao cadastrado (tbl_anbima_caracteristicas_debentures) - " + ticker)
        input("...")
        quit()

    x_0 = 0.15 * (math.log(fin_emissao) / 125) * fator_tipo_emissao
    L = 252
    k = math.log(fin_emissao)

    dias_para_liquidar = L / (1 + math.exp(-k * (perc_emissao_fundo - x_0)))

    return math.ceil(dias_para_liquidar)

def ADTV_Acoes(cursor, ticker, data_base):

    cursor.execute("select avg(daily_volume) from \
        (select NtlFinVol as daily_volume from tbl_b3_bvbg_086_01 where TckrSymb = %s and data_base <= %s order by data_base desc limit 21) DailyVolume;", (ticker, data_base))
    avg_daily_volume = cursor.fetchone()[0]

    return avg_daily_volume

def janela_liquidez_ativo(cotizacao_fundo_du):

    if (cotizacao_fundo_du == 0):
        return 1
    else:
        return cotizacao_fundo_du

def janela_liquidez_passivo(cotizacao_fundo_du):

    if (cotizacao_fundo_du == 0):
        return 5
    else:
        return 63