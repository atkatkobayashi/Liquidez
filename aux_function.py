from datetime import datetime
from icecream import ic
import pandas as pd

##########################
# FUNCOES AUXILIARES
##########################

def load_dias_uteis(cursor):
        cursor.execute("SELECT * FROM tbl_dias_uteis order by data_base;")
        return pd.DataFrame(cursor.fetchall(), columns = ['dias_uteis'])

def load_posicao_rf(cursor, fundo_id, data_base):
        cursor.execute("SELECT t1.ativo, sum(t1.posicao_qtde), t2.preco, t3.nome, t3.emissor, t3.vencimento, t3.liquidez, t3.veiculo, t3.ticker, t4.vne \
        FROM tbl_posicao_rf_priv t1 \
        left join tbl_precos_cust_rf_priv t2 on t1.ativo = t2.ativo and t1.data_base = t2.data_base and t2.custodia ='BRADESCO' \
        left join tbl_info_ativos t3 on t1.ativo = t3.ativo  \
        left join tbl_anbima_caracteristicas_debentures t4 on t3.ticker = t4.ticker  \
        where t1.fundo_id = %s and t1.data_base = %s \
        group by t1.ativo \
        order by t3.veiculo, t3.emissor;", (fundo_id, data_base))

        posicao_rf = pd.DataFrame(cursor.fetchall(), columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'vencimento', 'liquidez', 'veiculo', 'ticker', 'vne'])
        posicao_rf['financeiro'] = posicao_rf['posicao_qtde'] * posicao_rf['preco']
                                     
        return posicao_rf

def calc_cdi(cursor, data_base):
        cursor.execute("select indice from tbl_hist_indices where ativo='cdi acumulado' and data_base <= %s order by data_base desc limit 0, 2;", (data_base, ))
        list_cdi_dia = cursor.fetchall()
        cdi_dia = list_cdi_dia[0][0] / list_cdi_dia[1][0] - 1

        return cdi_dia

def calc_nro_dias_uteis(data_inicial, data_final, df_dias_uteis):
                                     
        return len(df_dias_uteis[(df_dias_uteis['dias_uteis'] > data_inicial) & (df_dias_uteis['dias_uteis'] <= data_final)])


# columns = ['ativo', 'posicao_qtde', 'preco', 'nome', 'emissor', 'vencimento', 'liquidez', 'veiculo']
def fluxo_debentures(cursor, debenture, df_dias_uteis, cdi_dia, data_base, fundo_id):

        first_loop = True
        amortizacao_acum = 0
        list_fluxo_debenture = []

        ticker = debenture['ticker']
        preco = debenture['preco']
        vne = debenture['vne']
        ativo = debenture['ativo']
        posicao_qtde = debenture['posicao_qtde']
        valor_atual = posicao_qtde * preco

        cursor.execute("select data_fluxo, taxa_juros, indexador_juros, amortizacao, tipo_fluxo from tbl_fluxo_renda_fixa_detalhado WHERE ticker = %s order by data_fluxo, tipo_fluxo desc;", (ticker, ))
        fluxo_atual = pd.DataFrame(cursor.fetchall(), columns = ['data_fluxo', 'taxa_juros', 'indexador_juros', 'amortizacao', 'tipo_fluxo'])
        fluxo_atual['preco'] = preco
        fluxo_atual['valor_fluxo'] = preco * fluxo_atual['taxa_juros']
        
        # TIPO DE FLUXO - % DI
        if (fluxo_atual['indexador_juros'][0].replace(" ", "") == '%DI'):
                
                for i, row in fluxo_atual.iterrows():
                        if (first_loop == True):
                                first_loop = False
                                data_inicial = row['data_fluxo']
                                data_final = data_inicial
                                valor_fluxo = 0
                        else:   
                                
                                data_final = row['data_fluxo']
                                
                                nro_dias_uteis = calc_nro_dias_uteis(data_inicial, data_final, df_dias_uteis)
                                fator_correcao = (1 + cdi_dia * row['taxa_juros']) ** nro_dias_uteis - 1

                                if (row['amortizacao'] != 0):
                                        amortizacao_acum = amortizacao_acum + row['amortizacao']
                                        valor_fluxo = row['amortizacao'] * vne
                                else:
                                        valor_fluxo = vne * fator_correcao * (1 - amortizacao_acum)
                                
                                data_inicial = data_final

                        if (data_final >= datetime.strptime(data_base, '%Y-%m-%d').date()):
                                list_fluxo_debenture.append((fundo_id, ativo, ticker, posicao_qtde, valor_fluxo, data_final, valor_atual))
        
        # TIPO DE FLUXO - DI +
        elif (fluxo_atual['indexador_juros'][0].replace(" ", "") == 'DI+'):
                for i, row in fluxo_atual.iterrows():
                        if (first_loop == True):
                                first_loop = False
                                data_inicial = row['data_fluxo']
                                data_final = data_inicial
                                valor_fluxo = 0
                        else:   
                                
                                data_final = row['data_fluxo']
                                taxa_juros_dia = (1 + row['taxa_juros']) ** (1/252) - 1
                                nro_dias_uteis = calc_nro_dias_uteis(data_inicial, data_final, df_dias_uteis)
                                fator_correcao = ((1 + cdi_dia) * (1 + taxa_juros_dia)) ** nro_dias_uteis - 1

                                if (row['amortizacao'] != 0):
                                        amortizacao_acum = amortizacao_acum + row['amortizacao']
                                        valor_fluxo = row['amortizacao'] * vne
                                else:
                                        valor_fluxo = vne * fator_correcao * (1 - amortizacao_acum)
                                
                                data_inicial = data_final

                        if (data_final >= datetime.strptime(data_base, '%Y-%m-%d').date()):
                                list_fluxo_debenture.append((fundo_id, ativo, ticker, posicao_qtde, valor_fluxo, data_final, valor_atual))

        # TIPO DE FLUXO - IPCA +
        elif (fluxo_atual['indexador_juros'][0].replace(" ", "") == 'IPCA+'):
                
                for i, row in fluxo_atual.iterrows():
                        if (first_loop == True):
                                first_loop = False
                                data_inicial = row['data_fluxo']
                                data_final = data_inicial
                                valor_fluxo = 0
                                
                                cursor.execute("SELECT indice FROM tbl_hist_indices where ativo = 'BZCLVLUE Index' and data_base <= %s order by data_base desc limit 0, 1;", (data_inicial, ))
                                ipca_acum_inicial = cursor.fetchone()[0]
                        else:   
                                
                                data_final = row['data_fluxo']
                                taxa_juros_dia = (1 + row['taxa_juros']) ** (1/252) - 1
                                nro_dias_uteis = calc_nro_dias_uteis(data_inicial, data_final, df_dias_uteis)
                                
                                cursor.execute("SELECT indice FROM tbl_hist_indices where ativo = 'BZCLVLUE Index' and data_base <= %s order by data_base  desc limit 0, 1;", (data_inicial, ))
                                ipca_acum_ultimo = cursor.fetchone()[0]
                                
                                cursor.execute("SELECT ifnull(indice, 0) FROM tbl_hist_indices where ativo = 'BZCLASSU Index' and data_base <= %s order by data_base desc limit 0, 1;", (data_final, ))
                                try:
                                        ipca_projetado = cursor.fetchone()[0] / 100
                                except:
                                        ipca_projetado = 0
                                
                                ipca_acum_final = ipca_acum_ultimo * ((1 + ipca_projetado) ** (nro_dias_uteis / 22))

                                fator_correcao = (ipca_acum_final / ipca_acum_inicial) * ((1 + taxa_juros_dia) ** nro_dias_uteis - 1)
                                
                                if (row['amortizacao'] != 0):

                                        amortizacao_acum = row['amortizacao']
                                        #valor_fluxo = row['amortizacao'] * vne * fator_correcao 
                                        valor_fluxo = row['amortizacao'] * vne * (ipca_acum_final / ipca_acum_inicial)  
                                                                         
                                else:

                                        #valor_fluxo = vne * fator_correcao * (amortizacao_acum)
                                        valor_fluxo = vne * fator_correcao

                                        #if (ticker == 'MRSL17'):
                                        #        ic(data_final, ipca_acum_inicial, ipca_acum_final, valor_fluxo, amortizacao_acum)      


                                
                                data_inicial = data_final

                        if (data_final >= datetime.strptime(data_base, '%Y-%m-%d').date()):
                                list_fluxo_debenture.append((fundo_id, ativo, ticker, posicao_qtde, valor_fluxo, data_final, valor_atual))                
                
        
        return list_fluxo_debenture

