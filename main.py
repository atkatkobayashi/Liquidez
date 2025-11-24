from icecream import ic
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FormatStrFormatter
import matplotlib.ticker as mtick
import matplotlib.ticker as ticker
import pandas as pd
import sys
from babel.numbers import format_currency
import datetime
from dateutil.relativedelta import relativedelta
import os
from os import path
import logging
import traceback

import jinja2

import asyncio
from pyppeteer import launch

import locale
locale.setlocale(locale.LC_ALL, '')  # Use '' for auto, or force e.g. to 'en_US.UTF-8

sys.path.insert(0, r'R:\Quant')
#sys.path.insert(0, r'C:\Users\ATK\OneDrive\Rafter')

import lib_rafter
import lib_pricing_fixed_income
import lib_liquidez_ativo
import lib_liquidez_passivo

# Configuração básica de logging
logging.basicConfig(
    filename='liquidez_error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def CheckLiquidezResgates(FluxoAtivos, ResgatesFuturos, data_base, df_dias_uteis):
    """
    Verifica se a liquidez dos ativos cobre os resgates nas datas de pagamento.
    """
    import pandas as pd
    
    # --- 1. PREPARAR O PASSIVO (Resgates) ---
    df_passivo = ResgatesFuturos.copy()
    df_passivo['data_liquidacao'] = pd.to_datetime(df_passivo['data_liquidacao'])
    fluxo_passivo = df_passivo.groupby('data_liquidacao')['vl_bruto'].sum().reset_index()
    fluxo_passivo.columns = ['data', 'resgate_dia']
    
    # --- 2. PREPARAR O ATIVO (Liquidez) ---
    target_date = pd.to_datetime(data_base)
    df_dias_uteis['Data Base'] = pd.to_datetime(df_dias_uteis['Data Base'])
    
    try:
        idx_inicio = df_dias_uteis[df_dias_uteis['Data Base'] == target_date].index[0]
    except IndexError:
        print(f"Erro Crítico: Data Base {data_base} não encontrada na tabela de dias úteis.")
        return None, pd.DataFrame()

    def get_data_liquidacao(dias):
        target_idx = idx_inicio + int(dias)
        if target_idx < len(df_dias_uteis):
            return df_dias_uteis.loc[target_idx, 'Data Base']
        else:
            return df_dias_uteis.iloc[-1]['Data Base']

    df_ativo = FluxoAtivos.copy()
    df_ativo['data_disponivel'] = df_ativo['dias_liquidar'].apply(get_data_liquidacao)
    
    fluxo_ativo = df_ativo.groupby('data_disponivel')['vl_financeiro'].sum().reset_index()
    fluxo_ativo.columns = ['data', 'liquidez_dia']
    
    # --- 3. CONSOLIDAR E COMPARAR (CORREÇÃO DO WARNING AQUI) ---
    df_alm = pd.merge(fluxo_passivo, fluxo_ativo, on='data', how='outer')
    
    # Correção: infere os tipos corretos antes de preencher com 0 para evitar o FutureWarning
    df_alm = df_alm.infer_objects(copy=False).fillna(0)
    
    df_alm = df_alm.sort_values('data')
    
    # Filtra apenas datas futuras
    df_alm = df_alm[df_alm['data'] >= target_date]

    df_alm['resgate_acumulado'] = df_alm['resgate_dia'].cumsum()
    df_alm['liquidez_acumulada'] = df_alm['liquidez_dia'].cumsum()
    df_alm['sobra_caixa'] = df_alm['liquidez_acumulada'] - df_alm['resgate_acumulado']
    
    # Tolerância de centavos para evitar falsos negativos por arredondamento
    passou_no_check = df_alm['sobra_caixa'].min() >= -0.01 
    
    return passou_no_check, df_alm

def GraficoPercMedioResgates(c, fundo_id, data_base):

    AgregarPercMedioResgateHistList.columns = AgregarPercMedioResgateHistList.columns.astype(str)
    AgregarPercMedioResgateHistList.loc[:, AgregarPercMedioResgateHistList.columns != 'data_base'] = AgregarPercMedioResgateHistList.loc[:, AgregarPercMedioResgateHistList.columns != 'data_base'] * 100
    
    ax = plt.gca()

    AgregarPercMedioResgateHistList.plot(kind='line', x='data_base', y='1', ax=ax, label= '1 ' + '('+ str("{:.2f}".format(AgregarPercMedioResgateHistList['1'].iloc[-1])) + '%)')
    AgregarPercMedioResgateHistList.plot(kind='line', x='data_base', y='5', ax=ax, label= '5 ' + '('+ str("{:.2f}".format(AgregarPercMedioResgateHistList['5'].iloc[-1])) + '%)')
    AgregarPercMedioResgateHistList.plot(kind='line', x='data_base', y='10', ax=ax, label= '10 ' + '('+ str("{:.2f}".format(AgregarPercMedioResgateHistList['10'].iloc[-1])) + '%)')
    AgregarPercMedioResgateHistList.plot(kind='line', x='data_base', y='21', ax=ax, label= '21 ' + '('+ str("{:.2f}".format(AgregarPercMedioResgateHistList['21'].iloc[-1])) + '%)')
    AgregarPercMedioResgateHistList.plot(kind='line', x='data_base', y='63', ax=ax, label= '63 ' + '('+ str("{:.2f}".format(AgregarPercMedioResgateHistList['63'].iloc[-1])) + '%)')

    # Formata gridlines do grafico    
    plt.grid(linestyle = '-', linewidth = 0.5)
    plt.grid(which='minor', linestyle='dotted', linewidth = 0.5)

    # Formata o eixo horizontal
    if(len(AgregarPercMedioResgateHistList)<= 500):
        monthsFmt = mdates.DateFormatter('%m/%Y')
        ax.xaxis.set_major_formatter(monthsFmt)
    elif(len(AgregarPercMedioResgateHistList) > 500 and len(AgregarPercMedioResgateHistList) <= 2000):
        fmt_quarter = mdates.MonthLocator((3,6,9), bymonthday=-1)
        ax.xaxis.set_minor_locator(fmt_quarter)
        monthsFmt = mdates.DateFormatter('%b')
        ax.xaxis.set_minor_formatter(monthsFmt)
    elif(len(AgregarPercMedioResgateHistList)> 2000):
        fmt_quarter = mdates.YearLocator(base=2)
        ax.xaxis.set_minor_locator(fmt_quarter)
        monthsFmt = mdates.DateFormatter('%Y')
        ax.xaxis.set_minor_formatter(monthsFmt)
    
    plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)

    for tick in ax.xaxis.get_minor_ticks():
        tick.label1.set_fontsize(8) 

    # Formata o eixo vertical
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))

    ax.set_xlabel('')

    # Formata o Box de Legenda
    leg = plt.legend()
    leg.get_frame().set_facecolor('#EBEBEB')

    # Titulo do Grafico
    plt.title("Percentual Medio de Resgate")

    # Salva o grafico em formato png
    figure = plt.gcf()

    figure.set_size_inches(24/2.54, 8/2.54)
    plot_filename = "./Reports/aux_files/img/" + str(fundo_id) + "_" + data_base.strftime("%Y-%m-%d").replace("-", "") + ".png"
    plt.savefig(plot_filename, dpi = 600, bbox_inches='tight')
    plt.close()
    
    return 

def GraficoAtivoDiasLiquidar(cursor, fundo_id, data_base):
    
    df_dias_liquidar_ativo_todos = pd.DataFrame()
    list_janela = ['5', '10', '21', '63']

    cursor.execute("select data_base, perc_pl from tbl_passivo_dias_liquidar_ativo where fundo_id = %s and janela = '1' and data_base <= %s order by data_base desc limit 0, 252;", (fundo_id, data_base))
    df_dias_liquidar_ativo = pd.DataFrame(cursor.fetchall(), columns=['data_base', 'perc_pl'])
    df_dias_liquidar_ativo_todos = df_dias_liquidar_ativo

    for janela in list_janela:
        cursor.execute("select data_base, perc_pl from tbl_passivo_dias_liquidar_ativo where fundo_id = %s and janela = %s and data_base <= %s order by data_base desc limit 0, 252;", (fundo_id, janela, data_base))
        df_dias_liquidar_ativo = pd.DataFrame(cursor.fetchall(), columns=['data_base', janela])
       
        df_dias_liquidar_ativo_todos = pd.merge(df_dias_liquidar_ativo_todos, df_dias_liquidar_ativo, on = 'data_base')

    df_dias_liquidar_ativo_todos.columns=['data_base', '1', '5', '10', '21', '63']
    df_dias_liquidar_ativo_todos.loc[:, df_dias_liquidar_ativo_todos.columns != 'data_base'] = df_dias_liquidar_ativo_todos.loc[:, df_dias_liquidar_ativo_todos.columns != 'data_base'] * 100
    df_dias_liquidar_ativo_todos = df_dias_liquidar_ativo_todos.sort_values(by=['data_base'])
        
    ax = plt.gca()
    
    df_dias_liquidar_ativo_todos.plot(kind='line', x='data_base', y='1', ax=ax, label= '1 ' + '('+ str("{:.2f}".format(df_dias_liquidar_ativo_todos['1'].iloc[-1])) + '%)')
    df_dias_liquidar_ativo_todos.plot(kind='line', x='data_base', y='5', ax=ax, label= '5 ' + '('+ str("{:.2f}".format(df_dias_liquidar_ativo_todos['5'].iloc[-1])) + '%)')
    df_dias_liquidar_ativo_todos.plot(kind='line', x='data_base', y='10', ax=ax, label= '10 ' + '('+ str("{:.2f}".format(df_dias_liquidar_ativo_todos['10'].iloc[-1])) + '%)')
    df_dias_liquidar_ativo_todos.plot(kind='line', x='data_base', y='21', ax=ax, label= '21 ' + '('+ str("{:.2f}".format(df_dias_liquidar_ativo_todos['21'].iloc[-1])) + '%)')
    df_dias_liquidar_ativo_todos.plot(kind='line', x='data_base', y='63', ax=ax, label= '63 ' + '('+ str("{:.2f}".format(df_dias_liquidar_ativo_todos['63'].iloc[-1])) + '%)')
    
    # Formata gridlines do grafico    
    plt.grid(linestyle = '-', linewidth = 0.5)
    plt.grid(which='minor', linestyle='dotted', linewidth = 0.5)

    # Formata o eixo horizontal
    if(len(df_dias_liquidar_ativo_todos)<= 500):
        monthsFmt = mdates.DateFormatter('%m/%Y')
        ax.xaxis.set_major_formatter(monthsFmt)
    elif(len(df_dias_liquidar_ativo_todos) > 500 and len(df_dias_liquidar_ativo_todos) <= 2000):
        fmt_quarter = mdates.MonthLocator((3,6,9), bymonthday=-1)
        ax.xaxis.set_minor_locator(fmt_quarter)
        monthsFmt = mdates.DateFormatter('%b')
        ax.xaxis.set_minor_formatter(monthsFmt)
    elif(len(df_dias_liquidar_ativo_todos)> 2000):
        fmt_quarter = mdates.YearLocator(base=2)
        ax.xaxis.set_minor_locator(fmt_quarter)
        monthsFmt = mdates.DateFormatter('%Y')
        ax.xaxis.set_minor_formatter(monthsFmt)
    
    plt.setp(ax.xaxis.get_minorticklabels(), rotation=90)
    
    for tick in ax.xaxis.get_minor_ticks():
        tick.label.set_fontsize(8) 

    # Formata o eixo vertical
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))

    ax.set_xlabel('')

    # Formata o Box de Legenda
    leg = plt.legend()
    leg.get_frame().set_facecolor('#EBEBEB')

    # Titulo do Grafico
    plt.title("Dias Para Liquidar - Ativo")
    
    # Salva o grafico em formato png
    figure = plt.gcf()
    
    figure.set_size_inches(24/2.54, 8/2.54)
    plot_filename = "./Reports/aux_files/img/ativo_dias_liquidar_" + str(fundo_id) + "_" + data_base.strftime("%Y-%m-%d").replace("-", "") + ".png"
    
    plt.savefig(plot_filename, dpi = 600, bbox_inches='tight')
    plt.close()

    return 

def PosicaoMargem(data_base, fundo_id):
    
    list_posicao = []

    # SQL Injection Fix: Usando parâmetros %s em vez de f-string
    cursor.execute("select t1.ativo, t1.vencimento, t1.qtde_depositada, t1.valor \
        from tbl_margem_garantia t1 \
        where t1.data_base= %s and t1.fundo_id= %s;", (data_base, fundo_id))
    
    base_posicao = pd.DataFrame(cursor.fetchall(), columns=['ativo', 'vencimento', 'qtde_margem', 'fin_depositado'])
    
    return base_posicao

def AgregarAtivoPassivo(fundo_id, data_base, FluxoTodosFundo):
    
    if (fundo_id == '22597'):
        fundo_id='16778'
    AgregarPercMedioResgateAtual = lib_liquidez_passivo.PercMedioResgateData(cursor, fundo_id, data_base, '%')

    AgregadoAtivoPassivo = []

    for i, row in AgregarPercMedioResgateAtual.iterrows():
        Janela = int(row['janela'])

        AtivoAgregadoJanelaAtual = FluxoTodosFundo.loc[FluxoTodosFundo['dias_liquidar'] <= int(Janela), 'perc_pl'].sum()
        PassivoAgregadoJanelaAtual =  AgregarPercMedioResgateAtual.loc[AgregarPercMedioResgateAtual['janela'] == int(Janela), 'perc_medio_resgate'].item()
        AgregadoAtivoPassivo.append([Janela, AtivoAgregadoJanelaAtual, PassivoAgregadoJanelaAtual])

    return pd.DataFrame(AgregadoAtivoPassivo, columns = ['janela', 'ativo', 'passivo'])

def CriaRelatorio(dados_fundo, PathImgLogo, PathImgDiasLiquidarAtivo, PathImgPercMedioResgates, InfoConcentracaoPassivo, InfoCotistaParticipacaoRelevante, limite_maximo_cotista, ResgatesFuturos, PosicaoMargemFundo, AgregarAtivoPassivoList, CenariosStress, TabelaALM):

    # Cria Arquivo HTML
    templateLoader = jinja2.FileSystemLoader(searchpath="./")
    templateEnv = jinja2.Environment(loader=templateLoader)
    
    def commafy(value):
        return f"{locale.format_string('%.2f', value, True)}"

    def date_format(value):
        return value.strftime("%d/%m/%Y")

    templateEnv.filters['commafy'] = commafy
    templateEnv.filters['date_format'] = date_format

    TEMPLATE_FILE = "./Reports/ReportFiles/report_file_template.html"
    template = templateEnv.get_template(TEMPLATE_FILE)
    
    outputText = template.render(dados_fundo = dados_fundo, PathImgLogo = PathImgLogo, PathImgDiasLiquidarAtivo = PathImgDiasLiquidarAtivo, PathImgPercMedioResgates = PathImgPercMedioResgates, InfoConcentracaoPassivo = InfoConcentracaoPassivo, 
        ResgatesFuturos = ResgatesFuturos, PosicaoMargemFundo = PosicaoMargemFundo, AgregarAtivoPassivoList = AgregarAtivoPassivoList, InfoCotistaParticipacaoRelevante = InfoCotistaParticipacaoRelevante, limite_maximo_cotista = limite_maximo_cotista,
        CenariosStress = CenariosStress, TabelaALM = TabelaALM)
    
    html_file = open('./Reports/ReportFiles/html_files/' + str(dados_fundo['data_base']).replace('-','') + '_' + str(dados_fundo['fundo_id']) + '.html', 'w')
    html_file.write(outputText)
    html_file.close()   

async def CriaRelatorioPDF(dados_fundo):
    from playwright.async_api import async_playwright
    import os

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        # Construir o caminho completo do arquivo HTML
        html_path = os.path.join(os.path.abspath(os.getcwd()), 'Reports', 'ReportFiles', 'html_files', 
                               f"{str(dados_fundo['data_base']).replace('-','')}_" + 
                               f"{str(dados_fundo['fundo_id'])}.html")
        
        # Converter para URL file://
        file_url = 'file://' + html_path.replace('\\', '/')
        
        await page.goto(file_url)
        
        # Gerar PDF
        pdf_path = os.path.join('Reports', 'ReportFiles', 'pdf_files',
                               f"{str(dados_fundo['data_base']).replace('-','')}_" +
                               f"{str(dados_fundo['fundo_id'])}.pdf")
        
        await page.pdf(path=pdf_path)
        await browser.close()

        # --- LIMPEZA DO ARQUIVO TEMPORÁRIO ---
        if os.path.exists(html_path):
            try:
                os.remove(html_path)
            except Exception as e:
                print(f"Erro ao tentar excluir HTML temporário: {e}")

if __name__ == "__main__":  
    
    # Abre conexao SQL
    conn = lib_rafter.open_sql_conn()
    cursor = conn.cursor(buffered=True)

    data_base = input("Data Base (aaaa-mm-dd): ")
    #data_base = '2022-04-08'
    try:
        data_base = datetime.datetime.strptime(data_base, '%Y-%m-%d').date()
    except ValueError:
        print("Formato de data inválido. Use aaaa-mm-dd.")
        sys.exit(1)
    
    start_time = time.time()

    # Dados Gerais
    try:
        df_dias_uteis = lib_rafter.get_Lista_Dias_Uteis(cursor)
        cdi_dia = lib_rafter.calc_cdi(cursor, data_base)
        CDI_Acumulado = lib_rafter.get_CDI_Acumulado(cursor)
        Curva_DI = lib_rafter.get_Curva_DI(cursor, data_base, df_dias_uteis)
    except Exception as e:
        print(f"Erro crítico ao carregar dados gerais: {e}")
        logging.error(f"Erro crítico ao carregar dados gerais: {e}\n{traceback.format_exc()}")
        sys.exit(1)
        
    JanelasAnalisePassivo = [1, 5, 10, 21, 63]
    list_fundo = ['13151', '13152', '16778', 'PAVA', '14586', '16029', '19019', '14353', '22597']
    #list_fundo = ['13151']

    print("\n")

    for i in range(0, len(list_fundo)):
        fundo_id = list_fundo[i]
        
        try:
            # Dados do Fundo
            dados_fundo = {}
            dados_fundo['data_base'] = data_base
            dados_fundo['fundo_id'] = fundo_id
            dados_fundo['liquidacao_fundo'] = lib_liquidez_ativo.getLiquidacaoFundo(cursor, fundo_id)
            dados_fundo['cotizacao_fundo_dc'] = lib_liquidez_ativo.getCotizacaoFundo(cursor, fundo_id)[0]
            dados_fundo['cotizacao_fundo_du'] = lib_liquidez_ativo.getCotizacaoFundo(cursor, fundo_id)[1]
            dados_fundo['pl_fundo'] = lib_rafter.load_fundo_pl(cursor, fundo_id, data_base)
            dados_fundo['nome_fundo'] = lib_rafter.load_fundo_nome(cursor, fundo_id)
            if fundo_id == 'PAVA':
                dados_fundo['janela_liquidez_ativo'] = 3
                dados_fundo['janela_liquidez_passivo'] = 3
            else:
                dados_fundo['janela_liquidez_ativo'] = lib_liquidez_ativo.janela_liquidez_ativo(dados_fundo['cotizacao_fundo_du'])
                dados_fundo['janela_liquidez_passivo'] = lib_liquidez_ativo.janela_liquidez_passivo(dados_fundo['cotizacao_fundo_du'])

            print(50 * "*")
            print("Fundo: " + str(dados_fundo['nome_fundo']))

            InfoConcentracaoPassivo = lib_liquidez_passivo.ConcentracaoPassivo(cursor, data_base, fundo_id)
            ResgatesFuturos = lib_liquidez_passivo.ListaResgateFuturos(cursor, data_base, fundo_id)
            PosicaoMargemFundo = PosicaoMargem(data_base, fundo_id)  
            
            # Caso um fundo não tenho historico de resgates, adicionar uma condicao para pegar o perfil de um fundo semelhante
            if(fundo_id == '22597'):
                AgregarPercMedioResgateHistList = lib_liquidez_passivo.AgregarPercMedioResgateHist(cursor, '16778', JanelasAnalisePassivo)
            else:
                AgregarPercMedioResgateHistList = lib_liquidez_passivo.AgregarPercMedioResgateHist(cursor, fundo_id, JanelasAnalisePassivo)
            
            GraficoPercMedioResgates(AgregarPercMedioResgateHistList, fundo_id, data_base)

            #FluxoAtivosFundo = lib_liquidez_ativo.CriaDataFrameTodosAtivosFundo(cursor, data_base, fundo_id, cdi_dia, df_dias_uteis, CDI_Acumulado, Curva_DI, dados_fundo, df_dias_uteis)
            
            # SQL Injection Fix: Usando parâmetros %s
            cursor.execute("select data_fluxo, tipo_fluxo, du, vp, posicao_qtde, nome, fundo_id, liquidez, emissor, veiculo, vl_financeiro, perc_pl, dias_liquidar, perc_fluxo_ativo, data_base, vf \
                           from tbl_posicao_fluxo_ativos_fundos where data_base=%s and fundo_id=%s", (data_base, fundo_id))
            
            FluxoAtivosFundo = pd.DataFrame(cursor.fetchall(), columns = ['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo',  'vl_financeiro', 'perc_pl', 'dias_liquidar', 'perc_fluxo_ativo', 'data_base', 'VF'])
                    
            #FluxoAtivosFundo = FluxoAtivosFundo[['data_fluxo', 'tipo_fluxo', 'DU', 'VP', 'posicao_qtde', 'nome', 'fundo_id', 'liquidez', 'emissor', 'veiculo',  'vl_financeiro', 'perc_pl', 'dias_liquidar', 'perc_fluxo_ativo', 'data_base', 'VF']]

            # --- NOVO CHECK DE LIQUIDEZ ---
            print("Verificando Casamento de Liquidez...")
            # Chama a função
            check_status, df_alm_detalhe = CheckLiquidezResgates(FluxoAtivosFundo, ResgatesFuturos, data_base, df_dias_uteis)
            
            if check_status is None:
                # Caso de Erro Técnico (Data não encontrada)
                print(f"⚠️ PULA CHECK: Não foi possível calcular ALM para o fundo {fundo_id} (Verifique tabela de dias úteis).")
                
            elif check_status is False:
                # Caso de Falta de Liquidez Real
                print(f"❌ ALERTA DE LIQUIDEZ: Fundo {fundo_id} possui descasamento de fluxo!")
                
                # Só acessa as colunas se o DataFrame não estiver vazio
                if not df_alm_detalhe.empty and 'sobra_caixa' in df_alm_detalhe.columns:
                    dias_problematicos = df_alm_detalhe[df_alm_detalhe['sobra_caixa'] < 0]
                    print("Dias com insuficiência de caixa:")
                    print(dias_problematicos[['data', 'resgate_acumulado', 'liquidez_acumulada', 'sobra_caixa']].head(3))
                    
                    logging.warning(f"Fundo {fundo_id}: Liquidez insuficiente.\n{dias_problematicos.head(1)}")
            else:
                # Caso Sucesso
                print(f"✅ Check de Liquidez OK: Ativos cobrem todos os resgates futuros.")

            if check_status is not None:
                # 1. EXPORTAR DETALHE PARA EXCEL
                nome_arquivo_alm = f"ALM_Detalhado_{fundo_id}_{data_base}.xlsx"
                
                # Seleciona e renomeia colunas para ficar mais claro no Excel
                df_export = df_alm_detalhe[['data', 'resgate_dia', 'liquidez_dia', 'resgate_acumulado', 'liquidez_acumulada', 'sobra_caixa']].copy()
                df_export.columns = ['Data', 'Resgate do Dia', 'Liquidez do Dia', 'Resgate Acumulado (Passivo)', 'Liquidez Acumulada (Ativo)', 'Sobra de Caixa']
                
                # Salva o arquivo
                df_export.to_excel(nome_arquivo_alm, index=False)
                print(f"   -> Detalhe do ALM salvo em: {nome_arquivo_alm}")
            
            # --- PREPARAÇÃO DA TABELA PARA O RELATÓRIO ---
            # Se o check falhou ou deu erro, passamos uma lista vazia ou a tabela parcial
            if not df_alm_detalhe.empty:
                # Exemplo: Pegar apenas os próximos 10 dias de fluxo ou dias com caixa negativo
                df_view = df_alm_detalhe.head(10).copy() 
                TabelaALM_Formatada = df_view.to_dict('records')
            else:
                TabelaALM_Formatada = []

            GraficoAtivoDiasLiquidar(cursor, fundo_id, data_base)
            FluxoAtivosFundo.to_excel(str(fundo_id) + '.xlsx')

            AgregarAtivoPassivoList = AgregarAtivoPassivo(fundo_id, data_base, FluxoAtivosFundo)       

            # Cotistas com participacao relevante
            limite_maximo_cotista = 0.10
            InfoCotistaParticipacaoRelevante = lib_liquidez_passivo.CotistasParticipacaoRelevante(cursor, data_base, fundo_id, limite_maximo_cotista)
            
            # Cenarios - Stress
            CenariosStress = []
            MaiorResgate1Dia = lib_liquidez_passivo.MaiorResgate1Dia(cursor, data_base, fundo_id)
            CenariosStress.append(['Maior Resgate - 1 Dia (Últimos 3 Anos)', MaiorResgate1Dia, 'OK'])
        
            MaiorPercentualMedioResgate63Dias = [(AgregarPercMedioResgateHistList.loc[AgregarPercMedioResgateHistList['63'].argmax(), 'data_base'], AgregarPercMedioResgateHistList.loc[AgregarPercMedioResgateHistList['63'].argmax(), '63'] / 100)]
            CenariosStress.append(['Maior Percentual Medio Resgate - 63 Dias', MaiorPercentualMedioResgate63Dias, 'OK'])

            CenariosStress = pd.DataFrame(CenariosStress, columns = ['cenario', 'valor_cenario', 'situacao_cenario'])

            # Gera Relatorio de Liquidez
            PathImgLogo = "../../../Reports/aux_files/img/logo.png"
            PathImgPercMedioResgates = "../../../Reports/aux_files/img/" + str(fundo_id) + "_" + data_base.strftime("%Y-%m-%d").replace("-", "") + ".png"
            PathImgDiasLiquidarAtivo = "../../../Reports/aux_files/img/ativo_dias_liquidar_" + str(fundo_id) + "_" + data_base.strftime("%Y-%m-%d").replace("-", "") + ".png"

            CriaRelatorio(dados_fundo, PathImgLogo, PathImgDiasLiquidarAtivo, PathImgPercMedioResgates, InfoConcentracaoPassivo, InfoCotistaParticipacaoRelevante, limite_maximo_cotista, ResgatesFuturos, PosicaoMargemFundo, AgregarAtivoPassivoList, CenariosStress, TabelaALM_Formatada)
            asyncio.run(CriaRelatorioPDF(dados_fundo))
        
            # CHECK DE PL
            # SQL Injection Fix: Usando parâmetros %s
            cursor.execute("select sum(t1.financeiro) from tbl_posicao_cpr_cust t1 where t1.fundo_id=%s and t1.data_base =%s", (fundo_id, data_base))
            pl_despesas = cursor.fetchone()[0]
            
            print("PL Oficial: " + format_currency(dados_fundo['pl_fundo'], '', locale='es_CO'))
            pl_calculado = FluxoAtivosFundo['vl_financeiro'].sum()
            print("PL Calculado: " + format_currency(pl_calculado + pl_despesas, '', locale='es_CO'))
            print("PL Diferença: " + format_currency(dados_fundo['pl_fundo'] - pl_calculado - pl_despesas, '', locale='es_CO'))
        
        except Exception as e:
            print(f"Erro ao processar fundo {fundo_id}: {e}")
            logging.error(f"Erro ao processar fundo {fundo_id}: {e}\n{traceback.format_exc()}")
            continue

    print("Calculo Total --- %.5f seconds ---" % (time.time() - start_time)) 

    input("Processo finalizado...")
    # Fecha conexao SQL
    lib_rafter.close_mysql_conn(conn)