from _config import *
from _configModels import *
import _configModels
import requests
from datetime import datetime
from calendar import monthrange
import json
import re #Pode ser a causa de alguns problemas ta? Ele não estava instalado na build-dev (PC@IBM)
from os import remove, system, name
from os.path import exists
import csv
from time import sleep
import uuid
from uuid import *

#################### SETUP ####################
LINEBREAK = "\n"
WAITFOR = 3
def start(bank):
    print(f"################ IMPORTAÇÃO AUTOMÁTICA - FASE I ################")
    print(f"Alvo atual: {bank}")
    print(f"Alvo final: FireflyIII(DI)")
    print(f"################################################################")
    spacingConsole()
def cleanGenerated(filepath):
    if exists(filepath):
        remove(filepath)
        print(f"Antigo {filepath} apagado.")
    else:
        print(f"Arquivo '{filepath}' não existe.")
def cleanConsole():
    system('cls' if name == 'nt' else 'clear')
def spacingConsole():
    print("\n")
def waitFor(minutes):
    print(f"Esperando por {minutes} minutos para atualizar o banco de dados...")
    print(f"Futuramente implementarei um webhook para a espera não ser fixa.\n")
    minutes = minutes*60
    sleep(minutes)
    minutes = minutes/60
    print(f"Esperou por {minutes} minutos.{LINEBREAK}")
def updateBanks():
    print("Atualizando extratos...")
    def updateBank_authless(bankname, client, secret, update):
        urlAuth = "https://api.pluggy.ai/auth"
        payloadAuth = {
            "clientId": f"{client}",
            "clientSecret": f"{secret}"
        }
        headersAuth = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        responseAuth = requests.post(urlAuth, json=payloadAuth, headers=headersAuth)
        dataAuth = responseAuth.json()
        apikey = dataAuth["apiKey"]
        
        urlupdate = f"https://api.pluggy.ai/items/{update}"
        payloadUpdate = { "clientUserId": f"{client}"}#, "webhookUrl": f"{WEBHOOK_NU}"}
        headersUpdate = {
            "accept": "application/json",
            "content-type": "application/json",
            "X-API-KEY": f"{apikey}"
        }
        responseUpdate = requests.patch(urlupdate, json=payloadUpdate, headers=headersUpdate)
        print(f"{bankname} Status: {responseUpdate}")
    updateBank_authless("Nubank", CLIENT_NU, SECRET_NU, UPDATE_NU)
    print(LINEBREAK)
    waitFor(WAITFOR)
def getBalance(client, secret, account):
    urlAuth = "https://api.pluggy.ai/auth"

    payloadAuth = {
        "clientId": f"{client}",
        "clientSecret": f"{secret}"
    }
    headersAuth = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    responseAuth = requests.post(urlAuth, json=payloadAuth, headers=headersAuth)
    dataAuth = responseAuth.json()
    APIKEY = dataAuth["apiKey"]

    urlBalance = f"https://api.pluggy.ai/accounts/{account}"

    headersBalance = {
        "accept": "application/json",
        "X-API-KEY": f"{APIKEY}"
    }

    responseBalance = requests.get(urlBalance, headers=headersBalance)
    responseBalance = responseBalance.json()
    responseBalance = responseBalance["balance"]
    print(responseBalance)
# Todas as suas importações continuam iguais aqui

def startJob(bankName, twoLettersBankName, client, secret, accountId):
    saldo_api = 10
    start(bankName)
    cleanGenerated(f"jsonBase_{twoLettersBankName}.json")
    cleanGenerated(f"jsonTratado_{twoLettersBankName}.json")
    cleanGenerated(f"resultado_{twoLettersBankName}.csv")
    spacingConsole()

    # === Gerar a APIKEY e o TOKEN ===
    urlAuth = "https://api.pluggy.ai/auth"
    payloadAuth = { "clientId": f"{client}", "clientSecret": f"{secret}" }
    headersAuth = { "accept": "application/json", "content-type": "application/json" }
    responseAuth = requests.post(urlAuth, json=payloadAuth, headers=headersAuth)
    dataAuth = responseAuth.json()
    APIKEY = dataAuth["apiKey"]

    # === Recuperar saldo atual da API ===
    urlBalance = f"https://api.pluggy.ai/accounts/{accountId}"
    headersBalance = {
        "accept": "application/json",
        "X-API-KEY": f"{APIKEY}"
    }
    responseBalance = requests.get(urlBalance, headers=headersBalance)
    saldo_api = responseBalance.json()["balance"]

    # === Recuperar transações ===
    dateNow = datetime.now()
    c_year = dateNow.year
    c_month = dateNow.month
    l_day = monthrange(c_year, c_month)[1]
    dateStart=f"{c_year}-{c_month:02d}-01"
    dateEnd=f"{c_year}-{c_month:02d}-{l_day:02d}"
    pageSize = 500

    print("Procurando pelo extrato...")
    print(f"Data Inicial: {dateStart}")
    print(f"Data Final: {dateEnd}")
    spacingConsole()

    url = f"https://api.pluggy.ai/transactions?accountId={accountId}&from={dateStart}&to={dateEnd}&pageSize={pageSize}"
    headers = { "accept": "application/json", "X-API-KEY": f"{APIKEY}" }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        dados_json = response.json()
        nome_arquivo = f"jsonBase_{twoLettersBankName}.json"
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados_json, f, indent=2, ensure_ascii=False)
        print("Extrato encontrado e salvo com sucesso.")
        spacingConsole()
    else:
        print(f"Erro na requisição: {response.status_code}")
        print(response.text)
        return

    # === Funções auxiliares ===
    def carregar_json(caminho_arquivo):
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)

    def salvar_json(dados, caminho_saida):
        with open(caminho_saida, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)

    def salvar_como_csv(lista_dicionarios, caminho_csv):
        if not lista_dicionarios:
            print("Nenhuma transação para salvar em CSV.")
            return
        todas_colunas = set()
        for item in lista_dicionarios:
            todas_colunas.update(item.keys())
        colunas = sorted(todas_colunas)
        with open(caminho_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=colunas, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(lista_dicionarios)
        print(f"CSV salvo em: {caminho_csv}")

    def reconciliar_saldo(transacoes, saldo_api, output_json_path, output_csv_path):
        from uuid import uuid4
        saldo_estimado = sum(t["quantia"] for t in transacoes)
        diferenca = round(saldo_api - saldo_estimado, 2)

        print(f"Saldo estimado (base transações): {saldo_estimado}")
        print(f"Saldo real (API):                {saldo_api}")
        print(f"Diferença encontrada:            {diferenca}")

        if diferenca == 0:
            print("✅ Nenhuma diferença. Tudo certo.")
            return transacoes

        elif diferenca > 0:
            nova_transacao = {
                "id": str(uuid4()),
                "data": datetime.now().strftime("%Y-%m-%d"),
                "quantia": diferenca,
                "moeda": "BRL",
                "categoria": "FIX",
                "pagador": 12,
                "recebedor": 1,
                "direcao": "recebida",
                "tipoDetalhado": "RECONCILIAÇÃO",
                "nota": "Reparo automático para igualar saldo com API",
                "descricao": "Reparo de saldo"
            }
            transacoes.append(nova_transacao)
            print("🔧 Transação de reparo adicionada.")
            salvar_json(transacoes, output_json_path)
            salvar_como_csv(transacoes, output_csv_path)
            return transacoes

        else:
            print("❌ O saldo calculado está MAIOR que o saldo da API.")
            print("⚠️ Isso não era esperado. Você precisa decidir quem deve receber a diferença de", abs(diferenca), "R$.")
            return transacoes

    # === Tratamento ===
    def tratarDados(caminho_entrada, caminho_saida_json, caminho_saida_csv):
        json_original = carregar_json(caminho_entrada)
        transacoes = json_original.get("results", [])
        nome_func = f"processar_transacoes_{twoLettersBankName}"
        processar_transacoes = globals().get(nome_func)

        if not processar_transacoes:
            raise ValueError(f"Função {nome_func} não encontrada!")

        transacoes_tratadas = processar_transacoes(transacoes)
        salvar_json(transacoes_tratadas, caminho_saida_json)
        print(f"JSON tratado salvo em: {caminho_saida_json} (Registros ignorados por invalidez: {_configModels.IGNORADOS})")
        salvar_como_csv(transacoes_tratadas, caminho_saida_csv)

        # Reconciliar com saldo da API
        transacoes_final = reconciliar_saldo(
            transacoes_tratadas,
            saldo_api,
            caminho_saida_json,
            caminho_saida_csv
        )

    tratarDados(
        f"jsonBase_{twoLettersBankName}.json",
        f"jsonTratado_{twoLettersBankName}.json",
        f"resultado_{twoLettersBankName}.csv"
    )

    start(bankName)
    cleanGenerated(f"jsonBase_{twoLettersBankName}.json")
    cleanGenerated(f"jsonTratado_{twoLettersBankName}.json")
    cleanGenerated(f"resultado_{twoLettersBankName}.csv")
    spacingConsole()

    # Gerar a APIKEY e o TOKEN
    urlAuth = "https://api.pluggy.ai/auth"

    payloadAuth = {
        "clientId": f"{client}",
        "clientSecret": f"{secret}"
    }
    headersAuth = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    responseAuth = requests.post(urlAuth, json=payloadAuth, headers=headersAuth)
    dataAuth = responseAuth.json()
    APIKEY = dataAuth["apiKey"]

    urlConnector = "https://api.pluggy.ai/connect_token"

    headersConnector = {
        "accept": "application/json",
        "content-type": "application/json",
        "X-API-KEY": f"{APIKEY}"
    }

    responseConnector = requests.post(urlConnector, headers=headersConnector)
    dataConnector = responseConnector.json()
    TOKEN = dataConnector["accessToken"]

    #################### RETRIEVE ####################

    # Pegar data atual (ano, mês e dia) + último dia do mês atual + utils
    dateNow = datetime.now()
    c_year = dateNow.year
    c_month = dateNow.month
    c_day = dateNow.day
    l_day = monthrange(c_year, c_month)[1]
    pageSize=500 # Max 500
    FIRST_DAY = 1
    LAST_DAY = l_day

    ##### DEBUG #####
    # DEBUG = 8
    # dateStartDebug=f"{c_year}-{DEBUG:02d}-{FIRST_DAY:02d}"
    # dateEndDebug=f"{c_year}-{DEBUG:02d}-{LAST_DAY:02d}"
    # url = f"https://api.pluggy.ai/transactions?accountId={ACCOUNTID_NU}&ids=&from={dateStartDebug}&to={dateEndDebug}&pageSize={pageSize}"
    ##### DEBUG #####

    dateStart=f"{c_year}-{c_month:02d}-{FIRST_DAY:02d}"
    dateEnd=f"{c_year}-{c_month:02d}-{LAST_DAY:02d}"
    print("Procurando pelo extrato...")
    print(f"Data Inicial: {dateStart}")
    print(f"Data Final: {dateEnd}")
    spacingConsole()
    url = f"https://api.pluggy.ai/transactions?accountId={accountId}&ids=&from={dateStart}&to={dateEnd}&pageSize={pageSize}"
    #print(f"{url}")
    #print(f"{dataAuth}")
    #print(f"{responseConnector.text}")

    headers = {
        "accept": "application/json",
        "X-API-KEY": f"{APIKEY}"
    }

    response = requests.get(url, headers=headers)
    print("Extrato encontrado, convertendo...")

    if response.status_code == 200:
        dados_json = response.json()

        # Nome do arquivo com data e hora para não sobrescrever
        nome_arquivo = f"jsonBase_{twoLettersBankName}.json"

        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados_json, f, indent=2, ensure_ascii=False)

        print("Convertido com sucesso.")
        spacingConsole()
        print(f"JSON inicial salvo em: {nome_arquivo}")
    else:
        print(f"Erro na requisição: {response.status_code}")
        print(response.text)

    ## Lógica de paginação se passar de 500 (checar se tem page2... até a falha?)
    ## Criptomoedas (eu poderia traçar uma API pra acompanhar? Ou iria me entegar?)

    #################### GENERATE ####################

    def carregar_json(caminho_arquivo):
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)

    def salvar_json(dados, caminho_saida):
        with open(caminho_saida, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=2, ensure_ascii=False)


    ## 7 virou 13 porque só consigo colocar assets accounts + No-networth, mas queria usar expenses
    ## mesma coisa com 6 que virou 12
    def salvar_como_csv(lista_dicionarios, caminho_csv):
        """Salva uma lista de dicionários como um arquivo CSV, mesmo que os dicionários tenham chaves diferentes."""
        if not lista_dicionarios:
            print("Nenhuma transação para salvar em CSV.")
            return

        # Junta todos os campos únicos presentes em qualquer dicionário
        todas_colunas = set()
        for item in lista_dicionarios:
            todas_colunas.update(item.keys())

        colunas = sorted(todas_colunas)  # ordenado só pra consistência

        with open(caminho_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=colunas, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(lista_dicionarios)

        print(f"CSV salvo em: {caminho_csv}")

    def tratarDados(caminho_entrada, caminho_saida):
        json_original = carregar_json(caminho_entrada)
        transacoes = json_original.get("results", [])
        nome_func = f"processar_transacoes_{twoLettersBankName}"
        processar_transacoes = globals().get(nome_func)

        if not processar_transacoes:
            raise ValueError(f"Função {nome_func} não encontrada!")
        
        transacoes_tratadas = processar_transacoes(transacoes)
        salvar_json(transacoes_tratadas, caminho_saida)
        print(f"JSON tratado salvo em: {caminho_saida} (Registros ignorados por invalidez: {_configModels.IGNORADOS})")
        salvar_como_csv(transacoes_tratadas, f"resultado_{twoLettersBankName}.csv")
        spacingConsole()

    tratarDados(f"jsonBase_{twoLettersBankName}.json", f"jsonTratado_{twoLettersBankName}.json")

#################### START ####################
cleanConsole()
updateBanks()
startJob("Nubank", "nu", CLIENT_NU, SECRET_NU, ACCOUNTID_NU)