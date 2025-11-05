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
def startJob(bankName, twoLettersBankName, client, secret, accountId):
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


urlAuth = "https://api.pluggy.ai/auth"

payloadAuth = {
    "clientId": f"{CLIENT_NU}",
    "clientSecret": f"{SECRET_NU}"
}
headersAuth = {
    "accept": "application/json",
    "content-type": "application/json"
}

responseAuth = requests.post(urlAuth, json=payloadAuth, headers=headersAuth)
dataAuth = responseAuth.json()
APIKEY = dataAuth["apiKey"]
print(APIKEY)





urlBalance = f"https://api.pluggy.ai/accounts/{ACCOUNTID_NU}"

headersBalance = {
    "accept": "application/json",
    "X-API-KEY": f"{APIKEY}"
}

responseBalance = requests.get(urlBalance, headers=headersBalance)
responseBalance = responseBalance.json()
responseBalance = responseBalance["balance"]
print(responseBalance)