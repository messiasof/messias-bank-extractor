import requests
import uuid
from datetime import datetime
from calendar import monthrange
import json
import re
from os import remove, system, name
from os.path import exists
import csv
from time import sleep
IGNORADOS = 0

###################### CONFIGURAÇÃO NUBANK ######################
def processar_transacoes_nu(transacoes):
    global IGNORADOS
    IGNORADOS = 0
    def capitalizar_nome(texto):
        def formatar_palavra(palavra):
            if palavra.upper() == "RDB":
                return "RDB"
            return palavra.capitalize()
        return ' '.join([formatar_palavra(p) for p in texto.split()])

    transacoes_tratadas = []

    for transacao in transacoes:
        descricao_original = transacao.get("description", "")
        descricao_raw = transacao.get("descriptionRaw", "")
        tipo_operacao = transacao.get("operationType", "")
        categoria = transacao.get("category", "")
        tipo_transacao = transacao.get("type", "")

        # === Documentos (pagador e recebedor) ===
        receiver = transacao.get("receiver", {})
        payer = transacao.get("paymentData", {}).get("payer") or {}
        receiver_doc = receiver.get("documentNumber", {}).get("value", "") or ""
        payer_doc = payer.get("documentNumber", {}).get("value", "") or ""

        cpf_receiver = receiver_doc.replace(".", "").replace("-", "").strip()
        cpf_payer = payer_doc.replace(".", "").replace("-", "").strip()

        nova_transacao = {
            "id": transacao["id"],
            "data": transacao["date"][:10],
            "quantia": transacao["amount"],
            "moeda": transacao["currencyCode"],
            "categoria": categoria
        }

        desc_lower = descricao_original.lower()
        termos_ignorados = [
            "pix no crédito"
            #"valor adicionado na conta por cartão de crédito"
        ]

        if any(t in desc_lower for t in termos_ignorados):
            IGNORADOS = IGNORADOS + 1
            continue

        # ======== Regras específicas (pagador, recebedor, direcao) ========
        if "transferência recebida" in desc_lower and cpf_receiver == "47925767847" and cpf_payer == "47925767847":
            nova_transacao["pagador"] = 12
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"

        elif "transferência recebida" in desc_lower and cpf_receiver == "47925767847":
            nova_transacao["pagador"] = 12
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"

        elif "transferência enviada" in desc_lower:
            nova_transacao["pagador"] = 1
            nova_transacao["recebedor"] = 14
            nova_transacao["direcao"] = "enviada"

        elif "transferência recebida" in desc_lower:
            nova_transacao["pagador"] = 12
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"
        
        elif categoria.lower() == "same person transfer":
            # Fallback caso não tenha conseguido identificar
            nova_transacao["pagador"] = 12
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"

        elif "aplicação rdb" in desc_lower:
            nova_transacao["pagador"] = 1
            nova_transacao["recebedor"] = 4
            nova_transacao["direcao"] = "enviada"

        elif "resgate rdb" in desc_lower:
            nova_transacao["pagador"] = 4
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"

        elif "recarga de celular" in desc_lower:
            nova_transacao["pagador"] = 1
            nova_transacao["recebedor"] = 14
            nova_transacao["direcao"] = "enviada"

        elif "valor adicionado na conta por cartão de crédito" in desc_lower:
            nova_transacao["recebedor"] = 1
            nova_transacao["direcao"] = "recebida"

        elif "pagamento de fatura" in desc_lower:
            nova_transacao["pagador"] = 1
            nova_transacao["recebedor"] = 13
            nova_transacao["direcao"] = "enviada"

        elif "pagamento efetuado" in desc_lower:
            nova_transacao["pagador"] = 1
            nova_transacao["recebedor"] = 13
            nova_transacao["direcao"] = "enviada"

        else:
            # fallback com base no amount
            if transacao["amount"] < 0:
                nova_transacao["pagador"] = 1
                nova_transacao["direcao"] = "enviada"
            else:
                nova_transacao["recebedor"] = 1
                nova_transacao["direcao"] = "recebida"

        # ======== tipoDetalhado (type + operationType) ========
        tipo = tipo_transacao.strip().upper()
        operacao = tipo_operacao.strip().upper()
        if operacao:
            nova_transacao["tipoDetalhado"] = f"{tipo}, {operacao}"
        else:
            nova_transacao["tipoDetalhado"] = tipo

        # ======== nota (description capitalizada com espaçamento no "|") ========
        nota_raw = descricao_original or ""
        nota_tratada = nota_raw.replace("|", " | ")
        nota_final = capitalizar_nome(nota_tratada.strip())
        nova_transacao["nota"] = nota_final

        # ======== descricao (tratada, sem type ou operationType) ========
        nome_pos_pipe = ""
        if "|" in descricao_original:
            nome_pos_pipe = descricao_original.split("|", 1)[1].strip()
        elif "|" in descricao_raw:
            nome_pos_pipe = descricao_raw.split("|", 1)[1].strip()

        if "aplicação rdb" in desc_lower:
            nova_transacao["descricao"] = "Aplicação RDB"
        elif "resgate rdb" in desc_lower:
            nova_transacao["descricao"] = "Resgate RDB"
        elif nome_pos_pipe:
            nome_capitalizado = capitalizar_nome(nome_pos_pipe)
            nova_transacao["descricao"] = nome_capitalizado
        elif descricao_original.strip():
            nova_transacao["descricao"] = capitalizar_nome(descricao_original.strip())
        else:
            nova_transacao["descricao"] = "Descrição não disponível"

        transacoes_tratadas.append(nova_transacao)

    return transacoes_tratadas