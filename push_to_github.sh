#!/bin/bash
# ============================================================
# Script de push para GitHub
#
# USO:
#   1. Revogue os 2 tokens anteriores em github.com/settings/tokens
#   2. Gere um token novo (escopo: repo, expiração: 7 dias)
#   3. Rode:
#      bash push_to_github.sh SEU_TOKEN_NOVO_AQUI
# ============================================================

set -e

TOKEN="$1"

if [ -z "$TOKEN" ]; then
    echo "Uso: bash push_to_github.sh <github_token>"
    echo ""
    echo "Gere um token em: https://github.com/settings/tokens"
    echo "Escopo necessario: repo"
    exit 1
fi

echo "[1/5] Inicializando git..."
git init

echo "[2/5] Adicionando arquivos..."
git add .

echo "[3/5] Criando commit..."
git commit -m "Initial commit - BuscaPlace discovery prototype

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"

echo "[4/5] Configurando remote..."
git branch -M main
git remote add origin "https://${TOKEN}@github.com/vendermaisvendas-wq/fenomeno.git" 2>/dev/null || \
git remote set-url origin "https://${TOKEN}@github.com/vendermaisvendas-wq/fenomeno.git"

echo "[5/5] Enviando para GitHub..."
git push -u origin main

echo ""
echo "=== PUSH CONCLUIDO ==="
echo "https://github.com/vendermaisvendas-wq/fenomeno"
echo ""
echo "IMPORTANTE: Agora limpe o token do historico do git:"
echo "  git remote set-url origin https://github.com/vendermaisvendas-wq/fenomeno.git"
