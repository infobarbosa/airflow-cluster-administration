#!/bin/bash
#
# Verificacao do ambiente do laboratorio airflow-cluster-administration.
# Uso (no terminal do code-server):
#   curl -sS https://raw.githubusercontent.com/labzinho/airflow-cluster-administration/main/scripts/check.sh | bash
#

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

FALHAS=0
ok()    { echo -e "  ${GREEN}✓${NC} $1"; }
falha() { echo -e "  ${RED}✗${NC} $1"; FALHAS=$((FALHAS + 1)); }
aviso() { echo -e "  ${YELLOW}!${NC} $1"; }
secao() { echo -e "\n${BOLD}$1${NC}"; }

secao "Memória da Máquina"
MEM_MB=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)
if [ "${MEM_MB}" -ge 3500 ]; then
    ok "Memória RAM detectada: ${MEM_MB} MB"
else
    falha "Memória RAM detectada: ${MEM_MB} MB (o laboratório precisa de pelo menos 4 GB)"
fi

secao "Ferramentas"
for t in airflow psql redis-cli jq svc-status aguardar-dag; do
    if command -v "$t" >/dev/null 2>&1; then ok "$t"; else falha "$t não encontrado"; fi
done

secao "Serviços"
for s in postgres redis api-server scheduler1 scheduler2 dag-processor triggerer worker1 worker2 flower; do
    estado=$(svc-status 2>/dev/null | awk -v s="$s" '$1 == s {print $2}')
    if [ "${estado}" = "UP" ]; then ok "${s}: UP"; else falha "${s}: ${estado:-desconhecido} (veja /var/log/lab/${s}.log)"; fi
done

secao "Airflow"
VERSAO=$(airflow version 2>/dev/null | tail -1)
[ -n "${VERSAO}" ] && ok "Airflow versão ${VERSAO}" || falha "comando 'airflow version' falhou"
EXECUTOR=$(airflow config get-value core executor 2>/dev/null)
[ "${EXECUTOR}" = "CeleryExecutor" ] && ok "Executor: ${EXECUTOR}" || falha "Executor inesperado: ${EXECUTOR:-?}"
HEALTH=$(curl -s --max-time 5 http://localhost:8081/api/v2/monitor/health)
for c in metadatabase scheduler dag_processor triggerer; do
    st=$(echo "${HEALTH}" | jq -r ".${c}.status // empty" 2>/dev/null)
    if [ "${st}" = "healthy" ]; then ok "${c}: healthy"; else falha "${c}: ${st:-sem resposta} (API em http://localhost:8081)"; fi
done

secao "Cluster"
NSCHED=$(echo "${HEALTH}" | jq '.scheduler.instances | length' 2>/dev/null)
[ "${NSCHED:-0}" -ge 2 ] && ok "${NSCHED} schedulers com heartbeat recente" || falha "esperados 2 schedulers ativos, encontrados ${NSCHED:-0}"
[ "$(redis-cli ping 2>/dev/null)" = "PONG" ] && ok "Redis (broker) respondendo" || falha "Redis não responde"
WORKERS=$(curl -s --max-time 10 "http://localhost:5555/api/workers?refresh=1" | jq -r 'keys | join(", ")' 2>/dev/null)
if echo "${WORKERS}" | grep -q worker1 && echo "${WORKERS}" | grep -q worker2; then ok "Workers Celery: ${WORKERS}"; else falha "Workers Celery encontrados: ${WORKERS:-nenhum} (esperados worker1 e worker2)"; fi
NDAGS=$(airflow dags list -o json 2>/dev/null | sed -n '/^\[/,$p' | jq 'length' 2>/dev/null)
[ "${NDAGS:-0}" -ge 4 ] && ok "${NDAGS} DAGs do laboratório registrados" || falha "DAGs registrados: ${NDAGS:-0} (esperados 4)"

secao "Resultado"
if [ "${FALHAS}" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}Ambiente pronto para o laboratório.${NC}"
else
    echo -e "  ${RED}${BOLD}${FALHAS} verificação(ões) falharam.${NC} Aguarde 1 minuto e rode de novo; se persistir, veja o Apêndice C do README."
    exit 1
fi
