# Administração do Apache Airflow 3 em Cluster

Author: Prof. Barbosa<br>
Github: [infobarbosa](https://github.com/infobarbosa)

No laboratório [airflow-orchestration-demo](https://github.com/infobarbosa/airflow-orchestration-demo), o foco é **usar** o Airflow: escrever DAGs, agendar, reprocessar, encadear pipelines. Este laboratório olha para o outro lado, o de quem **opera** a plataforma: como os componentes se dividem, como as tasks chegam aos workers, o que limita a concorrência e, principalmente, **o que acontece quando cada peça cai**.

O ambiente é um Airflow 3 em cluster de verdade: **CeleryExecutor** com broker **Redis**, **dois schedulers** em alta disponibilidade, **dois workers** com filas diferentes, DAG Processor, Triggerer, API Server, Flower e o banco de metadados no **PostgreSQL**. Cada componente é um processo que você pode parar, matar e religar.

Os comandos são copy-paste no terminal do laboratório, na ordem em que aparecem.

---

## O que você vai saber fazer ao final

- Identificar os processos de um cluster Airflow 3 e o papel de cada um, inclusive a dependência das tasks em relação ao **API Server** (Task Execution API).
- Ler a configuração efetiva do Airflow e explicar a precedência entre `airflow.cfg` e variáveis de ambiente.
- Consultar o banco de metadados para responder perguntas operacionais: quem está vivo (tabela `job`), onde cada task rodou, quantas estão em cada estado.
- Explicar o caminho de uma task no **CeleryExecutor** (scheduler → Redis → worker) e direcionar tasks para workers específicos com **filas**.
- Distinguir os limites de concorrência (`parallelism`, `worker_concurrency`, Pools) e prever quantas tasks rodam ao mesmo tempo.
- Demonstrar a **alta disponibilidade de schedulers** e diagnosticar um componente morto pelo seu *heartbeat*.
- Descrever o que acontece quando cai um worker, o API Server ou o broker, e como recuperar as tasks afetadas.
- Coletar métricas StatsD, autenticar na API REST com token e fazer manutenção do banco de metadados (`pg_dump`, `airflow db clean`).
- Relacionar cada processo do laboratório com o container correspondente no `docker-compose.yaml` oficial e com os componentes de uma instalação em Kubernetes.

---

## Pré-requisitos

- Acesso ao **AWS Academy** (o ambiente é uma instância EC2 de **8 GB de RAM** criada pelo script `launch-lab.sh`, logo a seguir).
- Um navegador. Nada precisa ser instalado.
- Recomendado: ter feito o laboratório [airflow-orchestration-demo](https://github.com/infobarbosa/airflow-orchestration-demo) ou conhecer os conceitos de DAG, task, DAG run e task instance.

Fora da aula, o mesmo ambiente roda em qualquer máquina com **Docker** — veja o [Apêndice D](#d-execução-local).

---

## Índice

[O ambiente](#o-ambiente)

**Parte 1 — Arquitetura e configuração**

1. [Um cluster Airflow em processos](#1-um-cluster-airflow-em-processos)
2. [Configuração efetiva e precedência](#2-configuração-efetiva-e-precedência)
3. [O banco de metadados](#3-o-banco-de-metadados)

**Parte 2 — CeleryExecutor**

4. [Do scheduler ao worker](#4-do-scheduler-ao-worker)
5. [Filas: escolhendo o worker](#5-filas-escolhendo-o-worker)
6. [Concorrência e Pools](#6-concorrência-e-pools)

**Parte 3 — Falhas e recuperação**

7. [Dois schedulers: alta disponibilidade](#7-dois-schedulers-alta-disponibilidade)
8. [Um worker cai no meio da task](#8-um-worker-cai-no-meio-da-task)
9. [O API Server cai](#9-o-api-server-cai)
10. [O broker cai](#10-o-broker-cai)

**Parte 4 — Operação**

11. [Observabilidade: saúde, logs e métricas](#11-observabilidade-saúde-logs-e-métricas)
12. [Usuários, papéis e a API REST](#12-usuários-papéis-e-a-api-rest)
13. [Manutenção do banco de metadados](#13-manutenção-do-banco-de-metadados)
14. [Do laboratório à produção](#14-do-laboratório-à-produção)

**Apêndices**

- [A. Referência rápida de comandos](#a-referência-rápida-de-comandos)
- [B. Encerrando o ambiente](#b-encerrando-o-ambiente)
- [C. Troubleshooting](#c-troubleshooting)
- [D. Execução local](#d-execução-local)

---

## O ambiente

O laboratório roda em uma instância **AWS EC2 com 8 GB de RAM** (`t3.large`) criada a partir do **AWS CloudShell**. A instância executa um container com o **code-server** (Visual Studio Code no navegador) e um cluster **Apache Airflow 3.4** completo, com os 10 serviços abaixo como processos independentes.

![Os dez serviços do cluster dentro do container: PostgreSQL e Redis, API Server, dois schedulers, DAG Processor, Triggerer, dois workers Celery com filas diferentes e o Flower](diagramas/01-cluster-do-laboratorio.svg)

> **ATENÇÃO:**
> 1. **Região:** utilize sempre a região **Norte da Virgínia (`us-east-1`)** no Console AWS.
> 2. **Custos:** a instância EC2 e o volume EBS consomem créditos do seu laboratório AWS Academy. Ao encerrar a aula, garanta que a instância esteja parada (**Stopped**) ou aguarde o desligamento automático da sessão.
> 3. **Não recrie a máquina para continuar o trabalho:** cada execução do `launch-lab.sh` cria uma instância **nova e vazia**, com data e hora no nome. Se a sua instância já existir, siga as instruções de [Retomando o laboratório](#retomando-o-laboratório).

### Passo 1: Abrir o AWS CloudShell

1. Faça login no Console da AWS através do seu painel do AWS Academy.
2. Certifique-se de que a região selecionada no canto superior direito é **N. Virginia (`us-east-1`)**.
3. No topo do console, clique no ícone do **AWS CloudShell** (`>_`) ao lado da barra de pesquisa.
4. Aguarde o terminal do CloudShell carregar.

### Passo 2: Executar o script de provisionamento

No terminal do CloudShell, execute:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-cluster-administration/main/launch-lab.sh | bash

```

O script realiza de forma automatizada:

- Descoberta da VPC padrão e de uma Subnet numa zona que ofereça o tipo `t3.large` (8 GB de RAM).
- Configuração do Security Group `lab-airflow-cluster-sg`, liberando as portas `8080` (IDE), `8081` (Airflow), `5555` (Flower) e `22` (SSH).
- Lançamento de uma nova instância EC2 `lab-airflow-cluster-AAAA-MM-DD_HH_MI_SS` (Ubuntu 24.04, 20 GB gp3).
- Instalação do Docker e inicialização do container `airflow-cluster-lab`.
- Exibição do IP público e dos links de acesso.

### Passo 3: Acessar a IDE

1. Abra no navegador o link exibido ao final do script: `http://<IP-PUBLICO>:8080`.
2. Caso a página não responda, **aguarde de 3 a 5 minutos** para que o Docker conclua o download da imagem e atualize o navegador.
3. O code-server abre diretamente na IDE, sem solicitar senha.
4. Abra um terminal: menu superior (três linhas horizontais) > **Terminal** > **New Terminal**.

![Menu do code-server aberto: ícone de três linhas no canto superior esquerdo, item Terminal e, no submenu, New Terminal](images/ide-01-menu-terminal.png)

> **Atenção!** Todos os comandos deste laboratório são executados no **terminal do code-server**. O CloudShell só serve para criar a instância.

As outras interfaces web ficam no mesmo IP:

| Interface | Endereço | Acesso |
|---|---|---|
| **IDE (code-server)** | `http://<IP-PUBLICO>:8080` | sem senha |
| **Airflow** | `http://<IP-PUBLICO>:8081` | usuário `admin`, senha `admin` |
| **Flower** (monitor do Celery) | `http://<IP-PUBLICO>:5555` | sem senha |

Diferente do laboratório de uso, a UI do Airflow aqui exige login (seção 12):

![Tela de login do Airflow 3 com os campos Username e Password e o aviso de que o Simple auth manager está habilitado](images/airflow-01-login.png)

### Retomando o laboratório

Se a sessão do AWS Academy expirar, a instância será parada. **Não execute o `launch-lab.sh` novamente:** ele criaria uma instância nova, sem o seu trabalho.

1. No Console AWS (`us-east-1`), acesse **EC2** > **Instances**.
2. Selecione a sua instância **`lab-airflow-cluster-<data e hora>`** (se houver mais de uma, a mais recente) e clique em **Instance state** > **Start instance**.
3. Aguarde o status **Running** e copie o novo **Public IPv4 address** (ele muda a cada reinício).
4. Acesse `http://<NOVO_IP_PUBLICO>:8080`. O container reinicia automaticamente e o banco de metadados é preservado.

### O que acontece na subida do container

O banco de metadados já vem migrado dentro da imagem. Na subida, o container inicia os 10 serviços, um por vez, em cerca de **30 segundos**:

```sh
# 0.1
cat /var/log/lab/boot.log

```

Output esperado:
```
[boot] 2026-11-27 15:20:17 Iniciando os servicos do cluster...
[postgres] Iniciado (PID 65, log: /var/log/lab/postgres.log).
[redis] Iniciado (PID 94, log: /var/log/lab/redis.log).
[api-server] Iniciado (PID 132, log: /var/log/lab/api-server.log).
[scheduler1] Iniciado (PID 208, log: /var/log/lab/scheduler1.log).
[scheduler2] Iniciado (PID 239, log: /var/log/lab/scheduler2.log).
[dag-processor] Iniciado (PID 269, log: /var/log/lab/dag-processor.log).
[triggerer] Iniciado (PID 300, log: /var/log/lab/triggerer.log).
[worker1] Iniciado (PID 331, log: /var/log/lab/worker1.log).
[worker2] Iniciado (PID 415, log: /var/log/lab/worker2.log).
[flower] Iniciado (PID 489, log: /var/log/lab/flower.log).
[boot] Airflow API Server respondendo em http://localhost:8081
SERVICO        ESTADO PID      PORTA / FILAS
-------        ------ ---      -------------
postgres       UP     65       5432
redis          UP     94       6379
api-server     UP     132      8081
scheduler1     UP     208      -
scheduler2     UP     239      -
dag-processor  UP     269      -
triggerer      UP     300      -
worker1        UP     331      fila default (logs :8793)
worker2        UP     415      filas default,pesado (logs :8795)
flower         UP     489      5555
[boot] 2026-11-27 15:20:43 Ambiente pronto.
```

Se o arquivo ainda não tiver chegado em `Ambiente pronto.`, aguarde alguns segundos e repita o comando.

### Verificando o ambiente

```sh
# 0.2
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-cluster-administration/main/scripts/check.sh | bash

```

Output esperado:
```
Memória da Máquina
  ✓ Memória RAM detectada: 7816 MB

Ferramentas
  ✓ airflow
  ✓ psql
  ✓ redis-cli
  ✓ jq
  ✓ svc-status
  ✓ aguardar-dag

Serviços
  ✓ postgres: UP
  ✓ redis: UP
  ✓ api-server: UP
  ✓ scheduler1: UP
  ✓ scheduler2: UP
  ✓ dag-processor: UP
  ✓ triggerer: UP
  ✓ worker1: UP
  ✓ worker2: UP
  ✓ flower: UP

Airflow
  ✓ Airflow versão 3.4.0
  ✓ Executor: CeleryExecutor
  ✓ metadatabase: healthy
  ✓ scheduler: healthy
  ✓ dag_processor: healthy
  ✓ triggerer: healthy

Cluster
  ✓ 2 schedulers com heartbeat recente
  ✓ Redis (broker) respondendo
  ✗ Workers Celery encontrados: nenhum (esperados worker1 e worker2)
  ✓ 4 DAGs do laboratório registrados

Resultado
  1 verificação(ões) falharam. Aguarde 1 minuto e rode de novo; se persistir, veja o Apêndice C do README.
```

### Convenções

Cada bloco de comando do roteiro tem na primeira linha um rótulo, como `# 4.a.1` (seção 4, subseção 4.a, primeiro bloco). Os blocos sem rótulo, logo depois, mostram a saída esperada.

Alguns valores **mudam a cada execução**: datas e horas, identificadores de execução, PIDs, durações e a divisão exata de tasks entre os workers. Já os estados finais, os nomes dos serviços e os limites de concorrência devem bater.

Os comandos `svc-status`, `svc-stop`, `svc-start`, `svc-kill` e `aguardar-dag` são atalhos deste laboratório (veja o [Apêndice A](#a-referência-rápida-de-comandos)). O `svc-kill` mata o processo com `SIGKILL`, sem dar a ele chance de avisar ninguém: é o mais próximo de uma máquina que some da rede.

Os DAGs usados nos experimentos (`distribuicao`, `fila_pesada`, `tarefa_longa` e `carga`) já vêm prontos em `~/project/dags/cluster_lab.py`, e já nascem ativos.

---

# Parte 1 — Arquitetura e configuração

## 1. Um cluster Airflow em processos

### 1.a. Os serviços

```sh
# 1.a.1
svc-status

```

Output esperado:
```
SERVICO        ESTADO PID      PORTA / FILAS
-------        ------ ---      -------------
postgres       UP     65       5432
redis          UP     94       6379
api-server     UP     132      8081
scheduler1     UP     208      -
scheduler2     UP     239      -
dag-processor  UP     269      -
triggerer      UP     300      -
worker1        UP     331      fila default (logs :8793)
worker2        UP     415      filas default,pesado (logs :8795)
flower         UP     489      5555
```

| Serviço | Papel | Quantos em produção? |
|---|---|---|
| `postgres` | Banco de metadados: o estado de tudo | 1 (gerenciado: RDS, Cloud SQL...) |
| `redis` | **Broker** do Celery: a fila onde o scheduler deixa as tasks para os workers | 1 (ou um cluster Redis/RabbitMQ) |
| `api-server` | UI, API REST e **Task Execution API** (por onde as tasks em execução falam com o Airflow) | 2 ou mais, atrás de um balanceador |
| `scheduler1`, `scheduler2` | Criam DAG runs, decidem quais tasks estão prontas e as enviam ao executor | 2 ou mais (alta disponibilidade) |
| `dag-processor` | Lê e serializa os arquivos de DAG | 1 ou mais |
| `triggerer` | Acompanha as tasks *deferred* | 1 ou mais |
| `worker1`, `worker2` | Workers Celery: retiram tasks do broker e as executam | de poucos a centenas |
| `flower` | Monitor web do Celery | 1 (opcional) |

### 1.b. Quem está vivo: a tabela `job`

Cada scheduler, DAG Processor e Triggerer registra um **job** no banco de metadados e atualiza periodicamente o seu `latest_heartbeat`. É assim que o Airflow (e você) sabe quem está vivo:

```sh
# 1.b.1
psql -d airflow -c "SELECT id, job_type, hostname, state, latest_heartbeat FROM job ORDER BY id;"

```

Output esperado:
```
 id |    job_type     |   hostname    |  state  |       latest_heartbeat
----+-----------------+---------------+---------+-------------------------------
  1 | SchedulerJob    | scheduler1    | running | 2026-11-27 15:20:42.566814+00
  2 | SchedulerJob    | scheduler2    | running | 2026-11-27 15:20:44.583459+00
  3 | DagProcessorJob | dag-processor | running | 2026-11-27 15:20:41.073306+00
  4 | TriggererJob    | triggerer     | running | 2026-11-27 15:20:42.607116+00
(4 rows)
```

No laboratório, todos os processos rodam no mesmo container e teriam o mesmo *hostname*. A opção `[core] hostname_callable` foi apontada para uma função que devolve o nome do serviço (`scheduler1`, `worker2`...), para você saber quem fez o quê.

### 1.c. O endpoint de saúde

```sh
# 1.c.1
curl -s http://localhost:8081/api/v2/monitor/health | jq '{metadatabase: .metadatabase.status, scheduler: .scheduler.status, schedulers: [.scheduler.instances[].hostname], dag_processor: .dag_processor.status, triggerer: .triggerer.status}'

```

Output esperado:
```
{
  "metadatabase": "healthy",
  "scheduler": "healthy",
  "schedulers": [
    "scheduler2",
    "scheduler1"
  ],
  "dag_processor": "healthy",
  "triggerer": "healthy"
}
```

É este endpoint que um balanceador de carga ou um *liveness probe* do Kubernetes consulta. Repare que ele lista as **duas** instâncias de scheduler.

## 2. Configuração efetiva e precedência

### 2.a. O `airflow.cfg` do laboratório

```sh
# 2.a.1
grep -vE '^\s*(#|$)' $AIRFLOW_HOME/airflow.cfg | sed -E 's/^(fernet_key|jwt_secret) = .*/\1 = (gerado no primeiro boot)/'

```

Output esperado:
```
[core]
dags_folder = /home/barbosa/project/dags
executor = CeleryExecutor
parallelism = 16
load_examples = False
dags_are_paused_at_creation = False
execution_api_server_url = http://localhost:8081/execution/
hostname_callable = lab_hostname.nome_do_no
simple_auth_manager_users = admin:admin,operador:viewer
simple_auth_manager_all_admins = False
simple_auth_manager_passwords_file = /home/barbosa/airflow/senhas.json
fernet_key = (gerado no primeiro boot)
[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
[celery]
broker_url = redis://localhost:6379/0
result_backend = db+postgresql://airflow:airflow@localhost:5432/airflow
worker_concurrency = 4
[api]
host = 0.0.0.0
port = 8081
workers = 1
[api_auth]
jwt_secret = (gerado no primeiro boot)
[dag_processor]
refresh_interval = 10
[scheduler]
task_instance_heartbeat_timeout = 60
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

As opções de cluster: `executor = CeleryExecutor`; o `broker_url` (Redis) e o `result_backend` (PostgreSQL, onde o Celery registra o resultado de cada task); `worker_concurrency = 4` (tasks simultâneas por worker); e `task_instance_heartbeat_timeout = 60` (seção 8).

### 2.b. Variáveis de ambiente vencem o arquivo

Toda opção pode ser definida por variável de ambiente no formato `AIRFLOW__<SEÇÃO>__<CHAVE>`. Em containers e no Kubernetes, essa é a forma mais comum de configurar o Airflow:

```sh
# 2.b.1
airflow config get-value celery worker_concurrency
AIRFLOW__CELERY__WORKER_CONCURRENCY=8 airflow config get-value celery worker_concurrency

```

Output esperado:
```
4
8
```

A ordem de precedência, da maior para a menor: variável de ambiente (incluindo as variantes `_CMD` e `_SECRET`, que leem o valor de um comando ou de um *secrets backend*) → `airflow.cfg` → valor padrão do Airflow.

> **Toda a instalação precisa da mesma configuração.** Uma `fernet_key` diferente num worker o impede de ler as senhas das Connections; um `jwt_secret` diferente faz o API Server recusar os tokens das tasks; um `broker_url` diferente põe o worker para escutar outra fila. Em produção, a configuração vem de uma única fonte (variáveis do *Helm chart*, um ConfigMap, um *secrets manager*).

## 3. O banco de metadados

### 3.a. Conectividade e versão do esquema

```sh
# 3.a.1
airflow db check 2>&1 | tail -1
airflow db check-migrations 2>&1 | tail -1
psql -d airflow -Atc "SELECT count(*) || ' tabelas' FROM information_schema.tables WHERE table_schema = 'public';"

```

Output esperado:
```
2026-11-27T15:20:54.403879Z [info     ] Connection successful.         [airflow.utils.db] loc=db.py:1579
62 tabelas
```

O `airflow db migrate` cria e atualiza o esquema; nesta imagem, ele rodou durante o *build*. Numa atualização de versão do Airflow, ele é o primeiro passo, antes de subir os componentes novos.

### 3.b. As tabelas que um operador consulta

| Tabela | O que guarda |
|---|---|
| `job` | Schedulers, DAG Processors e Triggerers com seus heartbeats |
| `dag`, `serialized_dag`, `dag_version` | DAGs registrados e cada versão do código |
| `dag_run` | Cada execução de DAG |
| `task_instance` | Cada tentativa de cada task: estado, worker (`hostname`), fila, datas |
| `slot_pool` | Pools e seus slots |
| `log` | Log de auditoria (quem fez o quê pela UI, API ou CLI) |
| `celery_taskmeta` | Resultados gravados pelos workers Celery (`result_backend`) |

Essas tabelas vão aparecer ao longo do roteiro.

---

# Parte 2 — CeleryExecutor

## 4. Do scheduler ao worker

![Caminho de uma task no CeleryExecutor: o scheduler publica a mensagem numa fila do Redis, um worker Celery a consome e executa a task, que fala com o API Server pela Task Execution API; o resultado fica no banco de metadados](diagramas/02-celery-executor.svg)

Com o CeleryExecutor, o scheduler **não executa** nada: ele publica uma mensagem por task numa fila do Redis. Cada worker mantém `worker_concurrency` processos prontos e consome as mensagens das filas que escuta.

### 4.a. Distribuindo 8 tasks

O DAG `distribuicao` tem 8 tasks independentes de 5 segundos (geradas com `expand`):

```sh
# 4.a.1
airflow dags trigger distribuicao >/dev/null
aguardar-dag distribuicao

```

Output esperado:
```
[aguardar-dag] DAG distribuicao registrado.
[aguardar-dag] Execucoes de distribuicao:
  manual__2026-11-27T15:20:58.965516+00:00  success
```

Quem executou cada task fica na coluna `hostname` de `task_instance`:

```sh
# 4.a.2
psql -d airflow -c "SELECT hostname, queue, count(*) AS tasks FROM task_instance WHERE dag_id = 'distribuicao' GROUP BY hostname, queue ORDER BY hostname;"

```

Output esperado:
```
 hostname |  queue  | tasks
----------+---------+-------
 worker1  | default |     4
 worker2  | default |     4
(2 rows)
```

Os dois workers dividiram o trabalho. Nenhum deles foi "escolhido" pelo scheduler: cada um retirou mensagens da fila `default` enquanto tinha processos livres.

### 4.b. O Flower

Abra o Flower em `http://<IP-PUBLICO>:5555`. A aba **Workers** mostra os dois workers Celery (`worker1@lab` e `worker2@lab`), as filas que cada um consome e quantas tasks cada um já processou:

![Flower, aba Workers: worker1@lab e worker2@lab online, com as colunas de tasks ativas, processadas, com falha e com sucesso](images/flower-01-workers.png)

## 5. Filas: escolhendo o worker

### 5.a. Uma task que só o `worker2` executa

Os workers não precisam ser iguais. Um pode ter mais memória, uma GPU ou acesso a uma rede específica. A **fila** (`queue`) de uma task decide quais workers podem executá-la. No laboratório, o `worker1` escuta só a fila `default`; o `worker2` escuta `default` e `pesado`. A task `pesada` do DAG `fila_pesada` declara `@task(queue="pesado")`:

```sh
# 5.a.1
airflow dags trigger fila_pesada >/dev/null
aguardar-dag fila_pesada >/dev/null
psql -d airflow -c "SELECT task_id, queue, hostname, state FROM task_instance WHERE dag_id = 'fila_pesada' ORDER BY start_date;"

```

Output esperado:
```
 task_id |  queue  | hostname |  state
---------+---------+----------+---------
 leve    | default | worker2  | success
 pesada  | pesado  | worker2  | success
(2 rows)
```

### 5.b. Sem consumidor, a mensagem espera no broker

Pare o `worker2` e dispare o DAG de novo:

```sh
# 5.b.1
svc-stop worker2
airflow dags trigger fila_pesada >/dev/null
sleep 30
psql -d airflow -c "SELECT task_id, queue, hostname, state FROM task_instance WHERE dag_id = 'fila_pesada' AND state <> 'success';"
redis-cli llen pesado

```

Output esperado:
```
[worker2] Parado (PID 415).
 task_id | queue  | hostname | state
---------+--------+----------+--------
 pesada  | pesado |          | queued
(1 row)

1
```

A task `leve` rodou no `worker1`, mas a `pesada` ficou em `queued`: a mensagem está na lista `pesado` do Redis (`llen` = 1), e ninguém escuta essa fila. No Flower, o `worker2` aparece como **Offline**:

![Flower, aba Workers: worker1@lab Online e worker2@lab Offline, com o total de tasks processadas por cada um](images/flower-02-worker2-offline.png)

Religue o `worker2`:

```sh
# 5.b.2
svc-start worker2
aguardar-dag fila_pesada
redis-cli llen pesado

```

Output esperado:
```
[worker2] Iniciado (PID 1611, log: /var/log/lab/worker2.log).
[aguardar-dag] DAG fila_pesada registrado.
[aguardar-dag] Execucoes de fila_pesada:
  manual__2026-11-27T15:21:13.276185+00:00  success
  manual__2026-11-27T15:21:30.397675+00:00  success
0
```

## 6. Concorrência e Pools

### 6.a. Quantas tasks rodam ao mesmo tempo?

O DAG `carga` tem 20 tasks de 30 segundos. Dispare-o e, 25 segundos depois, conte as tasks por estado:

```sh
# 6.a.1
airflow dags trigger carga >/dev/null
sleep 25
psql -d airflow -c "SELECT state, count(*) FROM task_instance WHERE dag_id = 'carga' GROUP BY state ORDER BY state;"

```

Output esperado:
```
   state   | count
-----------+-------
 queued    |     8
 running   |     8
 scheduled |     4
(3 rows)
```

Três limites trabalharam juntos:

| Limite | Valor | Efeito |
|---|---|---|
| `[celery] worker_concurrency` | 4 por worker | No máximo 2 × 4 = **8** tasks em `running` |
| `[core] parallelism` | 16 | No máximo 16 tasks em `queued` + `running` na instalação: 8 rodando e 8 esperando no broker |
| Pool `default_pool` | 128 slots | Não limitou nada desta vez |

As 4 restantes ficaram em `scheduled`: prontas, mas o scheduler não as enviou ao executor porque o `parallelism` estava esgotado. Também existem limites por DAG (`max_active_tasks`, padrão 16) e por task (`max_active_tis_per_dag`).

### 6.b. Reduzindo o Pool ao vivo

Pools podem ser alterados sem reiniciar nada. Reduza o `default_pool` (onde caem as tasks sem pool declarado) para 4 slots:

```sh
# 6.b.1
airflow pools set default_pool 4 "Reduzido para o experimento" >/dev/null
sleep 40
psql -d airflow -c "SELECT state, count(*) FROM task_instance WHERE dag_id = 'carga' GROUP BY state ORDER BY state;"

```

Output esperado:
```
  state  | count
---------+-------
 running |     4
 success |    16
(2 rows)
```

Assim que as tasks em execução terminaram, só 4 novas puderam começar. Devolva o Pool ao tamanho original e espere o DAG terminar:

```sh
# 6.b.2
airflow pools set default_pool 128 "Default pool" >/dev/null
aguardar-dag carga

```

Output esperado:
```
[aguardar-dag] DAG carga registrado.
[aguardar-dag] Execucoes de carga:
  manual__2026-11-27T15:22:18.704449+00:00  success
```

---

# Parte 3 — Falhas e recuperação

![O que acontece quando cada componente cai: scheduler, worker, API Server e broker, com o efeito nas tasks e a forma de recuperação](diagramas/03-falhas-e-recuperacao.svg)

## 7. Dois schedulers: alta disponibilidade

### 7.a. Matando um scheduler

Os dois schedulers trabalham **ao mesmo tempo**, sem um líder. Eles se coordenam pelo banco de metadados: antes de mexer em um DAG run, cada scheduler trava as linhas correspondentes (`SELECT ... FOR UPDATE SKIP LOCKED`), e o outro simplesmente pula o que está travado. Por isso o Airflow exige um banco com bloqueio de linhas (PostgreSQL ou MySQL 8) para rodar mais de um scheduler.

Mate o `scheduler1` e dispare um DAG:

```sh
# 7.a.1
svc-kill scheduler1
airflow dags trigger distribuicao >/dev/null
aguardar-dag distribuicao | tail -1

```

Output esperado:
```
[scheduler1] Morto abruptamente (SIGKILL, PID 208).
  manual__2026-11-27T15:23:58.116223+00:00  success
```

O `scheduler2` fez todo o trabalho sozinho, sem nenhuma intervenção.

### 7.b. Diagnosticando um componente morto

Espere 40 segundos e olhe a tabela `job`:

```sh
# 7.b.1
sleep 40
psql -d airflow -c "SELECT id, hostname, state, date_trunc('second', now() - latest_heartbeat) AS sem_heartbeat FROM job WHERE job_type = 'SchedulerJob' ORDER BY id;"
airflow jobs check --job-type SchedulerJob --hostname scheduler1 2>&1 | tail -1
airflow jobs check --job-type SchedulerJob --hostname scheduler2 2>&1 | tail -1

```

Output esperado:
```
 id |  hostname  |  state  | sem_heartbeat
----+------------+---------+---------------
  1 | scheduler1 | running | 00:01:02
  2 | scheduler2 | running | 00:00:09
(2 rows)

No alive jobs found.
Found one alive job.
```

O `scheduler1` continua com `state = running`: ele morreu com `SIGKILL` e não teve como atualizar o próprio registro. **O que diz se um componente está vivo é o heartbeat**, não o estado gravado. Depois de `[scheduler] scheduler_health_check_threshold` (30 s) sem heartbeat, o `airflow jobs check` (usado nos *liveness probes*) e o endpoint de saúde passam a considerá-lo morto. Religue-o:

```sh
# 7.b.2
svc-start scheduler1

```

Output esperado:
```
[scheduler1] Iniciado (PID 2135, log: /var/log/lab/scheduler1.log).
```

## 8. Um worker cai no meio da task

### 8.a. Derrubando o worker que está executando

O DAG `tarefa_longa` tem uma task de 90 segundos, com 1 retry. Dispare-o, descubra em qual worker a task está rodando e mate esse worker:

```sh
# 8.a.1
airflow dags trigger tarefa_longa >/dev/null
sleep 20
WORKER=$(psql -d airflow -Atc "SELECT hostname FROM task_instance WHERE dag_id = 'tarefa_longa' AND state = 'running';")
echo "A task está rodando no $WORKER"
svc-kill $WORKER

```

Output esperado:
```
A task está rodando no worker2
[worker2] Morto abruptamente (SIGKILL, PID 1611).
```

### 8.b. O que o Airflow faz

Acompanhe as tentativas da task a cada 15 segundos (cerca de 3 minutos no total):

```sh
# 8.b.1
for i in $(seq 1 12); do
  sleep 15
  echo "$((i*15))s: $(psql -d airflow -Atc "SELECT string_agg('tentativa ' || try_number || ' ' || state || ' (' || coalesce(hostname, '-') || ')', ' | ' ORDER BY try_number) FROM task_instance WHERE dag_id = 'tarefa_longa';")"
done

```

Output esperado:
```
15s: tentativa 1 running (worker2)
30s: tentativa 1 running (worker2)
45s: tentativa 1 running (worker2)
60s: tentativa 1 failed (worker2) | tentativa 2 up_for_retry (-)
75s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
90s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
105s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
120s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
135s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
150s: tentativa 1 failed (worker2) | tentativa 2 running (worker1)
165s: tentativa 1 failed (worker2) | tentativa 2 success (worker1)
180s: tentativa 1 failed (worker2) | tentativa 2 success (worker1)
```

1. O processo da task morreu junto com o worker, sem avisar ninguém: para o banco, ela continuou `running`.
2. A task deixou de enviar heartbeats. Depois de `task_instance_heartbeat_timeout` (60 s neste laboratório, 300 s por padrão), o scheduler a declarou perdida e a marcou `failed`.
3. Como ainda havia 1 retry, uma **nova tentativa** foi criada e entregue ao broker, e o worker que sobreviveu a executou do começo.

A task recomeçou do zero, e por isso **tasks precisam ser idempotentes**: a tentativa 1 pode ter feito parte do trabalho antes de morrer. Religue o worker:

```sh
# 8.b.2
svc-start $WORKER

```

Output esperado:
```
[worker2] Iniciado (PID 2451, log: /var/log/lab/worker2.log).
```

> **E o `svc-stop` (SIGTERM)?** Um worker Celery que recebe `SIGTERM` faz um *warm shutdown*: para de pegar tasks novas e espera as que estão em execução terminarem. É assim que se tira um worker de operação para manutenção sem perder trabalho.

## 9. O API Server cai

No Airflow 2, o webserver era "só a interface": se ele caísse, as tasks continuavam rodando. No Airflow 3 isso mudou. As tasks em execução falam com o **API Server** pela Task Execution API: é por ela que enviam heartbeats, leem Connections e Variables, gravam XComs e informam o resultado.

### 9.a. Parando o API Server com uma task em execução

```sh
# 9.a.1
airflow dags trigger tarefa_longa >/dev/null
sleep 20
svc-stop api-server
sleep 90
RUN_ID=$(airflow dags list-runs tarefa_longa -o json 2>/dev/null | jq -r '.[0].run_id')
psql -d airflow -c "SELECT try_number, state, hostname FROM task_instance WHERE dag_id = 'tarefa_longa' AND run_id = '$RUN_ID' ORDER BY try_number;"

```

Output esperado:
```
[api-server] Parado (PID 132).
 try_number | state  | hostname
------------+--------+----------
          1 | failed | worker2
          2 | failed |
(2 rows)
```

A tentativa 1 perdeu a comunicação com o API Server e falhou. A tentativa 2 (o retry) foi entregue a um worker, mas não conseguiu nem começar: sem a Task Execution API, o worker não tem como iniciar a task. As tentativas acabaram e o DAG run terminou como `failed`.

### 9.b. Recuperando

Religar o API Server não ressuscita o que falhou. Depois de religar, limpe (*clear*) as tasks afetadas para que rodem de novo:

```sh
# 9.b.1
svc-start api-server
airflow tasks clear tarefa_longa --only-failed --yes >/dev/null 2>&1
aguardar-dag tarefa_longa

```

Output esperado:
```
[api-server] Iniciado (PID 2672, log: /var/log/lab/api-server.log).
[aguardar-dag] DAG tarefa_longa registrado.
[aguardar-dag] Execucoes de tarefa_longa:
  manual__2026-11-27T15:24:58.798267+00:00  success
  manual__2026-11-27T15:28:27.296526+00:00  success
```

> **Consequência para produção:** o API Server agora é um componente crítico para a **execução**, não só para a interface. Em produção, ele roda com várias réplicas atrás de um balanceador, e o `execution_api_server_url` dos workers aponta para esse balanceador.

## 10. O broker cai

### 10.a. Sem Redis, o scheduler não consegue entregar tasks

```sh
# 10.a.1
svc-stop redis
airflow dags trigger fila_pesada >/dev/null
sleep 30
RUN_ID=$(airflow dags list-runs fila_pesada -o json 2>/dev/null | jq -r '.[0].run_id')
psql -d airflow -c "SELECT task_id, state FROM task_instance WHERE dag_id = 'fila_pesada' AND run_id = '$RUN_ID' ORDER BY task_id;"
grep -h 'reported that the task instance' /var/log/lab/scheduler*.log | tail -1 | cut -c1-160

```

Output esperado:
```
[redis] Parado (PID 94).
 task_id |      state
---------+-----------------
 leve    | failed
 pesada  | upstream_failed
(2 rows)

2026-11-27T15:32:11.401586Z [error    ] Executor CeleryExecutor(parallelism=16) reported that the task instance <TaskInstance: fila_pesada.leve manual__2026-11-
```

O scheduler tentou publicar a task `leve` no Redis, o executor informou a falha e a task foi marcada `failed` (sem retries, porque o DAG `fila_pesada` não os define). A `pesada` ficou `upstream_failed`. Os workers, enquanto isso, ficam tentando reconectar:

```sh
# 10.a.2
grep -h 'Cannot connect' /var/log/lab/worker1.log | tail -1 | cut -c1-140

```

Output esperado:
```
2026-11-27T15:32:37.804769Z [error    ] consumer: Cannot connect to ***: Error 111 connecting to localhost:6379. Connection refused..
```

### 10.b. Recuperando

```sh
# 10.b.1
svc-start redis
sleep 10
airflow tasks clear fila_pesada --only-failed --yes >/dev/null 2>&1
airflow tasks clear fila_pesada --only-running --yes >/dev/null 2>&1
aguardar-dag fila_pesada | tail -1

```

Output esperado:
```
[redis] Iniciado (PID 3065, log: /var/log/lab/redis.log).
  manual__2026-11-27T15:32:10.648271+00:00  success
```

O Redis deste laboratório não persiste nada em disco (`--save ''`): mensagens que estivessem na fila quando ele caiu teriam sido perdidas. Em produção, o broker tem persistência ou replicação, e as tasks têm retries para absorver falhas curtas.

---

# Parte 4 — Operação

## 11. Observabilidade: saúde, logs e métricas

### 11.a. Logs dos componentes e das tasks

Cada serviço escreve o seu log em `/var/log/lab/<serviço>.log`. Os logs das **tasks** ficam em `$AIRFLOW_HOME/logs`, um arquivo por tentativa. Os da task `tarefa_longa`, da seção 8:

```sh
# 11.a.1
ls $AIRFLOW_HOME/logs/dag_id=tarefa_longa/*/task_id=processar_lote/ | head -8

```

Output esperado:
```
/home/barbosa/airflow/logs/dag_id=tarefa_longa/run_id=manual__2026-11-27T15:24:58.798267+00:00/task_id=processar_lote/:
attempt=1.log
attempt=2.log

/home/barbosa/airflow/logs/dag_id=tarefa_longa/run_id=manual__2026-11-27T15:28:27.296526+00:00/task_id=processar_lote/:
attempt=1.log
attempt=2.log
attempt=3.log
```

Neste laboratório, todos os componentes compartilham o mesmo disco. Num cluster de verdade, cada worker grava os logs no próprio disco e os serve por HTTP (porta `8793`, o *log server* do worker) para o API Server mostrar na UI; por isso, em produção, é comum configurar um **remote logging** (S3, GCS, Elasticsearch...) para não depender de o worker ainda existir.

### 11.b. Métricas StatsD

O Airflow emite métricas no protocolo **StatsD** (pacotes UDP) para o endereço configurado em `[metrics]` (aqui, `localhost:8125`). Normalmente quem escuta é um agente (StatsD exporter do Prometheus, Datadog...). Escute por 15 segundos com um pequeno programa Python, enquanto um DAG roda:

```sh
# 11.b.1
airflow dags trigger distribuicao >/dev/null
python3 -c '
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 8125)); s.settimeout(1)
nomes, fim = set(), time.time() + 15
while time.time() < fim:
    try:
        for linha in s.recv(4096).decode().splitlines():
            nomes.add(linha.split(":")[0])
    except socket.timeout:
        pass
print("\n".join(sorted(n for n in nomes if "." in n)[:20]))
'

```

Output esperado:
```
airflow.asset.orphaned
airflow.connection_test.active
airflow.connection_test.pending
airflow.dag.distribuicao.trabalho.duration
airflow.dag_processing.file_path_queue_size
airflow.dag_processing.file_path_queue_update_count
airflow.dag_processing.import_errors
airflow.dag_processing.last_duration
airflow.dag_processing.last_duration.dags-folder.cluster_lab
airflow.dag_processing.processes
airflow.dag_processing.total_parse_time
airflow.dag_processor_heartbeat
airflow.dagbag_size
airflow.dagrun.dependency-check
airflow.dagrun.dependency-check.distribuicao
airflow.dagrun.duration.success
airflow.dagrun.duration.success.distribuicao
airflow.executor.open_slots
airflow.executor.open_slots.CeleryExecutor
airflow.executor.queued_tasks
```

Algumas métricas que um operador acompanha:

| Métrica | O que indica |
|---|---|
| `scheduler_heartbeat` | O scheduler está vivo |
| `executor.open_slots`, `executor.queued_tasks`, `executor.running_tasks` | Capacidade e ocupação do executor |
| `pool.open_slots.<pool>` | Slots livres por Pool |
| `dag_processing.total_parse_time` | Tempo para ler todos os DAGs (código de topo pesado aparece aqui) |
| `ti.finish.<dag>.<task>.<estado>` | Tasks concluídas por estado |
| `dagrun.duration.success.<dag>` | Duração das execuções |

## 12. Usuários, papéis e a API REST

### 12.a. O SimpleAuthManager

Este laboratório usa o *auth manager* padrão do Airflow 3, o **SimpleAuthManager**, com dois usuários definidos em `[core] simple_auth_manager_users`: `admin` (papel `admin`) e `operador` (papel `viewer`, só leitura). As senhas ficam no arquivo apontado por `simple_auth_manager_passwords_file`. Para ambientes com cadastro de usuários pela UI, grupos e integração com LDAP/OAuth, o Airflow oferece o *FAB auth manager* (provider `fab`) e integrações como Keycloak.

```sh
# 12.a.1
airflow config get-value core simple_auth_manager_users
cat $AIRFLOW_HOME/senhas.json

```

Output esperado:
```
admin:admin,operador:viewer
{"admin": "admin", "operador": "operador"}
```

Faça login na UI (`http://<IP-PUBLICO>:8081`) como `operador` / `operador`: você vê tudo, mas os botões de disparar, pausar e limpar não funcionam.

### 12.b. Token JWT e a API REST

A API REST (`/api/v2`) exige um token, obtido em `/auth/token`:

```sh
# 12.b.1
TOKEN=$(curl -s -X POST http://localhost:8081/auth/token -H 'Content-Type: application/json' -d '{"username": "admin", "password": "admin"}' | jq -r .access_token)
curl -s -o /dev/null -w 'sem token: HTTP %{http_code}\n' http://localhost:8081/api/v2/dags
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8081/api/v2/dags | jq -c '[.dags[] | {dag_id, is_paused}]'

```

Output esperado:
```
sem token: HTTP 401
[{"dag_id":"carga","is_paused":false},{"dag_id":"distribuicao","is_paused":false},{"dag_id":"fila_pesada","is_paused":false},{"dag_id":"tarefa_longa","is_paused":false}]
```

Agora, a mesma operação de escrita (pausar um DAG) com cada usuário:

```sh
# 12.b.2
TOKEN_OP=$(curl -s -X POST http://localhost:8081/auth/token -H 'Content-Type: application/json' -d '{"username": "operador", "password": "operador"}' | jq -r .access_token)
curl -s -o /dev/null -w 'operador pausando o DAG carga: HTTP %{http_code}\n' -X PATCH -H "Authorization: Bearer $TOKEN_OP" -H 'Content-Type: application/json' -d '{"is_paused": true}' http://localhost:8081/api/v2/dags/carga
curl -s -o /dev/null -w 'admin pausando o DAG carga: HTTP %{http_code}\n' -X PATCH -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"is_paused": true}' http://localhost:8081/api/v2/dags/carga
curl -s -o /dev/null -w 'admin despausando o DAG carga: HTTP %{http_code}\n' -X PATCH -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"is_paused": false}' http://localhost:8081/api/v2/dags/carga

```

Output esperado:
```
operador pausando o DAG carga: HTTP 403
admin pausando o DAG carga: HTTP 200
admin despausando o DAG carga: HTTP 200
```

`401` = não autenticado (sem token); `403` = autenticado, mas sem permissão. A mesma API é a que a UI usa e a que ferramentas de automação (CI/CD, scripts de operação) devem usar, no lugar de acessar o banco diretamente. Toda ação executada fica registrada na tabela `log`, com o usuário que a fez (a tentativa recusada do `operador` não aparece):

```sh
# 12.b.3
psql -d airflow -c "SELECT date_trunc('second', dttm) AS quando, owner AS usuario, event, dag_id FROM log WHERE dag_id = 'carga' AND event LIKE '%patch%' ORDER BY dttm DESC LIMIT 3;"

```

Output esperado:
```
         quando         | usuario |   event   | dag_id
------------------------+---------+-----------+--------
 2026-11-27 15:45:48+00 | admin   | patch_dag | carga
 2026-11-27 15:45:48+00 | admin   | patch_dag | carga
(2 rows)
```

## 13. Manutenção do banco de metadados

### 13.a. Backup

O banco de metadados é o estado do Airflow: histórico, Connections, Variables, Pools. O backup é um backup comum de PostgreSQL:

```sh
# 13.a.1
mkdir -p ~/project/backup
pg_dump -d airflow -Fc -f ~/project/backup/airflow-$(date +%Y%m%d).dump
ls -lh ~/project/backup/
pg_restore -l ~/project/backup/airflow-*.dump | grep -c 'TABLE DATA'

```

Output esperado:
```
total 176K
-rw-r--r-- 1 barbosa barbosa 174K Nov 27 15:45 airflow-20261127.dump
62
```

### 13.b. Limpeza do histórico: `airflow db clean`

Com o tempo, tabelas como `log`, `task_instance`, `dag_run` e `xcom` crescem sem parar e deixam o scheduler e a UI lentos. O `airflow db clean` apaga (ou arquiva) registros anteriores a uma data. Sempre comece com `--dry-run`:

```sh
# 13.b.1
airflow db clean --clean-before-timestamp "$(date -u -d '+1 day' +%Y-%m-%d)" --tables log --dry-run 2>&1 | grep -E 'Found|Checking'

```

Output esperado:
```
Checking table log
Found 183 rows meeting deletion criteria.
```

Sem o `--dry-run` (e com `--yes`), os registros são movidos para tabelas de arquivo (`_airflow_deleted__...`) e removidos da tabela original; `airflow db export-archived` exporta esse arquivo e `airflow db drop-archived` o descarta. Em produção, a limpeza roda periodicamente (muitas vezes, como um DAG de manutenção), mantendo algumas semanas de histórico.

## 14. Do laboratório à produção

Neste laboratório, os componentes são **processos** num único container. Numa instalação real, a separação é a mesma, mas cada componente é um container (Docker Compose) ou um conjunto de *pods* (Kubernetes, com o Helm chart oficial do Airflow):

![Equivalência entre o laboratório, o docker-compose oficial e o Helm chart: cada serviço do svc-status corresponde a um container no Compose e a um Deployment ou StatefulSet no Kubernetes](diagramas/04-laboratorio-vs-producao.svg)

| Laboratório (`svc-status`) | `docker-compose.yaml` oficial | Helm chart oficial (Kubernetes) |
|---|---|---|
| `postgres` | `postgres` | banco externo gerenciado (recomendado) ou subchart PostgreSQL |
| `redis` | `redis` | StatefulSet `redis` |
| `api-server` | `airflow-apiserver` | Deployment `api-server` (várias réplicas + Service) |
| `scheduler1`, `scheduler2` | `airflow-scheduler` (escalável com `--scale`) | Deployment `scheduler` com `replicas: 2` |
| `dag-processor` | `airflow-dag-processor` | Deployment `dag-processor` |
| `triggerer` | `airflow-triggerer` | StatefulSet `triggerer` |
| `worker1`, `worker2` | `airflow-worker` | StatefulSet `worker` (ou KEDA para escalar pela fila) |
| `flower` | `flower` (perfil opcional) | Deployment `flower` (opcional) |
| *(build da imagem)* | `airflow-init`: `db migrate` + usuário inicial | Job `migrate-database` a cada `helm upgrade` |

O que muda na passagem para produção:

- **Uma imagem para todos.** Todos os componentes usam a mesma imagem do Airflow (com os providers e bibliotecas dos DAGs), mudando só o comando (`api-server`, `scheduler`, `celery worker`...). O `airflow-init` do Compose e o *job* de migração do Helm rodam o `airflow db migrate` antes de os componentes subirem.
- **Configuração por variáveis de ambiente**, iguais em todos os containers (seção 2.b). No `docker-compose.yaml` oficial, um bloco comum (`x-airflow-common`, com âncoras YAML `&`/`*`) define as variáveis `AIRFLOW__...` uma vez e é reaproveitado por todos os serviços.
- **Os DAGs chegam aos componentes** por um volume compartilhado, por um *git-sync* (um container auxiliar que sincroniza um repositório Git) ou embutidos na imagem.
- **Healthchecks** usam os mesmos comandos deste roteiro: `/api/v2/monitor/health` para o API Server, `airflow jobs check --job-type SchedulerJob --hostname "$HOSTNAME"` para schedulers, DAG Processor e Triggerer, `celery inspect ping` para os workers.
- **Logs remotos** (S3, GCS, Elasticsearch), **métricas** (StatsD/OpenTelemetry → Prometheus) e **segredos** num *secrets backend* (AWS Secrets Manager, Vault) em vez de Connections no banco.
- **Executores alternativos.** No **KubernetesExecutor**, não há broker nem workers fixos: cada task vira um *pod*. Também é possível combinar executores (por exemplo, Celery para tasks rápidas e Kubernetes para tasks pesadas) com a opção `executor` por task, recurso do Airflow 3.

---

# Apêndices

## A. Referência rápida de comandos

### Serviços do cluster

| Comando | Descrição |
|---|---|
| `svc-status` | Estado de todos os serviços |
| `svc-stop <serviço\|all>` | Para o serviço de forma limpa (SIGTERM) |
| `svc-kill <serviço>` | Mata o serviço abruptamente (SIGKILL) |
| `svc-start <serviço\|all>` / `svc-restart <serviço\|all>` | Inicia / reinicia |
| `aguardar-dag <dag_id>` | Espera as execuções do DAG terminarem e mostra o estado de cada uma |
| `/var/log/lab/<serviço>.log` | Log de cada serviço |

### Diagnóstico

| Comando | Descrição |
|---|---|
| `curl -s localhost:8081/api/v2/monitor/health \| jq` | Saúde do banco, schedulers, DAG Processor e Triggerer |
| `airflow jobs check --job-type SchedulerJob --hostname <nome>` | Um scheduler específico está vivo? |
| `psql -d airflow -c "SELECT ... FROM job"` | Heartbeats de cada componente |
| `psql -d airflow -c "SELECT ... FROM task_instance"` | Estado, worker e fila de cada tentativa |
| `redis-cli llen <fila>` | Mensagens esperando numa fila do Celery |
| `airflow config get-value <seção> <chave>` | Valor efetivo de uma opção |
| `airflow pools set <pool> <slots> <descrição>` | Cria ou altera um Pool |
| `airflow tasks clear <dag> --only-failed --yes` | Recoloca as tasks com falha para rodar |
| `airflow db check` / `airflow db clean ... --dry-run` | Banco de metadados |

## B. Encerrando o ambiente

Ao fim de cada aula, pare a instância: no Console AWS (`us-east-1`), **EC2** > **Instances**, selecione a sua instância **`lab-airflow-cluster-<data e hora>`** e clique em **Instance state** > **Stop instance**. Quando o laboratório tiver terminado de vez, use **Instance state** > **Terminate instance**.

## C. Troubleshooting

Rode o diagnóstico completo do ambiente:

```sh
curl -sS https://raw.githubusercontent.com/infobarbosa/airflow-cluster-administration/main/scripts/check.sh | bash

```

Se algum serviço ficou parado depois de um experimento:

```sh
svc-start all

```

Se o `default_pool` ficou reduzido depois da seção 6:

```sh
airflow pools set default_pool 128 "Default pool"

```

## D. Execução local

Para executar localmente no Docker (cerca de 4 GB de RAM livres), em `amd64` ou `arm64` (Mac com Apple Silicon):

```sh
mkdir airflow-cluster-administration && cd airflow-cluster-administration
curl -sSO https://raw.githubusercontent.com/infobarbosa/airflow-cluster-administration/main/compose.yaml
docker compose up -d

```

Ou diretamente:

```sh
docker run -d --name airflow-cluster-lab -p 8080:8080 -p 8081:8081 -p 5555:5555 --stop-timeout 60 ghcr.io/infobarbosa/airflow-cluster-lab-docker-image:latest

```

Acesse a IDE em `http://localhost:8080`, o Airflow em `http://localhost:8081` (`admin` / `admin`) e o Flower em `http://localhost:5555`.

---

## Parabéns!

Você operou um cluster Airflow 3: leu a configuração efetiva, acompanhou tasks do scheduler ao worker, direcionou trabalho com filas, previu a concorrência e, principalmente, viu de perto o que acontece quando cada componente falha e como recuperar. Esse é o dia a dia de quem mantém o Airflow de uma equipe de dados.
