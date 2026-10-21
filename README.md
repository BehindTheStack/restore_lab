# Kafka Restore Lab V8 — Saúde consolidada da reconciliação

Esta V8 **evolui a V7 sem instalar Grafana, Prometheus, Loki ou Uptime Kuma**. Aproveita a GUI existente (`monitor`) em `http://localhost:8082`, Kafbat e Adminer. Preserva o fluxo PostgreSQL/SQL Server → Python → Kafka → PostgreSQL blue-green e suas políticas de DDL/deletes. O painel é **somente leitura**; **não acessa a origem diretamente**.

## Como iniciar uma instalação nova

```bash
cd kafka_restore_lab_v8
docker compose up -d --build
docker compose --profile monitor up -d --build monitor
# http://localhost:8082 — monitor V8
# http://localhost:8080 — Kafbat
# http://localhost:8081 — Adminer
```

O nome do projeto Compose continua `kafka-restore-lab` **intencionalmente**, para evitar que a mudança de versão crie volumes novos e esconda os dados da V7. NÃO execute `docker compose down -v` para atualizar. Execute em ambiente isolado quando estiver testando a V7 e a V8 simultaneamente, usando outro `--project-name` (nesse caso os volumes serão separados).

### Atualizar volumes PostgreSQL existentes da V7, sem perda de dados

O PostgreSQL executa os scripts de `docker-entrypoint-initdb.d` somente na criação do volume. **Obrigatório executar o upgrade antes de subir a V8 do consumer ou monitor em dados antigos.**

```bash
# No diretório V8, com o volume da V7 montado pelo mesmo nome de projeto:
docker compose up -d local-db kafka
# Seguro para repetir; NÃO faz DROP/TRUNCATE e não altera os dados dos contratos.
docker compose exec -T local-db   psql -v ON_ERROR_STOP=1 -U postgres -d lab_local   < sql/monitor_upgrade.sql

# Reconstrói consumer e interface sobre o volume existente.
docker compose up -d --build consumer
docker compose --profile monitor up -d --build monitor
```

Se V7 ainda estiver executando, substitua os containers pelo Compose V8 no mesmo projeto; **não rode duas versões do consumer no mesmo group id** ao mesmo tempo. O upgrade acrescenta duas colunas na auditoria, `replica.range_checkpoints`, permissões SELECT e índices. Não reconstrói blue/green e não toca no remoto. Checkpoints só existem para faixas **promovidas após essa atualização**; execuções passadas não são inventadas para compor a cobertura.

## O que é consolidado na GUI

| Etapa | Sinal | Fonte de evidência | Limitação |
|---|---|---|---|
| Origem | Última extração, falhas e motor PostgreSQL/SQL Server | `replica.sync_runs` | Disponibilidade atual não é sondada |
| Extração/reconciliação | Lotes pendentes, travados e rejeitados, tamanho do intervalo | `sync_runs` e checkpoints | Faixas independentes não são snapshot global |
| Kafka | Broker, tópico de dados e auditoria, consumer group/lag | Metadados de Kafka (somente leitura) | Lag não equivale a alterações pendentes |
| Consumer | Membros, estado de grupo, offsets confirmados | API administrativa Kafka | Não garante integridade sozinho |
| PostgreSQL local | Acessibilidade + contadores de auditoria | PostgreSQL, role `monitor_reader` | Não consulta os 700 GB da tabela ativa a cada atualização |
| Blue-green | Slot ativo, versões BLUE/GREEN, transição/rollback | `active_state`, `slot_state`, `transitions` | Mesma instância, não HA física |
| Integridade/deletes | INSERT/UPDATE/DELETE promovidos, lote bloqueado | `sync_runs`, somente promovidos | Deletes inferidos por ausência na faixa validada |
| DDL | Hash ativo, hash pendente, backfill e dicionários por origem | `schema_state`, `source_catalog_observations` | Detecção do estado atual, não trilha literal de todos ALTER TABLE |
| Cobertura | União dos intervalos numéricos de PK promovidos nas últimas 24h e na linhagem atual | `range_checkpoints` | Não é percentual de linhas, bytes ou garantia de simultaneidade |

A visão ponta a ponta inclui alertas com orientação de diagnóstico, links para Kafbat/Adminer, lista de execuções, página de detalhes `/run/<UUID>`, `/status.json`, `/healthz` (liveness do painel) e `/readyz` (Kafka + PostgreSQL local acessíveis). Tudo por polling HTTP, sem frontend separado e sem stack de telemetria adicional.

**Cobertura de IDs:** o producer registra `sweep_target_max_id` no `start` Kafka. O consumer só grava a faixa em `range_checkpoints` **dentro da mesma transação de promoção blue-green**. Lotes rejeitados/abortados e gerações descartadas por rollback não somam cobertura. Faixas sem limite superior registrado são "não aferidas", nunca 100%. A janela configurável padrão é de 24 h. A união representa o espaço de chaves inteiras entre 1 e o maior ID observado, portanto, bases de IDs esparsos podem mostrar uma porcentagem que não corresponde à proporção de registros conferidos.

**Não prometemos consistência global:** uma sequência de scans de faixas em transações diferentes não comprova a igualdade simultânea de todo o banco. Sem `updated_at`, `deleted_at`, CDC ou log autorizado, alterações que ocorreram depois da leitura de uma faixa dependem de outro ciclo completo para aparecer.

## Configurar expectativas e alertas

Valores do serviço `monitor` em `docker-compose.yml`:

```yaml
LAB_STALLED_MINUTES: "15"              # bloqueio de lote por tempo
LAB_COVERAGE_WINDOW_HOURS: "24"        # janela móvel da cobertura
LAB_MAX_COVERAGE_INTERVALS: "20000"    # limite de query do painel
LAB_MAX_CONSUMER_LAG: "1000"            # limite de mensagens em atraso
LAB_EXPECTED_INTERVAL_MINUTES: "0"     # zero = cronograma NÃO definido
```

**Não habilite um SLA de frescor sem existir agendamento.** O valor `0` é intencional para o laboratório iniciado manualmente. Se houver scheduler confiável, configure o intervalo máximo aceitável, por exemplo `15`; a interface alertará se o tempo desde a última promoção ultrapassar esse valor. Isso não é uma estimativa de RPO do conjunto remoto.

O painel atribui `unknown` quando não há métrica real. Um Kafka acessível sem consumer group/offset confirmado não vira lag zero; origem indisponível mas sem execução recente continua como "não sondada". `overall=operational` significa somente que **os sinais observados não produziram alertas**, não que os dados remotos e locais estejam integralmente iguais. Se o monitor ficar indisponível, o `/healthz` deixa de responder; `/readyz` retorna HTTP 503 caso Kafka ou PostgreSQL local falhem.

## Consultas rápidas

```bash
curl -s http://localhost:8082/status.json
curl -si http://localhost:8082/healthz
curl -si http://localhost:8082/readyz

# Ver evidências de faixas efetivamente promovidas (não de extrações incompletas)
docker compose exec -T local-db psql -U postgres -d lab_local -c   'SELECT run_id,sweep_low,sweep_high,target_max_id,source_engine,version,checked_at FROM replica.range_checkpoints ORDER BY checked_at DESC LIMIT 15;'

# Testes offline (sem Docker)
python tests/check_static.py
python tests/test_monitor.py

# Smoke opcional: depende de ambiente Docker previamente inicializado
LAB_MONITOR_SMOKE=1 bash tests/e2e_monitor.sh
```

## O que conscientemente NÃO foi implementado

- Grafana/Prometheus/Loki/Tempo/Uptime Kuma, exporters, tracing, novo banco de métricas.
- Probes ativos da origem, para não acrescentar carga nem credenciais remotas ao monitor. Esse indicador permanece `unknown` quando faltam evidências.
- Alarmes push por e-mail/Slack, controle de correções, replay pelo navegador, ou qualquer permissão de escrita.
- Percentual de consistência dos 700 GB, RPO global derivado de última promoção ou latência artificial entre banco remoto e local.

---

## Histórico e documentação preservada da V7

# Kafka Restore Lab V7 — Observabilidade mínima sem Grafana

**Decisão:** a V6 já dispõe de Kafbat UI (tópicos, brokers, consumidores e lag), Adminer (SQL e auditoria), logs JSON do Docker e Toxiproxy (testes de resiliência). **Não foram adicionados** Grafana, Prometheus, Loki, Uptime Kuma, exporters, séries temporais nem banco de dados novo.

A única lacuna operacional era **conferir em uma GUI, sem executar consultas manualmente, se o fluxo de reconciliação está progredindo e se uma geração foi promovida com sucesso**. Esta V7 acrescenta um painel HTTP **opcional, somente de leitura**. O pipeline de sincronização, consumidores, reconciliação DDL e blue-green da V6 não foi modificado.

## GUI disponíveis

| Endereço (host local) | Função | Situação |
|---|---|---|
| http://localhost:8080 | Kafbat UI: tópicos, mensagens, consumer groups, offset lag | Já existia |
| http://localhost:8081 | Adminer: banco local, SQL de auditoria, histórico DDL | Já existia |
| http://localhost:8082 | **Monitor V7**: última promoção, versão/slot, falhas, lotes pendentes, DDL pendente e broker | **Novo, opt-in** |

A GUI V7 consulta `replica.active_state`, `replica.transitions`, `replica.sync_runs` e `replica.schema_state` com **`monitor_reader` (somente SELECT)**; verifica o tópico de dados Kafka. **Nunca** consulta o remoto, executa reparos, aplica DDL, altera offsets ou acessa o Docker socket. Sem credenciais de administração do PostgreSQL no painel.

## Inicialização com bancos novos

```bash
cd kafka_restore_lab_v7
docker compose up -d --build
docker compose run --rm cli chaos init
# Opcional: sobe apenas o monitor, sem novos stacks
docker compose --profile monitor up -d --build monitor
# GUI de status: http://localhost:8082
```

### Migração da V6 para V7 sem perder dados

Se **já possui volume V6** inicializado, o PostgreSQL NÃO reexecuta `sql/local.sql`. Aplique **uma única vez** a concessão mínima (idempotente):

```bash
# Use o diretório e banco local da V7 após migrar os volumes, ou adapte ao
# projeto Compose existente que já mantém os volumes. NÃO use down -v.
docker compose exec -T local-db psql -U postgres -d lab_local < sql/monitor_upgrade.sql
docker compose --profile monitor up -d --build monitor
```

**Atenção:** como o Compose usa `name: kafka-restore-lab` neste pacote, o prefixo dos volumes muda em relação à V6. Isso cria bancos novos por padrão. Para manter os volumes existentes, ajuste o nome do projeto Compose para o nome usado na V6 (`kafka-restore-lab-v6`) **antes de iniciar a V7**, ou faça a migração de volumes explicitamente. Faça backup antes; não use `docker compose down -v`.

Exemplo seguro para reusar o projeto e os volumes V6, com o arquivo V7:

```bash
# Dentro de kafka_restore_lab_v7, sem modificar o projeto V6
docker compose -p kafka-restore-lab-v6 exec -T local-db \
  psql -U postgres -d lab_local < sql/monitor_upgrade.sql
docker compose -p kafka-restore-lab-v6 --profile monitor up -d --build monitor
```

A opção `-p` substitui o `name:` do Compose para apontar aos volumes do projeto V6. Confirme previamente que não há outra instância usando esse nome.

## Endpoints do monitor

- `/` — dashboard HTML que atualiza a cada 15 segundos.
- `/status.json` — o mesmo estado em JSON, útil para testes manuais.
- `/healthz` — prontidão do próprio painel e conectividade com o PostgreSQL local; **não** é certificação de consistência de dados.

O monitor reporta `operational`, `attention`, `not_initialized` e `unavailable`. `attention` é acionado por lotes travados por mais de `LAB_STALLED_MINUTES=15`, migração DDL pendente ou falha/rejeição nas últimas 24 horas. Não emite alertas externos. A GUI continua aberta mesmo que o Kafka esteja indisponível, desde que o PostgreSQL local responda.

## O que o monitor NÃO comprova

- **Broker online ≠ consumer em dia.** Verifique *consumer lag* e membros do grupo `lab-range-bluegreen-v4` no Kafbat UI.
- **Promoção recente ≠ cobertura da origem inteira.** Com varredura por faixas sem CDC e sem `updated_at`, alterações só aparecem após a respectiva faixa ser examinada. O painel exibe `next_sweep_id`, mas a V6 ainda não mantém um indicador formal de cobertura completa/ciclo concluído.
- **Banco local acessível ≠ origem acessível.** O monitor evita consultas à fonte remota para não amplificar degradações. Erros de origem aparecem pelo histórico de execução.
- Não há monitoramento de CPU/memória por serviço, alertas automáticos de SLA nem histórico temporal de métricas. Se o laboratório evoluir para execução contínua com SLO/RPO formal, avaliar Prometheus+Grafana+Alertmanager, sem instalar preventivamente.

## Segurança e limites

- A UI não tem autenticação: **exposta somente em `127.0.0.1:8082` no Docker Compose**. Não publique esta porta em redes corporativas. Para produção, seria necessária autenticação/TLS e secrets reais.
- Senha `monitor-reader` é **somente de fixture** e deve ser trocada fora do laboratório.
- Não exponha Kafbat/Adminer publicamente; eles continuam vinculados a `127.0.0.1`.
- As leituras do monitor são leves e limitadas às últimas 12 execuções (as contagens agregadas usam índices/tabelas locais).

## Validar

```bash
python -m py_compile app/monitor.py
python tests/test_monitor.py
python tests/check_static.py
# Teste integrado com Docker (requer daemon):
curl -fsS http://localhost:8082/status.json
curl -fsS http://localhost:8082/healthz
```

**Validação neste ambiente:** estática e testes unitários; não foi possível executar Compose e testar integração em banco e Kafka reais.

---

## Documentação da V6 (preservada)

# Kafka Restore Lab V6 — SQL Server ou PostgreSQL → Kafka → PostgreSQL

**Implementação experimental: SQL Server passou a ser origem de DADOS, e não somente origem de catálogo DDL.** A V6 preserva o reconciliador PostgreSQL da V5 e acrescenta um extrator SQL Server com conta de leitura, comparação canônica Python, Kafka e blue-green PostgreSQL. O ambiente é **laboratório descartável, não homologado para banco de 700 GB/produção**.

## 1. Novidades V6

| Função | PostgreSQL remoto | SQL Server remoto |
|---|---|---|
| Metadados estruturais | `pg_catalog` | `sys.*` (histórico JSONB) |
| Bootstrap inicial | Sim | **Sim** |
| Varredura por chave `BIGINT` sem `updated_at` | Sim | **Sim** |
| Detecção de INSERT / UPDATE | SHA-256 PG | **Canonical SHA-256 em Python** |
| Hard DELETE | Ausência em faixa COMPLETA | **Ausência em faixa COMPLETA** |
| Kafka de dados | `lab.sync.events.v6` | Mesmo tópico |
| Blue-green/rollback | Duas tabelas e journal | Mesmo consumidor |
| DDL inesperado | Gate V5, migração PG aditiva | **Fail closed, observação em JSONB; sem migração automática** |
| Tolerância a falhas | Toxiproxy PostgreSQL | **Toxiproxy SQL Server** |

**Limite irredutível:** sem timestamp, CDC ou tracking de mudanças, o produtor SQL Server precisa **ler o conteúdo de cada faixa** para saber o que mudou. Somente os registros divergentes seguem como payload Kafka; eventos leves de presença (`seen`) são emitidos para detectar deletes. Isso reduz o tráfego Kafka, **não** o custo de leitura/rede entre SQL Server e extrator. A convergência depende de cobrir todas as faixas.

## 2. Subir o experimento SQL Server → Kafka → PostgreSQL

Requisitos: Docker Compose, CPU compatível com SQL Server 2022, memória disponível (SQL Server requer vários GiB). A V6 usa os dados falsos criados no contêiner; **nunca aponte os comandos de fixture para uma instância real**.

```bash
cd kafka_restore_lab_v6

# SQL Server, PostgreSQL local, Kafka, consumer, rede simulada e UI.
docker compose --profile mssql --profile mssql-tools up -d --build \
  remote-sqlserver remote-db local-db kafka toxiproxy consumer kafbat adminer

# Fixture descartável: cria SQL Server lab_remote, 100 contratos e conta SELECT.
# Somente este processo usa sa. Inclui ALLOW_SNAPSHOT_ISOLATION ON.
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture init

# Faz a carga inicial, uma única vez.
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli bootstrap

# Verifica promoções após o consumer terminar de processar.
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli status
```

Gere alterações sintéticas:

```bash
# Os três comandos abaixo usam SA exclusivamente no contêiner de fixture.
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture mutate-insert
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture mutate-update
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture mutate-delete

# Agora apenas compara IDs 1 a 200, publica diferenças e presença de IDs.
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  incremental --from-id 1 --sweep-width 200

# Status: inseridos, atualizados, removidos, erro, geração ativa.
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli status
```

Kafbat UI `http://localhost:8080`; Adminer `http://localhost:8081` (`local-db`, db `lab_local`, user `postgres`, password `lab-admin`). Compare `public.contracts` com `replica.blue_contracts`, `replica.green_contracts`, `replica.sync_runs`, `replica.transitions` e `replica.source_catalog_observations`.

A promoção é feita pelo consumer em outra execução: `status` pode mostrar o lote `loading` até o evento `end` ser validado. A versão pública não muda antes disso.

## 3. Testar rede degradada e DDL

```bash
# Inicializar proxy SQL Server (pode ser chamado mais de uma vez):
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli chaos init --target sqlserver

# Injetar 300ms de latência:
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli chaos latency 300 --target sqlserver

# Restaurar conectividade normal:
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli chaos clear --target sqlserver

# Derrubar conexão remota (próximo incremental não deve promover):
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli chaos down --target sqlserver

# Reparar:
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli chaos up --target sqlserver
```

A documentação/alterações de DDL podem ser vistas no banco local. Teste com fixture:

```bash
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture add-column
# Observa a nova versão, registrando seu hash e diff, sem migrar o destino:
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  catalog observe --engine sqlserver --source-key mssql-dbo-contracts
# Nova leitura deve BLOQUEAR por schema drift; versão local permanece disponível:
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli incremental \
  --from-id 1 --sweep-width 200
```

## 4. Garantias, pré-requisitos e limitações

1. **Transação SNAPSHOT obrigatória.** O leitor consulta `sys.databases.snapshot_isolation_state` e recusa iniciar se a opção `ALLOW_SNAPSHOT_ISOLATION` estiver desabilitada. O SQL Server do lab a habilita via SA **somente no bootstrap da fixture**. Uma origem corporativa real precisaria já oferecê-la, ou exigiria negociação com o DBA. `READ COMMITTED`, mesmo com RCSI, não substitui uma visão transacional consistente entre múltiplas consultas. Não usamos `NOLOCK`.
2. **Acesso remoto mínimo.** O produtor usa `remote_reader` com `GRANT SELECT` e visibilidade dos metadados. A aplicação jamais envia `ALTER TABLE`, `CREATE`, trigger, CDC ou qualquer DDL para a origem. O usuário `sa` existe apenas no fixture de teste separado.
3. **Contrato de dados estrito.** A V6 replica **uma tabela** `dbo.contracts` com cinco campos (`id BIGINT`, `contract_code NVARCHAR`, `amount DECIMAL(14,2)`, `status NVARCHAR`, `description NVARCHAR`) para `public.contracts`. A PK deve ser `id` com valores inteiros positivos; `IDENTITY` no próprio `id` é permitido. SQL Server JSON/XML, FLOAT, DATETIMEOFFSET, GUID/PK composta, schemas diferentes e múltiplas tabelas **ainda não estão implementados** na replicação de registros (embora o observador de metadados conheça outros tipos).
4. **Canonicalização conservadora.** O SHA-256 é calculado em Python usando UTF-8, JSON estável, `DECIMAL` com duas casas e comparação literal de strings; `NULL` e string vazia são diferentes; alteração de collation ou tipo nativo bloqueia ciclos posteriores. Não se faz trim, uppercase/lowercase ou normalização Unicode que possa esconder drift.
5. **Diferenças e deletes.** O extrator lê todos os registros da faixa via SQL Server, compara com o slot ativo local e emite Kafka `delta` para diferença. Emite `seen` para cada ID da faixa; o consumer só remove ID não visto após conferir hash/contagem, fim do lote e a política `MAX_REMOVE_FRACTION`. Falhas no meio da extração não promovem deletes.
6. **Blue-green transacional.** O consumer e o journal V5 são reutilizados, inclusive retenção limitada de versão anterior para rollback. São *duas tabelas na mesma instância* PostgreSQL, não dois clusters. Para 700 GB, o tamanho local (duas cópias + índices + WAL) e o custo de replay do journal precisam ser medidos.
7. **DDL SQL Server ainda fail-closed.** A V6 observa `sys.*`, armazena dicionários JSONB por versão e bloqueia qualquer alteração nativa inesperada, inclusive alteração de comprimento. Não inclui aprovação de `ADD COLUMN` vindo do SQL Server; essa evolução exigirá mapeamento de tipos e backfill próprios.
8. **Isolamento de Kafka.** Um único tópico de dados com partição única preserva a ordem de `start`, `seen`, `delta`, `end`; não há paralelismo horizontal de milhares de faixas. O Kafka não faz backup do SQL Server por conta própria, e o histórico Kafka não é substituto para backups físicos.
9. **Volume de 700 GB.** `--sweep-width` permite uma janela de até `MAX_SQLSERVER_SWEEP_WIDTH` (= 100.000 por padrão). Observe `remote_payload_json_bytes` (bytes da projeção JSON em Python, **não** bytes reais do TDS) e `kafka_bytes` para medir diferenças. O bootstrap completo implica leitura integral, assim como a cobertura eventual de todas as faixas. Não execute testes volumosos em origem sem janela e limites acordados.
10. **DDL concorrente.** Um segundo catálogo é observado antes da emissão do evento de fechamento. Se o DDL mudar durante a transação, a leitura falha ou aborta. `pg_catalog` e `sys.*` não são um log histórico de alterações DDL.

## 5. Testes

```bash
# Testes offline; não executam containers:
python tests/check_static.py
python tests/test_reconcile.py
python tests/test_generation_paths.py
python tests/test_schema_contract.py
python tests/test_source_catalog.py
python tests/test_mssql_contract.py
python tests/test_mssql_paths.py

# End-to-end: APAGA volumes Kafka/PostgreSQL/SQL Server deste Compose!
LAB_RESET=1 bash tests/e2e_sqlserver_sync.sh
```

O E2E testa bootstrap, INSERT, UPDATE, hard DELETE, promoção blue-green, hashes, Unicode e bloqueio de DDL. Em ambiente sem Docker, apenas os testes offline são executáveis. **Não há comprovação de execução integrada Docker/SQL Server neste pacote.**

---

## Apêndice: documentação V5 (origem PostgreSQL + schema evolution)

# Kafka Restore Lab V5 — Schema Evolution + PostgreSQL / SQL Server Catalog

**Laboratório experimental, não homologado em produção.** A V5 mantém a V4 (reconciliação por faixas de chave primária, detecção de INSERT / UPDATE / hard DELETE, Kafka e blue-green) e acrescenta um mecanismo de evolução de DDL **com aprovação humana**. Também inclui um observador de metadados multiplataforma capaz de ler os catálogos do SQL Server e PostgreSQL e registrar dicionários de schema versionados no PostgreSQL local.

## Escopo executável

| Recurso | PostgreSQL remoto | SQL Server remoto |
|---|---|---|
| Observação de colunas, tipos, PK, índices, constraints | Sim, `pg_catalog` | Sim, `sys.*` |
| Dicionários nativo e lógico, SHA-256, diff, histórico JSONB | Sim | Sim |
| Metadados observados com conta exclusivamente de leitura | Sim | Sim, sujeita à visibilidade de metadados |
| Publicação de evidência em tópico Kafka de auditoria | Sim | Sim |
| Reconciliar dados por faixa com hashes + Kafka | Sim | **Não nesta versão** |
| Bootstrap / UPSERT / hard DELETE / blue-green de dados | Sim | **Não nesta versão** |
| Detectar mudanças de DDL e bloquear sincronização de dados | Sim | Detecta; integração com gate de dados pendente |
| Aprovar `ADD COLUMN TEXT NULL` + backfill + ativar view | Sim | **Não nesta versão** |

**Não confundir:** o catálogo SQL Server está implementado para DDL, mas `bootstrap`, `incremental` e `schema apply/backfill/activate` continuam PostgreSQL→PostgreSQL. Para o SQL Server se tornar fonte de dados, será necessário implementar outro adaptador para leitura de faixas, transações/snapshot, normalização de valores e fingerprints comparáveis com os do PostgreSQL. Trocar somente a string de conexão não funciona.

## 1. Arquitetura

```text
            PostgreSQL remoto             SQL Server remoto (opcional)
               pg_catalog                        sys.*
                    \                            /
                     Schema Observer (source_catalog.py)
                                │
                         native_manifest
                                │
              normalização conservadora + dicionário
                                │
                   native_hash / logical_hash
                                │
                    Diff por nomes de colunas
                                │
         PostgreSQL local: source_catalog_observations (JSONB)
                                │
                     Kafka audit: SOURCE_DDL_OBSERVED

PostgreSQL REMOTO (somente SELECT)
          │
          ├─ V4 range scan: chaves + SHA-256 + dados divergentes
          │                     │
          └─ V5 schema gate     │
                    │           ▼
               APP DDL     Kafka data topic
                    │           │
              APROVAÇÃO         ▼
               EXPLÍCITA   Consumer + blue-green
                    │           │
          ADD COLUMN TEXT NULL em ambos os slots locais
                    │
             Backfill por faixas via Kafka
                    │
             Verificação das contagens e hashes
                    │
             Ativação transacional da public.contracts
```

**A origem permanece read-only para o middleware.** Apenas o PostgreSQL local recebe `ALTER TABLE`; os scripts que alteram a origem são fixtures descartáveis, isolados em serviços Docker com credenciais administrativas que não são expostas ao extrator/consumer.

### Dicionários estruturais

O `source_catalog.py` armazena **duas representações** por observação:

- `native_manifest`: nomes, ordem, tipos nativos, precisão/escala, NULL, defaults, collation, PK, índices e constraints (conforme disponibilidade dos metadados).
- `logical_manifest`: `columns` indexado pelo nome, tipos lógicos, PK, ordem, índices e constraints. Serve para comparação rápida e portabilidade. A transformação é conservadora, não perde o contrato nativo.

`native_hash` detecta alterações específicas do banco, por exemplo `NVARCHAR(30)` → `NVARCHAR(100)` ou mudança de collation. `logical_hash` mede mudanças no contrato lógico, por exemplo `ADD COLUMN`, `DROP`, `NULL` → `NOT NULL`, `DECIMAL(14,2)` → `DECIMAL(16,4)`.

Alterações detectadas recebem `review_adaptive_migration`, `review_required`, `native_only_review` ou `blocked`; **nenhuma mudança observada no SQL Server gera DDL automático**. Uma observação que não mudou não cria nova linha no histórico.

As seguintes tabelas são criadas no PostgreSQL local:

```text
replica.source_catalog_observations  (engine-neutral native/logical JSONB)
replica.schema_versions               (contrato PostgreSQL remoto ativo)
replica.schema_state                  (active_hash e pending_hash)
replica.schema_changes                (decisões de migração)
replica.schema_backfill_runs          (lotes Kafka da nova coluna)
replica.schema_backfill_values        (staging de valores por lote)
replica.schema_backfill_intervals     (faixas concluídas)
```

## 2. Subir laboratório padrão (PostgreSQL → Kafka → PostgreSQL)

```bash
cd kafka_restore_lab_v5
docker compose up -d --build
docker compose run --rm cli chaos init

# Primeira carga; depois, somente diferenças encontradas por faixa:
docker compose run --rm cli bootstrap
docker compose run --rm cli incremental --from-id 1 --sweep-width 1000

docker compose run --rm cli status
```

Interfaces: Kafbat `http://localhost:8080`; Adminer `http://localhost:8081`. O arquivo `sql/remote.sql` cria a conta de leitura `remote_reader`; scripts de fixture que alteram a origem existem **apenas para o laboratório**.

### Detectar e aprovar DDL aditivo (PostgreSQL como origem)

```bash
# Fixture fictícia: única conexão ADMIN ao PostgreSQL remoto no laboratório.
docker compose exec -T remote-db psql -U postgres -d lab_remote -c \
  'ALTER TABLE public.contracts ADD COLUMN currency_code TEXT;'

# 1. Planejar; não altera o destino
docker compose run --rm cli schema plan

# 2. Operador aprova UMA coluna TEXT nullable, append-only, sem default
docker compose run --rm cli schema apply

# 3. Varreduras de backfill; repetir até marcar complete.
docker compose run --rm cli schema backfill --sweep-width 1000
docker compose run --rm cli schema status

# 4. Ativar após todas as faixas estarem validadas
docker compose run --rm cli schema activate

# 5. Retomar reconciliação com o novo contrato
docker compose run --rm cli incremental --from-id 1 --sweep-width 1000
```

`schema apply` prepara os dois slots locais sob barreira de migração, sem expor prematuramente a coluna na view. Durante backfill, a reconciliação normal e o rollback são bloqueados. `schema activate` amplia a view somente quando as faixas foram concluídas, a estrutura ainda coincide com a origem e não surgiram novos IDs fora do máximo registrado. Uma nova alteração da origem durante o backfill bloqueia a ativação; não se deve forçar a passagem de schema.

Renomear/remover coluna, modificar tipo, NULL/NOT NULL, PK, constraints ou índices exige revisão manual. O protótipo não propaga índices arbitrários, sequences, roles, grants ou FKs de um banco inteiro. Após uma migração, rollback para uma geração anterior incompatível é rejeitado: blue-green de DDL não equivale a backup físico.

## 3. SQL Server como origem OPCIONAL do catálogo

**Requisitos:** Docker Compose, máquina com RAM suficiente (SQL Server Developer normalmente precisa de pelo menos ~2 GB), arquitetura de CPU suportada pela imagem. O serviço é opt-in e não é iniciado no cenário padrão. Portas expostas apenas no host local: PostgreSQL remoto `55432`, local `55433`, SQL Server `51433`.

```bash
# Inicia apenas o SQL Server fictício e os serviços locais necessários
# (a primeira inicialização pode carregar imagens grandes)
docker compose --profile mssql --profile mssql-tools up -d --build \
  remote-sqlserver local-db kafka

# Cria banco e usuário fictícios: somente no container SQL Server do lab
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture init

# Faz inspeção, persistência em JSONB e envia evento de auditoria Kafka
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  catalog observe --engine sqlserver --source-key mssql-dbo-contracts

# Admin sintético faz um ALTER TABLE; reader nunca recebe esse privilégio
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture add-column

# Observa novo estado e registra o diff
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  catalog observe --engine sqlserver --source-key mssql-dbo-contracts

# Outras mutações sintéticas para testar tipagem e DROP:
docker compose --profile mssql --profile mssql-tools run --rm mssql-fixture change-length
docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  catalog observe --engine sqlserver --source-key mssql-dbo-contracts

docker compose --profile mssql --profile mssql-tools run --rm mssql-cli \
  catalog history --source-key mssql-dbo-contracts
```

Para conectar a um SQL Server real, configure `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DATABASE`, `MSSQL_USER` e `MSSQL_PASSWORD` no serviço de inspeção por secrets/variáveis de ambiente. **Não copie a senha fictícia deste Compose para qualquer ambiente real.** A conta SQL Server precisa ter `SELECT` na tabela e visibilidade dos metadados relevantes. Alguns textos de defaults, computed columns e outras definições podem exigir `VIEW DEFINITION`; valores invisíveis não devem ser tratados como expressão vazia.

A inspeção SQL Server é repetida duas vezes para detectar alterações concorrentes durante a leitura do catálogo; isso é uma checagem de estabilidade, não uma garantia transacional universal. A operação captura **o estado atual** do schema, não o histórico literal de comandos `ALTER TABLE` executados entre observações.

### Consultar dicionários no PostgreSQL local

```sql
SELECT observation_id, source_engine, object_name,
       classification, observed_at, native_hash, logical_hash,
       changes, native_changes
FROM replica.source_catalog_observations
WHERE source_key='mssql-dbo-contracts'
ORDER BY observation_id DESC;

-- Acesso direto a coluna por chave de dicionário JSONB:
SELECT logical_manifest #> '{columns,currency_code}' AS currency_code
FROM replica.source_catalog_observations
WHERE source_key='mssql-dbo-contracts'
ORDER BY observation_id DESC LIMIT 1;

-- Tamanho nativo NVARCHAR/precisão e collation:
SELECT jsonb_pretty(native_manifest) FROM replica.source_catalog_observations
WHERE source_key='mssql-dbo-contracts'
ORDER BY observation_id DESC LIMIT 1;
```

## 4. Testar

```bash
# Offline, sem Docker nem SQL Server:
python tests/check_static.py
python tests/test_reconcile.py
python tests/test_generation_paths.py
python tests/test_schema_contract.py
python tests/test_source_catalog.py

# E2E apenas contra volumes descartáveis do laboratório:
LAB_RESET=1 bash tests/e2e_schema.sh
LAB_SQLSERVER_TEST=1 bash tests/e2e_sqlserver_catalog.sh
```

**Atenção aos volumes:** o script `e2e_schema.sh` executa `docker compose down -v` para zerar os dados fictícios. Nunca use esse Compose e esses comandos contra instâncias reais. Se você já possui volumes de uma V4/V5 antiga, scripts SQL em `docker-entrypoint-initdb.d` NÃO executam novamente em volumes inicializados: recrie apenas volumes descartáveis ou migre o catálogo via SQL manualmente.

Testes offline cobrem parser `sys.*` simulado, identificação de alterações nativas e lógicas, hash determinístico, segurança de identificadores, rejeição de DDL incompatível e caminhos de blue-green. **Não foi possível executar os testes Docker nem validar SQL Server real neste ambiente**. A inspeção SQL Server foi verificada com fixtures de metadados, não contra um servidor real.

## 5. Limitações para 700 GB e próxima evolução

- O transporte é incremental **em diferenças**, mas a auditoria precisa ler dados da origem por faixas. Sem `updated_at`, `deleted_at`, CDC/WAL ou log autorizado, nenhum motor pode detectar toda mudança sem cobrir a fonte ao longo de um ciclo.
- O laboratório de dados atual é PostgreSQL→PostgreSQL, PK `BIGINT`, uma tabela, um tópico Kafka de dados com partição única. Para uma fonte SQL Server, precisamos de uma estratégia de isolamento/snapshot que a origem efetivamente suporte (por exemplo `SNAPSHOT` habilitado pelo DBA); não presumir que `READ COMMITTED` seja consistente entre múltiplas consultas.
- O driver SQL Server não utiliza o fingerprint PostgreSQL `jsonb_build_array(... )::text`. Para ampliar o reconciliador será necessário definir serialização canônica dos valores, parâmetros de collation/tipos, `NULL`, datas, timezones, unicode e DECIMAL, ou um mecanismo de comparação independente. `HASHBYTES('SHA2_256', ...)` existe, mas não resolve sozinho essas divergências de representação.
- Uma definição nativa `NVARCHAR(30)` e `NVARCHAR(100)` pode gerar o mesmo tipo lógico `text`; por isso persistimos **native_hash e native_changes além dos hashes lógicos**. Alterações de collation devem ser revisadas antes de promover dados.
- Apenas `ADD COLUMN TEXT NULL` no final da tabela é automaticamente elegível para aprovação humana **quando a origem de dados é PostgreSQL**. SQL Server pode adicionar `NVARCHAR`, mas isso ainda não autoriza aplicar DDL local sem mapeamento e testes.
- Blue-green do laboratório usa duas tabelas na mesma instância PostgreSQL, não duas instâncias independentes; não substitui alta disponibilidade ou backup físico. A V5 protege promoções de schemas incompatíveis e não executa SQL arbitrário vindo do Kafka.

**Conclusão:** a camada de *catalog/schema parsing* agora pode ser independente de PostgreSQL/SQL Server. A camada de reconciliação de dados ainda não. Essa separação é deliberada para evitar falsos positivos de atualização/deleção em um banco de 700 GB.
