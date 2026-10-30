# Decisões Arquiteturais (ADRs) – Saga de Compra

Este documento registra as decisões, os riscos mitigados e as estratégias de resiliência da Saga de compra (Order, Inventory e Payment). Os diagramas citados estão em [`../diagrams/`](../diagrams/).

## Sumário

1. [ADR-001: Saga com Orquestração](#adr-001-saga-com-orquestração)
2. [ADR-002: Mensageria com Transactional Outbox](#adr-002-mensageria-com-transactional-outbox)
3. [ADR-003: Idempotent Consumer](#adr-003-idempotent-consumer)
4. [ADR-004: Tabela de estado da Saga e timeouts](#adr-004-tabela-de-estado-da-saga-e-timeouts)
5. [ADR-005: Retry, Circuit Breaker e Bulkhead](#adr-005-retry-circuit-breaker-e-bulkhead)
6. [ADR-006: Dead Letter Queues](#adr-006-dead-letter-queues)
7. [Riscos mitigados](#riscos-mitigados)
8. [Observabilidade e operação](#observabilidade-e-operação)

---

## ADR-001: Saga com Orquestração

- **Status:** aceita
- **Contexto:** o fluxo de compra tem ramificações (aprovado, recusado, timeout) e deve ganhar novos participantes (antifraude, frete, nota fiscal).
- **Decisão:** usar um orquestrador (*Create Order Saga*) hospedado no Order Service. Ele envia comandos, recebe respostas e persiste o estado.
- **Consequências:**
  - (+) Estado e fluxo visíveis em um só lugar; compensação em ordem inversa e previsível.
  - (+) Inventory e Payment não conhecem a saga, só seus próprios comandos.
  - (−) O orquestrador concentra lógica de coordenação. Mitigação: manter nele **apenas coordenação**, sem regra de negócio dos outros domínios; várias instâncias *stateless* com estado persistido.

A comparação ponderada com a Coreografia está no [README](../README.md#2-estilo-escolhido-orquestração).

## ADR-002: Mensageria com Transactional Outbox

- **Status:** aceita
- **Problema:** gravar no banco e publicar no broker são duas operações. Se o serviço cair entre as duas, o estado muda sem a mensagem sair (ou o inverso).
- **Decisão:** o orquestrador grava **na mesma transação local** a mudança de estado e a mensagem na tabela `outbox`. Um *relay* (poller ou CDC) publica as mensagens no broker e marca como enviadas.
- **Consequências:** entrega **at-least-once** (pode haver duplicata), por isso todo consumidor precisa ser idempotente (ADR-003).
- **Ordem:** o `sagaId` é a chave de partição, então as mensagens de uma mesma saga chegam em ordem.

## ADR-003: Idempotent Consumer

- **Status:** aceita
- **Problema:** o broker pode entregar a mesma mensagem mais de uma vez (reentrega após falha de ack, retry do orquestrador, reprocessamento da DLQ). Reservar estoque ou cobrar duas vezes seria um erro grave.
- **Decisão, em duas camadas:**

**Camada 1: deduplicação por mensagem.** Cada serviço tem:

```sql
CREATE TABLE processed_messages (
  message_id   UUID        PRIMARY KEY,
  consumer     VARCHAR(50) NOT NULL,
  reply_type   VARCHAR(50),
  reply_body   JSONB,
  processed_at TIMESTAMP   NOT NULL DEFAULT now()
);
```

Fluxo do consumidor, **tudo em uma única transação local**:

1. `INSERT` do `message_id` em `processed_messages`.
2. Se violar a chave primária (duplicata): não executa o efeito, lê `reply_body` já gravado e **reenvia a mesma resposta**; depois dá *ack* na mensagem.
3. Se inseriu: executa o efeito de negócio, grava a resposta na *outbox* e faz *commit*.

**Camada 2: idempotência de negócio.** Protege contra duplicatas com `messageId` diferente (por exemplo, o orquestrador gerou um novo comando):

| Operação | Chave natural | Regra |
| --- | --- | --- |
| `ReserveStock` | `(sagaId, sku)` único em `reservation` | Se já existe reserva da saga, devolve `StockReserved` sem reservar de novo |
| `ReleaseStock` | `reservation.status` | Só libera se estiver `RESERVED`. Se já está `RELEASED`, responde `StockReleased` sem alterar nada. Se nunca existiu reserva, também responde sucesso |
| `ProcessPayment` | `payment.saga_id` único | Uma cobrança por saga; o gateway recebe `Idempotency-Key = sagaId`, então uma repetição devolve o resultado original |
| `CancelOrder` | `order.status` | Cancelar um pedido já `CANCELED` não faz nada |

### Matriz de decisão: duplicatas, reenvios e falhas

| Cenário | Como é detectado | Ação do sistema | Garantia |
| --- | --- | --- | --- |
| Mesmo comando entregue 2× pelo broker | `message_id` já existe em `processed_messages` | Ignora o efeito e reenvia a resposta gravada | Sem reserva ou cobrança duplicada |
| Resposta duplicada chega ao orquestrador | Estado da saga já avançou (`version`/`current_step` não bate) | Descarta a resposta e dá *ack* | Sem transição de estado duplicada |
| Resposta atrasada de um passo já compensado (ex.: `PaymentApproved` depois do timeout) | Saga está `COMPENSATING` ou `COMPENSATED` | Orquestrador envia `RefundPayment` para desfazer a cobrança tardia | Dinheiro não fica retido sem pedido |
| Timeout esperando resposta | `deadline_at < now()` e estado `WAITING_*` | Reenvia o **mesmo `messageId`**; após N tentativas, compensa | Retry seguro por causa da idempotência |
| Serviço cai depois de gravar o efeito e antes do *ack* | Broker reentrega a mensagem | O `message_id` já existe: não repete o efeito, apenas reenvia a resposta | Efeito exatamente uma vez (*effectively-once*) |
| Chamada ao gateway financeiro sem resposta | Timeout da chamada HTTP (5 s) | Retry com a mesma `Idempotency-Key`; se persistir, consulta o status da cobrança antes de decidir | Sem cobrança dupla nem compensação indevida |
| Dois orquestradores pegam a mesma saga | `UPDATE ... WHERE version = :v` afeta 0 linhas | Quem perdeu a corrida descarta e relê o estado | Sem decisões concorrentes |

## ADR-004: Tabela de estado da Saga e timeouts

- **Status:** aceita
- **Decisão:** o estado da saga fica em tabelas do Order Service (a máquina de estados está em [`saga-state-machine.png`](../diagrams/saga-state-machine.png)).

```sql
CREATE TABLE saga_instance (
  saga_id       UUID        PRIMARY KEY,
  order_id      VARCHAR(50) NOT NULL UNIQUE,
  state         VARCHAR(30) NOT NULL,   -- STARTED, STOCK_RESERVED, PAYMENT_APPROVED,
                                        -- COMPLETED, COMPENSATING, COMPENSATED, FAILED_MANUAL
  current_step  VARCHAR(30) NOT NULL,
  version       INT         NOT NULL,   -- controle otimista de concorrência
  retry_count   INT         NOT NULL DEFAULT 0,
  deadline_at   TIMESTAMP,              -- prazo para a resposta do passo atual
  payload       JSONB       NOT NULL,
  created_at    TIMESTAMP   NOT NULL DEFAULT now(),
  updated_at    TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE TABLE saga_step_log (
  id          BIGSERIAL PRIMARY KEY,
  saga_id     UUID        NOT NULL REFERENCES saga_instance,
  step        VARCHAR(30) NOT NULL,
  direction   VARCHAR(12) NOT NULL,     -- FORWARD ou COMPENSATION
  message_id  UUID        NOT NULL,
  outcome     VARCHAR(20),              -- SENT, SUCCEEDED, FAILED, TIMED_OUT
  created_at  TIMESTAMP   NOT NULL DEFAULT now()
);
```

### Tratamento de timeouts de rede

Cada passo que espera resposta grava um `deadline_at`. Um *scheduler* roda a cada poucos segundos e busca sagas com `state` em espera e `deadline_at < now()`:

| Passo | Prazo (`deadline_at`) | Tentativas | Ao esgotar |
| --- | --- | --- | --- |
| `ReserveStock` | 30 s | 3 (mesmo `messageId`) | `COMPENSATING` (cancela o pedido) |
| `ProcessPayment` | 60 s | 3 (mesmo `messageId`) | Consulta o status no gateway. Se não houver cobrança, `COMPENSATING`; se aprovada, segue o fluxo |
| `CommitStock` | 30 s | Ilimitadas, com backoff (é retentável) | Alerta operacional após 10 tentativas |
| `ReleaseStock` (compensação) | 30 s | Ilimitadas, com backoff | Após 10 tentativas, vai a `FAILED_MANUAL` e dispara alerta |

Regras importantes:

1. **O retry reutiliza o `messageId`.** Assim, se a primeira mensagem na verdade chegou, o consumidor a reconhece como duplicata.
2. **Compensações nunca são abandonadas em silêncio.** Ficam em retry com backoff; se esgotarem, a saga fica `FAILED_MANUAL` e a equipe é alertada (ver ADR-006).
3. **Rede lenta não é recusa.** Um timeout no pagamento não pode ser tratado como "recusado": primeiro se consulta o gateway. Compensar cedo demais poderia liberar o estoque de um pedido que foi cobrado.
4. **Reserva com TTL como rede de segurança.** Toda reserva no Inventory expira (15 min). Se a saga se perder por completo, o estoque volta sozinho.

## ADR-005: Retry, Circuit Breaker e Bulkhead

- **Status:** aceita
- **Escopo:** chamadas **síncronas** do Payment ao gateway financeiro (o ponto mais frágil, por ser externo) e acesso aos bancos.

| Mecanismo | Configuração inicial (ajustar com métricas reais) | Por quê |
| --- | --- | --- |
| **Timeout** | 5 s na chamada ao gateway | Não prender threads esperando um serviço lento |
| **Retry** | 3 tentativas, backoff exponencial (200 ms, 400 ms, 800 ms) com *jitter*, só em erros transitórios (timeout, 5xx, 429). **Nunca** em 4xx de negócio (ex.: cartão recusado) | Absorver falhas momentâneas sem causar tempestade de requisições |
| **Circuit Breaker** | Abre com 50% de falhas nas últimas 20 chamadas; fica aberto 30 s; *half-open* com 5 chamadas de teste | Parar de bater em um gateway fora do ar e se recuperar sozinho |
| **Bulkhead** | Pool de threads/conexões separado para o gateway, limite de 20 chamadas simultâneas | Uma lentidão no gateway não derruba o resto do Payment Service |
| **Idempotency-Key** | `sagaId` em toda chamada de cobrança | Tornar o retry seguro (sem cobrança dupla) |

**Com o circuit breaker aberto:** o Payment não chama o gateway e responde ao orquestrador com `PaymentRejected` (motivo `GATEWAY_UNAVAILABLE`) ou deixa a mensagem em retry com atraso, conforme a política de negócio. O padrão recomendado é **atrasar e tentar de novo** dentro do prazo da saga; se o prazo estourar, a saga compensa.

## ADR-006: Dead Letter Queues

- **Status:** aceita
- **Problema:** algumas mensagens nunca serão processadas com sucesso (corpo inválido, bug, dado inconsistente). Sem tratamento, elas travam a fila (*poison message*).
- **Decisão:** cada tópico de comandos e de respostas tem uma DLQ (`inventory.commands.dlq`, `payment.commands.dlq`, `saga.replies.dlq`).

**Fluxo:**

1. O consumidor tenta processar. Falha transitória: retry local com backoff (3 tentativas).
2. Persistindo a falha, a mensagem vai para uma fila de retry com atraso (1 min, depois 5 min).
3. Esgotadas as tentativas, vai para a **DLQ**, com cabeçalhos de diagnóstico: erro, *stack trace*, nº de tentativas, `sagaId` e `messageId`.
4. Um **alerta** é disparado quando a DLQ recebe qualquer mensagem.
5. Operação analisa a causa. Corrigido o problema, uma ferramenta de **reprocessamento** reenvia a mensagem ao tópico original. Isso é seguro porque o consumidor é idempotente.

**Casos especiais:**

- Mensagem de **compensação** na DLQ é prioridade máxima: a saga está `COMPENSATING` com estoque possivelmente preso. O alerta é de alta severidade.
- Mensagem com **formato inválido** (não desserializável) vai direto para a DLQ, sem retry, porque repetir não vai resolver.
- A saga associada fica visível como `COMPENSATING` ou em espera; a tabela `saga_instance` mostra quais pedidos estão afetados.

## Riscos mitigados

| Risco | Impacto | Mitigação |
| --- | --- | --- |
| Pagamento recusado após reserva de estoque | Estoque preso, pedido inconsistente | Compensação `ReleaseStock` + `CancelOrder` (ADR-001) |
| Mensagem duplicada | Reserva ou cobrança em dobro | Idempotent Consumer em duas camadas + `Idempotency-Key` no gateway (ADR-003) |
| Falha entre gravar no banco e publicar mensagem | Saga trava ou estado perdido | Transactional Outbox (ADR-002) |
| Timeout de rede sem resposta | Saga parada, cliente esperando | `deadline_at`, retry com mesmo `messageId`, compensação (ADR-004) |
| Resposta tardia depois da compensação | Cliente cobrado sem pedido | Orquestrador detecta o estado e envia `RefundPayment` |
| Gateway financeiro indisponível | Falhas em cascata no Payment | Timeout, retry com backoff, circuit breaker e bulkhead (ADR-005) |
| Mensagem venenosa trava a fila | Compras paradas | Retry limitado + DLQ + alerta + reprocessamento (ADR-006) |
| Dois orquestradores processam a mesma saga | Decisões conflitantes | Controle otimista com `version` e partição por `sagaId` |
| Saga perdida por falha grave | Estoque preso indefinidamente | TTL de 15 min nas reservas |
| Compensação que falha repetidamente | Inconsistência permanente | Retry ilimitado com backoff, estado `FAILED_MANUAL`, alerta e runbook |
| Leitura do pedido durante a saga (consistência eventual) | Cliente vê `PENDING` | Status explícito `PENDING` na API e notificação ao final; a UI deve tratar o estado intermediário |

## Observabilidade e operação

- **Rastreamento:** `sagaId` é o `correlationId` em todos os logs, mensagens e *traces* (OpenTelemetry). Com ele, é possível reconstruir qualquer compra de ponta a ponta.
- **Métricas:** sagas por estado, duração por passo, taxa de compensação, taxa de timeout, tamanho das DLQs, estado do circuit breaker, idade da mensagem mais antiga em cada fila.
- **Alertas:** qualquer mensagem na DLQ; sagas em `FAILED_MANUAL`; `COMPENSATING` por mais de 5 min; circuit breaker aberto por mais de 2 min.
- **Runbook de saga travada:** consultar `saga_instance` e `saga_step_log` por `sagaId`, verificar o último passo, checar a DLQ e, se necessário, reprocessar a mensagem; como último recurso, forçar a compensação pela ferramenta administrativa.
- **Testes de resiliência:** testes de caos para duplicação de mensagem, queda do serviço entre efeito e *ack*, gateway lento e DLQ cheia.
