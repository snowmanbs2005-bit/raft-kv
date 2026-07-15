# raft-kv

[![CI](https://github.com/snowmanbs2005-bit/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/snowmanbs2005-bit/raft-kv/actions/workflows/ci.yml)

Реализация консенсус-алгоритма Raft с нуля на Go и небольшое реплицированное key-value хранилище поверх неё.

## Что решает

Когда данные хранятся на нескольких узлах, а узлы могут падать, зависать или терять сеть, встаёт вопрос: как гарантировать, что все живые узлы видят одну и ту же историю изменений, а не расходятся в разные версии данных? Raft решает это через выборы единственного лидера в каждый момент времени и репликацию журнала команд от лидера к большинству узлов: команда считается зафиксированной (committed), только когда её подтвердило больше половины кластера, поэтому кластер продолжает корректно работать даже при падении меньшинства узлов.

Этот проект — реализация Raft "с нуля", без `hashicorp/raft` и других готовых библиотек: цель — показать, что алгоритм консенсуса понят и реализован самостоятельно, а не подключён как зависимость.

## Возможности

- Выборы лидера (leader election) со случайными таймаутами
- Репликация журнала (log replication) с быстрым восстановлением отстающих followers (conflict term/index backtracking)
- Персистентность состояния (term, votedFor, log) на диск с fsync перед подтверждением RPC
- HTTP API для key-value операций (`PUT`/`GET`/`DELETE`)
- CLI-клиент `raftctl`
- Кластер из 3 узлов в Docker Compose
- Симуляция сетевых партиций и задержек для тестов (`MemoryTransport`)

## Архитектура

**Транспорт отделён от ядра.** Пакет `internal/raft` не импортирует `net`, `net/http` или `grpc` — всё общение с другими узлами идёт только через интерфейс `Transport`:

```go
type Transport interface {
    RequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
    AppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
```

Благодаря этому в тестах используется `MemoryTransport` — доставка RPC напрямую между `*Raft`-инстансами в одном процессе, без сокетов. Это даёт две вещи: тесты выборов/репликации/партиций выполняются за миллисекунды, и в `MemoryTransport` легко встроить fault injection — `SetPartition`, `SetDelay`, `SetDropRate` — чтобы честно проверить поведение при потере сети. В проде та же логика ядра работает поверх HTTP/JSON-транспорта (`internal/transport/httprpc`) без единой правки в `internal/raft`.

**Один goroutine — одно состояние, без мьютексов на горячем пути.** Все изменяемые поля Raft (`currentTerm`, `votedFor`, `log`, `commitIndex`, `nextIndex`/`matchIndex`) принадлежат единственной goroutine, выполняющей `(*Raft).run()`. Входящие RPC не вызывают методы Raft напрямую из чужой goroutine — они кладутся в канал `rpcCh` вместе с каналом для ответа и обрабатываются внутри цикла событий. Клиентские предложения (`Propose`) идут через отдельный канал `proposeCh` тем же способом. Такой дизайн убирает целый класс гонок данных без единого `sync.Mutex` вокруг основного состояния — синхронизация делается через сообщения, а не через блокировки. То немногое, что должно читаться снаружи (роль узла, текущий term, адрес известного лидера), публикуется через `atomic` после каждого изменения внутри цикла.

**Гарантия "сначала диск, потом ответ".** Раньше чем узел подтвердит голос (`VoteGranted=true`) или успешное добавление записи (`Success=true`), состояние (`term`, `votedFor`, `log`) синхронно сохраняется на диск через `fsync`. Если бы ответ отправлялся раньше записи на диск, а узел упал сразу после ответа, при перезапуске он мог бы отдать второй голос в том же term или "забыть" уже подтверждённую запись — оба сценария нарушают безопасность Raft.

**Term-restricted commit.** Лидер продвигает `commitIndex` только когда запись из **текущего** term реплицирована на большинство — это правило из статьи Raft (Figure 8), которое защищает от коммита "чужой" записи прошлого term только из-за случайного совпадения индексов.

## Стек

| Компонент | Технология |
|---|---|
| Язык | Go 1.25+ |
| Консенсус | Raft, реализация с нуля (`internal/raft`) |
| Межузловой транспорт | HTTP + JSON (`internal/transport/httprpc`), см. "Отклонения от изначального плана" ниже |
| Клиентский API | `net/http` |
| Персистентность | JSON-файл на диске с atomic rename + fsync |
| Контейнеризация | Docker, Docker Compose |
| CI | GitHub Actions |

### Отклонение от изначального плана: gRPC → HTTP/JSON

Изначально межузловой транспорт планировался на gRPC/protobuf. В окружении, где собирался проект, не было `protoc`/`protoc-gen-go`/`protoc-gen-go-grpc`, поэтому вместо генерации кода из `.proto` был написан прагматичный эквивалент на стандартной библиотеке: `internal/transport/httprpc` реализует ровно те же два RPC (`RequestVote`, `AppendEntries`) с теми же полями, что и `internal/raft/rpc.go`, но сериализует их в JSON и шлёт по HTTP вместо protobuf/HTTP2. Клиент переиспользует один `http.Client` с пулом keep-alive соединений на узел — аналог пула `grpc.ClientConn`. Поскольку `internal/raft` зависит только от интерфейса `Transport`, замена на настоящий gRPC в будущем не потребует ни одной правки в ядре консенсуса — только новую реализацию `Transport` рядом с `httprpc`.

## Запуск

### Кластер из 3 узлов в Docker

```bash
cd deploy
docker compose up --build
```

Поднимутся три узла (`node1`, `node2`, `node3`), HTTP API доступны на хосте на портах `8081`, `8082`, `8083`.

Если запрос попадёт не на лидера, узел ответит `307 Temporary Redirect` на HTTP-адрес текущего лидера (метод и тело запроса сохраняются, как того требует 307 — в отличие от 301/302). Внутри docker-сети (например, из другого контейнера или через `raftctl`) это работает прозрачно — узлы адресуют друг друга по именам сервисов (`node1`, `node2`, `node3`), которые резолвятся внутри compose-сети. При обращении с хост-машины через `curl` Location редиректа будет содержать внутреннее имя контейнера, которое хост не резолвит, поэтому для ручного тестирования с хоста проще сперва спросить, кто лидер, и обратиться напрямую к его порту:

```bash
# узнаём, кто сейчас лидер
curl localhost:8081/status
curl localhost:8082/status
curl localhost:8083/status
# {"is_leader":true,"leader_hint":"node2"} -- допустим, лидер node2, порт 8082

curl -X PUT localhost:8082/kv/foo -d bar
curl localhost:8082/kv/foo
curl -X DELETE localhost:8082/kv/foo
```

`raftctl` (см. ниже) решает эту проблему сам, если запускать его тоже внутри docker-сети:

```bash
docker compose exec node1 raftctl -peers=node1:8081,node2:8082,node3:8083 put foo bar
docker compose exec node1 raftctl -peers=node1:8081,node2:8082,node3:8083 get foo
docker compose exec node1 raftctl -peers=node1:8081,node2:8082,node3:8083 leader
```

### CLI-клиент raftctl

```bash
go build -o raftctl ./cmd/raftctl

./raftctl -peers=localhost:8081,localhost:8082,localhost:8083 put foo bar
./raftctl -peers=localhost:8081,localhost:8082,localhost:8083 get foo
./raftctl -peers=localhost:8081,localhost:8082,localhost:8083 delete foo
./raftctl -peers=localhost:8081,localhost:8082,localhost:8083 leader
```

`raftctl` сам обходит список известных узлов, следует за редиректом на лидера и переключается на следующий адрес, если текущий недоступен.

### Локальный запуск одного узла

```bash
go build -o raftnode ./cmd/raftnode

./raftnode \
  --id=node1 \
  --peers=node1=127.0.0.1:9001:127.0.0.1:8081,node2=127.0.0.1:9002:127.0.0.1:8082,node3=127.0.0.1:9003:127.0.0.1:8083 \
  --data-dir=./data/node1
```

## Тесты

```bash
# Юнит-тесты ядра Raft, FSM, хранилища, конфига, kvstore
go test ./internal/...

# Интеграционные тесты кластера (5 узлов в памяти)
go test ./test/integration/...

# Всё сразу, с детектором гонок (обязательно перед коммитом)
go test -race ./...
```

Юнит-тесты `internal/raft` (`election_test.go`, `replication_test.go`, `partition_test.go`) поднимают несколько `*raft.Raft` поверх общего `MemoryTransport` и работают с укороченными таймаутами (десятки миллисекунд), поэтому весь набор выполняется за секунды. Партиции сети симулируются через `MemoryTransport.SetPartition(a, b, reachable)` — это разрывает связь именно между парой узлов, а не отключает узел целиком, что позволяет честно проверить сценарий "меньшинство не может закоммитить, большинство может" и последующую конвергенцию журналов после `HealAll()`.

Интеграционные тесты в `test/integration` собирают кластер из 3-5 узлов и прогоняют сценарии целиком: выборы при старте, выборы после "падения" лидера (`Stop()`), партиция сети с недоступностью меньшинства и конкурентные клиентские записи из нескольких goroutine с проверкой отсутствия потерь и рассинхронизации журналов (этот тест стоит гонять с `-race`).

## Структура проекта

```
raft-kv/
  cmd/raftnode/            # процесс узла: raft + транспорт + KV + HTTP API
  cmd/raftctl/              # CLI-клиент (put/get/delete/leader)
  internal/raft/            # ядро Raft: выборы, репликация, единственный event-loop на узел
  internal/storage/         # персистентность term/votedFor/log на диск (JSON + fsync)
  internal/transport/httprpc/ # HTTP/JSON транспорт между узлами (см. "Отклонение от плана")
  internal/fsm/              # интерфейс StateMachine + KV state machine (map[string]string)
  internal/kvstore/          # HTTP API (PUT/GET/DELETE), редирект на лидера, клиент для raftctl
  internal/config/           # парсинг конфигурации узла (id, peers, дата-директория)
  deploy/                    # Dockerfile, docker-compose.yml (кластер из 3 узлов)
  test/integration/          # сквозные тесты кластера поверх MemoryTransport
  .github/workflows/ci.yml   # go vet + go build + go test -race в CI
```
