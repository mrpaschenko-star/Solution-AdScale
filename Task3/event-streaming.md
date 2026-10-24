# Потоковая обработка событий

## 1. Тракт данных и надёжность приёма

Показы, клики и уведомления приходят через отдельный HTTPS Event Ingestion сервиса Statistics. Он проверяет аутентификацию, размер, schema и семантику источника, нормализует событие и пишет в региональную Kafka. ACK клиенту выдаётся **после** broker ACK. Сетевой timeout после записи оставляет исход неизвестным: источник повторяет тот же business key. Если браузер не повторяет событие, потери до устойчивого приёма измеряются отдельно и не маскируются заявлением RPO 0.

Campaign и Finance публикуют через transactional outbox. Данные владельца и строка outbox фиксируются одной SQL-транзакцией; publisher помечает строку только после Kafka ACK. Crash между ACK и пометкой даёт повтор с тем же `eventId`. Публикация и mark не являются распределённой транзакцией. Порядок изменений одного агрегата сохраняется при конкуренции publisher workers; version/gap detection остаётся на consumer.

Региональный кластер: RF=3, по одной replica partition на AZ, `min.insync.replicas=2`, producer `acks=all`, `enable.idempotence=true`, `unclean.leader.election.enable=false`. KRaft controller quorum — 3 узла в 3 AZ. Без достаточного ISR запись не подтверждается. Producer idempotence не устраняет HTTP-повторы после рестарта и не заменяет бизнес-дедупликацию. Kafka недоступна — callbacks получают повторяемую ошибку, Ingestion использует bounded buffers/backpressure; финансовое событие нельзя подтвердить после записи только в память процесса.

Bidding не ждёт Kafka в критическом bid path. Обязательство обеспечивается grant и атомарным резервом; notice передаёт `reservationProof`, который Finance проверяет без чтения чужого Redis. Если notices задерживаются, средства удерживаются дольше. Произвольное уменьшение удержаний ради доступности недопустимо.

## 2. Топики и retention

Число partitions — начальная гипотеза для теста, а не расчёт по одному RPS. Для всех топиков RF=3; `cleanup.policy=delete`. Kafka retention ограничивает окно оперативного replay; archive policy — отдельная настройка.

| Topic | Ключ | Partitions, старт | Retention Kafka | Producer → consumer groups |
|---|---|---:|---|---|
| `campaign.changes.v1` | `campaign_id` | 12 | 7 дней | Campaign outbox → `bidding-campaign-{region}-{instance}`, `delivery-campaign-{region}`, `analytics-campaign-{region}`, `archive-campaign-{region}` |
| `budget.grants.v1` | `grant_id` | 12 | 7 дней | Finance outbox → `quota-grants-{region}`, `archive-grants-{region}` |
| `ad.impressions.v1` | `auction_id` | 48 | 24 часа | Ingestion → `analytics-impressions-{region}`, `archive-impressions-{region}` |
| `ad.clicks.v1` | `auction_id` | 24 | 24 часа | Ingestion → `analytics-clicks-{region}`, `archive-clicks-{region}`; отдельная group антифрода при появлении функции |
| `auction.notices.v1` | `reservation_id` | 24 | 7 дней | Ingestion → `finance-notices-{region}`, `analytics-notices-{region}`, `archive-notices-{region}` |
| `finance.settlements.v1` | `grant_id` | 24 | 7 дней | Finance outbox → `quota-settlements-{region}`, `analytics-settlements-{region}`, `archive-settlements-{region}` |
| `events.quarantine.v1` | исходные topic/partition | 12 | 7 дней | Проверяющий consumer → `reconciliation-{region}`, архив; финансовый incident дополнительно фиксируется в Finance PostgreSQL |

Один shared group распределяет partitions между workers, поэтому подходит для единого Analytics sink или quota manager с общим Redis. Quota manager маршрутизирует по grant в нужный Redis slot, включает generation/version и применяет изменения идемпотентно. Но **каждой полной in-memory проекции Bidding нужны все изменения**, поэтому у неё отдельная group, а не один group на все replicas. У Delivery в таблице предполагается общий Redis-кеш: при переходе на полные локальные индексы нужна та же fan-out модель. Уничтоженные группы удаляются после безопасного закрытия жизненного цикла экземпляра.

Порядок гарантируется только внутри partition; общего порядка Campaign/Finance/notices нет. `campaignVersion`, версия расчёта и business state machine важнее времени получения. Поздний loss не отменяет уже проведённый billable. Heartbeats Campaign проходят по каждой partition в порядке после подтверждённого outbox checkpoint; freshness не определяется только временем последнего изменения кампании.

Compaction не применяется к исходным финансовым фактам и журналам, которые нужны для аудита/непрерывного replay. В будущем отдельный `campaign.snapshot` compacted topic может хранить последнюю **полную** модель по ключу. Это дополнительная проекция: удалённые промежуточные versions нельзя использовать как непрерывный changes-журнал. Kafka tombstone для удаления кампании также не является tombstone RTB-дедупликации.

24 часа raw retention выбраны для снижения дисковой стоимости при наличии непрерывного архива. Предупреждение при архивном lag >15 мин, критический сигнал >1 ч; при lag 12 ч уже запускается увеличение ёмкости/retention либо управляемое ограничение источника. Это заранее выбранные пороги, они уточняются по скорости восстановления. Retention удаляет старые сегменты независимо от consumer ACK: «consumer ещё не прочитал» не защищает данные. `retention.bytes` не должен незаметно сокращать рассчитанное временное окно.

Для financial/control 7 дней — стартовое окно online replay, не срок окончательности обязательств. Долгий простой восстанавливается по архиву/ledger. Финансовый карантин не исчезает как неразобранный incident по Kafka TTL; он хранится с ledger и блокирует зависимую последовательность при необходимости.

## 3. Формат: JSON + JSON Schema

Выбран JSON с registry версионируемых JSON Schema, поскольку профиль OpenRTB и события задания 2 уже используют JSON, а команде за три месяца важны простота диагностики и единая схема. Avro уменьшает объём и стоимость кодирования, поддерживает schema resolution, но потребует миграции клиентов и binary tooling. Решение пересматривается по фактическим CPU, bytes/event и стоимости хранения; замена JSON на Avro не меняет бизнес-ключи и гарантии доставки. При миграции — новый согласованный wire format/topic и сверка двух проекций, без двойных денежных эффектов.

Registry развёртывается отказоустойчиво; producers/consumers имеют локальный cache заранее одобренных schemas. Registry не вызывается на каждое событие. Новые schemas регистрируются в release pipeline, экспортируются вместе с backup; недоступность registry блокирует выпуск неизвестной версии, но не обработку известной. CI проверяет backward/forward compatibility относительно всех поддерживаемых consumers. Новые optional-поля допускаются, старые значения не меняют смысл; rename/type change требует v2. Регистрация schema не заменяет проверку денег, подписи и контрактных ограничений.

Общий envelope содержит `eventId`, `eventType`, `schemaVersion`, `source`, `occurredAt`, `receivedAt`, `region`; по типу обязательны aggregate IDs/version и payload. ID непрозрачные строки, времена UTC RFC 3339. У billable обязательны partner event ID, auction/bid/imp, reserve/grant/proof, campaign/version, currency, amount и price basis. Пример, совместимый с заданием 2:

```json
{
  "eventId": "event-6001",
  "eventType": "AuctionBillable",
  "schemaVersion": 1,
  "source": "partner-new-dsp",
  "occurredAt": "2026-10-24T10:00:00.000Z",
  "receivedAt": "2026-10-24T10:00:00.050Z",
  "partnerId": "new-dsp",
  "partnerEventId": "settlement-801",
  "auctionId": "auction-9001",
  "impId": "imp-1",
  "bidId": "bid-9001",
  "reservationId": "reservation-9001",
  "reservationProof": "opaque-demo-proof",
  "grantId": "grant-37",
  "region": "region-a",
  "campaignId": "campaign-42",
  "campaignVersion": 17,
  "currency": "USD",
  "amountMicros": 1250,
  "priceBasis": "IMPRESSION",
  "traceId": "trace-example"
}
```

`amountMicros` — целое неотрицательное значение, не больше допустимого безопасного JSON integer `2^53−1` и контрактного лимита; вычисления в сервисах — checked int64/decimal, без float. 1,25 USD CPM соответствует 1 250 micros на показ до комиссий. Reserve округляется вверх, включает допустимые комиссии и FX-ограничения. `priceBasis=IMPRESSION` не позволяет ещё раз разделить сумму на 1000. Все значения примера вымышлены, proof не рабочий.

Для CPM-пилота charge kind выводится из `eventType=AuctionBillable` как первичное CPM-списание. Новые типы корректировок имеют отдельный ID, ссылку на исходный settlement и явную семантику; их нельзя представить повтором того же billable с другой суммой.

Нормализатор сохраняет стабильный `eventId` по авторитетному ID источника либо детерминированному namespace+business key. Два реальных клика — два разных события, дедупликация по одному `auction_id` неверна. Если источник не даёт устойчивый ID, SDK должен создать его до первого отправления и сохранять при retry; гарантировать точный подсчёт без этого нельзя. Для проверки конфликтов сравнивается канонический бизнес-payload, исключая меняющиеся `receivedAt`/trace транспорта.

## 4. Доставка, дедупликация и аналитические суммы

**Finance:** at-least-once consumer → локальная SQL-транзакция inbox + ledger + outbox → commit Kafka offset. Если process упал после SQL commit, replay не создаёт новую проводку. Одинаковый business key с разными суммами — durable quarantine и alert. Commit продвигается только по непрерывному обработанному диапазону. Если зависимое событие нельзя пропустить, partition/агрегат приостанавливается. Kafka transactions сами по себе не дают exactly-once для внешнего PostgreSQL.

**Analytics:** каноническая таблица `ReplicatedReplacingMergeTree(version)` имеет `PARTITION BY toYYYYMM(event_date)` и `ORDER BY (advertiser_id, event_date, event_type, event_id)`. Все версии одного факта сохраняют advertiser/date/type и shard. `event_date` получается из проверенного неизменяемого времени источника. Противоречивые immutable поля одного ID — конфликт, а не новая версия. Для источников impressions/clicks точные повторы имеют одинаковый payload; финансовые корректировки — самостоятельные facts.

Повторы могут физически лежать в ClickHouse до merge. Поэтому **нельзя** считать корректным `SUM(amount)` или обычный инкрементальный materialized view поверх сырых вставок. Для текущих отчётов канонические факты читаются с `FINAL` в ограниченном tenant/time диапазоне и ресурсном pool; стоимость такого запроса проверяется нагрузкой. Текущие отчёты отображают watermark и допускают согласованную задержку.

Для длинных диапазонов jobs строят агрегаты из дедуплицированной канонической выборки, записывают новую полную версию bucket (tenant/day), сверяют count/sum и публикуют manifest завершённой версии. Читатель выбирает ровно одну последнюю **завершённую** версию bucket; append частичных сумм и суммирование всех версий запрещены. Manifest публикуется после подтверждения всех блоков данных; неполная версия невидима. Позднее событие помечает bucket для пересчёта, вместо повторного прибавления всей пачки. Финансовые итоги сверяются с ledger, а не с числом win/impression.

Consumer подтверждает offset после синхронного успешного insert quorum. Crash/неясный insert ACK → повтор той же партии и безопасная дедупликация. Insert dedup tokens ClickHouse полезны для кратких retry, но имеют ограниченное окно и не заменяют business identity при длительном replay. Для backfill используется отдельная квота, чтобы не вытеснить online ingestion.

## 5. Архив, replay и регионы

Archive workers читают независимыми groups, формируют сжатые immutable объекты (например, Parquet с исходным envelope) по topic/partition/диапазону offsets. Checksum, min/max offsets, record count, schema IDs и временной диапазон входят в manifest. Сначала устойчиво записываются объект и manifest условной идемпотентной операцией, затем commit offset. Crash между шагами даёт повтор того же диапазона или перекрытие; manifest-check и event dedup делают replay безопасным. Checkpoint не продвигается до подтверждения хранения. Проверка непрерывности обнаруживает пропуски.

Raw impressions/clicks — 30 дней в архиве как рабочее предложение; Campaign/grants/notices/settlements, необходимые для финансовой сверки, хранятся по политике ledger с минимизацией персональных полей (предварительно 5 лет, требует согласования). Архив raw не должен продлевать хранение персональных данных только потому, что финансовые проводки хранятся дольше. Доступ к proof/финансовому карантину ограничен; BI-потребителю выдаётся редактированная проекция без лишних полей.

Replay запускается в отдельный namespace/group с зафиксированными входными диапазонами и schema version. Для rebuilding Analytics сначала staging, контроль сумм/количеств, затем публикация. События финансов можно повторно обработать только через штатную идемпотентность Finance, без прямых вставок в ledger. Удалённый Kafka offset восстанавливается через архив; если и архивного диапазона нет, это incident потери данных, а не повод перейти на `latest`.

Между регионами — асинхронное копирование нужных журналов/архивов с сохранением origin region, topic/partition/offset и `eventId`. Числовые offsets разных кластеров не равны. Mirror namespaces исключают циклы; один активный финансовый consumer владеет каждым origin stream, promotion использует fencing и непрерывные checkpoints с безопасным перекрытием. Потеря региона до копирования хвоста возможна; target lag ≤60 с не является строгим RPO. Порядок campaign-проекции подтверждается региональными watermarks.

## 6. Эксплуатация

TLS, сервисные ACL по topics/groups, отдельные producer quotas для raw и финансовых событий. Неизвестная schema/недопустимый payload не попадает в финансовый normal flow. Всплеск кликов не должен занять весь broker disk или network; capacity, quotas и задержка контрольного потока проверяются вместе. При необходимости финансовые topics переносятся в отдельный кластер по измеренному риску, а не заранее для каждого сервиса.

Наблюдаемость: ISR/under-replicated partitions, disk time-to-full, producer errors/latency, outbox oldest age, consumer lag в секундах и offsets, archive coverage, mirror lag, quarantine age, ClickHouse merge backlog и report watermark. Capacity-расчёт и пример объёма: [scaling.md](scaling.md). Планы резервного копирования: [database-strategy.md](database-strategy.md).
