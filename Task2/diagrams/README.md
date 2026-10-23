# Диаграммы задания 2

| Сценарий | PlantUML | SVG | PNG |
|---|---|---|---|
| Границы и компоненты Bidding | [source](component-bidding.puml) | [SVG](component-bidding.svg) | [PNG](component-bidding.png) |
| Запрос ставки и резерв | [source](sequence-bid.puml) | [SVG](sequence-bid.svg) | [PNG](sequence-bid.png) |
| Кампания, снимок и replay | [source](sequence-campaign.puml) | [SVG](sequence-campaign.svg) | [PNG](sequence-campaign.png) |
| События и финансовый расчёт | [source](sequence-events.puml) | [SVG](sequence-events.svg) | [PNG](sequence-events.png) |
| Timeout, breaker и восстановление | [source](sequence-failure.puml) | [SVG](sequence-failure.svg) | [PNG](sequence-failure.png) |

Component view использует C4-PlantUML v2.12.0 из общей локальной библиотеки [Diagrams/vendor/C4-PlantUML](../../Diagrams/vendor/C4-PlantUML/README.md). Показана внутренняя структура одного Bidding-процесса и его внешние зависимости; фоновые компоненты не являются синхронными стадиями аукциона. Чтение in-memory индекса не является сетевым RPC.

Sequence-диаграммы используют стандартный PlantUML. Component view использует встроенный Smetana; Graphviz и сеть для рендеринга не нужны. Java и PlantUML установленного расширения VS Code остаются необходимы. Настройки Java уже заданы для проекта и пользователя; описание — в [инструкции задания 1](../../Diagrams/README.md).

Для переноса задания вместе с component view нужно сохранить структуру репозитория и общую папку библиотеки. Не копируйте один `C4_Component.puml` без его зависимостей. Если отдельно установлена CLI-команда PlantUML, из этой папки:

```sh
plantuml -tsvg component-bidding.puml sequence-bid.puml sequence-campaign.puml sequence-events.puml sequence-failure.puml
```

Пояснения к трактовке:

- Внешние ответы идут по исходному соединению; внутренняя reason-категория не выдаётся DSP.
- Уведомления и события — отдельный поток после ставки, а не часть 65-мс внутреннего бюджета.
- Успешный local rendering подтверждает синтаксис и видимость схем, но не работу gateway, финансового протокола или SLA под нагрузкой.

Проверено 24 октября 2026 года: все пять исходников скомпилированы в SVG/PNG через установленный Java 21 и PlantUML расширения; component view отрендерен с заблокированными HTTP/HTTPS-запросами. PNG просмотрены, подписи sequence-диаграмм расширены для читаемости. Проверены ссылки документов, JSON-примеры API, идентификаторы/связи component view и SHA-256 локальной библиотеки.
