# C4-PlantUML v2.12.0

Локальная копия официальной библиотеки для автономного рендеринга диаграмм AdScale. Загружена 24 октября 2026 года из [релиза v2.12.0](https://github.com/plantuml-stdlib/C4-PlantUML/releases/tag/v2.12.0). Лицензия: [MIT](LICENSE).

Файлы сохранены без изменений из `https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/v2.12.0/`:

| Файл | SHA-256 |
|---|---|
| C4.puml | `ee4f85b5c31104155e14059d942831c3361cddd5009e22d66b1242060464bf7d` |
| C4_Context.puml | `5c0c2a05b9f5e443447cd874a8f3923674390e5b2ff07d0db8e0de2bde872035` |
| C4_Container.puml | `65fb59e1504da77ba0c32ddc6aa75179bfa28882143ddb23a2f0b11da6e6b596` |
| LICENSE | `9faafe39848fc89e1e17fdb9b4c5ce87a843981cfc341d910733d9f406a70bd7` |

Цепочка зависимостей: `C4_Container.puml` → `C4_Context.puml` → `C4.puml`. Схемы задают `!define RELATIVE_INCLUDE` перед подключением библиотеки, поэтому выбираются локальные ветви include. Сетевые fallback-ветви в оригинальных файлах библиотеки не выполняются.

При обновлении нужно заменить все три файла одной версией, проверить лицензию, обновить SHA-256 и повторно отрендерить обе схемы. Для Container view дополнительные файлы библиотеки не нужны.
