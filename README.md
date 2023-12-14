# Загрузчик ГАР
[![Python package](https://github.com/Feosen/django-fias/actions/workflows/dev.yml/badge.svg?branch=dev)](https://github.com/Feosen/django-fias/actions/workflows/dev.yml)

Приложение работы с выгрузкой ГАР в Django.

## Основные возможности
* Импорт базы ГАР:
    * из архива XML,
    * из каталога с XML,
    * напрямую с сайта http://fias.nalog.ru в формате XML.
* Возможность хранить данные в отдельной БД.

## Некоторые особенности
* Часть справочников импортируется независимо от настроек: вся статусная информация, типы адресных объектов, таблица AddrObj
* Все справочники и классификаторы связаны между собой посредством ForeignKey, что требует консистентного состояния БД ФИАС. В реальной жизни такого не бывает, поэтому:
* В случае отсутствия родительского поля для ForeignKey включается механизм регрессивного импорта:
    * При возникновении любой ошибки пачка объектов делится на части и каждая часть импортируется отдельно
    * При повторном возникновении ошибки часть с ошибкой снова делится и импортируется.
    * Так повторяется, пока в пачке не останется один объект, который просто отбрасывается.
    Таким образом достигается минимальная просадка производительности импорта при возникновении ошибок.

## Использование
1. Клонировать репозиторий.
2. Проверить, что установлены Python 3.11 и [Poetry](https://python-poetry.org/docs/).
3. Установить зависимости (*выполнить из корня проекта*): 
```sh
poetry install --no-dev
```
4. Настроить проект (см. [настройка](#Настройка)).
5. Выполнить [миграцию](#Миграция) баз данных.
6. Загрузить или обновить данные ГАР в служебные таблицы с помощью команды [fias](#fias)
7. Заполнить или обновить целевые таблицы с помощью команды [target](#target).

## Разработка

Все зависимости устанавливаются так:
```sh
poetry install
```
Запуск форматирования, проверки типов и тестов через tox
```sh
poetry run tox
```

## Настройка

### Параметры
В файле [gar_loader/settings.py](gar_loader/settings.py)
#### DATABASES
Указываются параметры подключения к двум базам: default (на данный момент практически не используется)
и gar (в ней хранятся служебные и целевые таблицы).
#### FIAS_UNRAR_TOOL
Переменная хранит путь до unrar.exe, если её не задавать, приложение будет искать unrar (либо unrar.exe в случае
использования MS Windows) в глобальных переменных.
#### FIAS_REGIONS
Перечень обрабатываемых регионов. Принимает либо кортеж отдельных регионов (строки двузначных чисел)
```("01", "03", "99")``` либо строку ```"__all__"```, если необходимо обрабатывать все.
#### FIAS_HOUSE_TYPES
Перечень обрабатываемых типов домов. Принимает либо кортеж отдельных типов ```(2, 5, 7, 10)```
либо строку ```"__all__"```, если необходимо обрабатывать все.
#### FIAS_VALIDATE_HOUSE_PARAM_IDS
ID типов параметров для проверки командой validate_house_params, по умолчанию (6, 7) - ОКАТО и ОКТМО.
#### FIAS_STORE_INACTIVE_TABLES
Названия таблиц, для которых сохраняются записи с аттрибутом ISACTIVE = False. По умолчанию add_house_type и house_type.
#### TARGET_MANAGE
Указывает приложению, создавать ли целевые таблицы во время миграции (True) или пользователь создаёт их самостоятельно.
#### TARGET_LOAD_HOUSE_78_ONLY
Если True - заполняем только gar_house_78, а gar_house останется пустой.
#### TARGET_LOAD_HOUSE_BULK_SIZE
Если меньше либо равно 0 - заполняем gar_house_78 и gar_house сразу всеми записями, любое целое больше 0 - заполняем gar_house_78 и gar_house группами не более указанного количества строк.

### Проверка настройки
Проверить настройку можно, запустив тесты
```sh
poetry run manage.py test
```

## Миграция
Выполняется следующими командами
```sh
poetry run manage.py migrate
poetry run manage.py migrate --database=gar
```

## Команды
### fias
Загружает первичные данные из файлов ГАР в служебные таблицы либо обновляет их из дельта-файлов.
#### Ключи
`--src <path|filename|url|AUTO>`
    Путь до архива с БД ФИАС, каталога, в который предварительно был распакован архив, или URL-адрес,
    с которого требуется скачать импортируемый архив. Отсутствующее значение или значение AUTO означает автоматическое
    получение данных с сайта http://fias.nalog.ru.

`--truncate`
    Указывает полностью удалять все данные из таблицы перед импортом в неё

`--i-know-what-i-do`
    В случае если в БД уже есть какие-то данные, приложение не даст ничего импортировать, пока не будет указан этот ключ.
    На возможность обновления никак не влияет.

`--update`
    Обновляет БД ФИАС до актуальной версии (после или вместо импорта).
    Если в БД ничего ещё не импортировалось, будет выдано сообщение об ошибке.

`--skip`
    Используется только вместе с --update. Указывает пропускать повреждённые архивы с обновлениями.

`--format <xml>`
    Указывает, в каком формате скачивать архивы с данными в формате ГАР. Допустимые значения: xml.

`--limit`
    Устанавливает размер пачки записей, единовременно загружаемой в БД. Чем больше размер, тем быстрее импорт
    (в теории), но дольше обработка ошибок, если таковые возникнут. По умолчанию: 10000.

`--tables`
    Задаёт список таблиц для импорта через запятую.

`--update-version-info <yes|no>`
    Указывает, обновлять ли список версий БД ФИАС.
    По-умолчанию: yes

`--keep-indexes <yes|pk|no>`
    При первоначальном импорте удаляются все индексы из таблиц перед импортом и пересоздаются заново после.
    Ключ отключает такое поведение для всех индексов (yes) или только для первичных ключей (pk).
    На процесс обновления никак не влияет.

`--tempdir <path>`
    Путь к каталогу, где будут размещены временные файлы в процессе импорта.
    Каталог должен существовать и быть доступен для записи.

`--house-param-report <path>`
    Проверяет коды ОКАТО и ОКТМО и выгружает все некорректные в указанный файл *.CSV.
    Проверка выполняется для всех версий при первичной загрузке таблиц и только для новых записей при обновлении.

`--house-param-regions <str>`
    Перечень регионов для проверки ОКАТО и ОКТМО (разделяются пробелом).


#### Примеры использования
Первичная инициализация служебных таблиц из архива ГАР
```sh
poetry run manage.py fias --src G:\gar_xml.rar --tempdir G:\tmp --truncate --i-know-what-i-do --keep-indexes no
```

Обновление служебных таблиц из каталога с дельта-файлами без обновления информации о доступных версиях с
сайта http://fias.nalog.ru.
```sh
poetry run manage.py fias --src G:\deltas --tempdir G:\tmp --update --update-version-info no
```

### target
Загружает все первичные данные из служебных таблиц в целевые или обновляет их.
#### Ключи
`--truncate`
    Указывает полностью удалять все данные из таблицы перед импортом в неё

`--i-know-what-i-do`
    В случае если в БД уже есть какие-то данные, приложение не даст ничего импортировать, пока не будет указан этот ключ.
    На возможность обновления никак не влияет.

`--update`
    Обновляет целевые таблицы.
    Если в БД ничего ещё не импортировалось, будет выдано сообщение об ошибке.

`--keep-indexes`
    При первоначальном импорте удаляются все индексы из таблиц перед импортом и пересоздаются заново после.
    Ключ отключает такое поведение для всех индексов.
    На процесс обновления никак не влияет.

#### Примеры использования
Первичная инициализация целевых таблиц
```sh
poetry run manage.py target --truncate --i-know-what-i-do
```
Обновление целевых таблиц
```sh
poetry run manage.py target --update
```

### validate_house_params
Проверяет коды ОКАТО и ОКТМО и выгружает в файл *.CSV все некорректные.
#### Ключи
`--output <path>`
    Файл *.CSV отчёта.

`--min_ver <int>`
    Минимальная версия записей, для которых выполняется проверка.

`--regions`
    Список регионов, для которых выполняется проверка (разделяются пробелом).


#### Пример использования
Проверка параметров 50 и 78 регионов, начиная с версии 20221201.
```sh
poetry run manage.py validate_house_params --output D:\to_delete\ttt.csv --region 50 78 --min_ver 20221201
```


## Примечания и благодарности

За основу взят проект https://github.com/aldev12/django-fias
