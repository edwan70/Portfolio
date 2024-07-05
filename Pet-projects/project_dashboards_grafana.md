## ПРОЕКТ: расширение функционала системы мониторинга (snmp manager Octopus).

### Цели: 
- реализовать выгрузку и преобразование данных из имеющихся источников (MiniLink.exe - текстовый файл, snmp manager Octopus - БД MS SQL Server Express Edition, MonSRK service - БД MS SQL Server Express Edition) и помещение их в БД MS SQL Server Enterprise Edition, связанную с Grafana.
- сделать визуализацию значений уровня сигнала радиорелейных линий (РРЛ) за интересующие промежутки времени и за весь период наблюдения (линейные графики).
- реализовать визуализацию распределения значений уровня сигнала радиорелейных линий (РРЛ) (гистограммы).
- разработать визуализацию статусов АДГ (дата время запуска аварийных дизель-генераторов (АДГ)).
- разработать визуализацию статусов посещений РТП (дата время открытия дверей контейнеров РТП). 
- построить дашборды температур и напряжений радиотехнических постов (РТП).
- построить дашборд - таблица текущих значений уровня сигнала РРЛ.
- построить дашборд - таблица переключения и работы сплитсистем на РТП.
- сделать визуализацию тревог по АИС (автоматическая идентификационная система).
- построить дашборды с основной информацией по БД.


### Схема выгрузки данных.
![alt Схема выгрузки данных.](https://github.com/edwan70/Datasets/blob/main/Technical_Diagrams.jpg?raw=true)

### Дашборды главная страница.
![alt Главная страница](https://github.com/edwan70/Datasets/blob/main/dashboards_grafana.jpg?raw=true)

### Примеры дашбордов "Уровни сигнала РРЛ".
`Уровни сигнала РРЛ Геленджик.`
![alt Уровни сигнала РРЛ Геленджик.](https://github.com/edwan70/Datasets/blob/main/RRL_Gelen.jpg?raw=true)
`Уровни сигнала РРЛ Пенай.`
![alt Уровни сигнала РРЛ Пенай.](https://github.com/edwan70/Datasets/blob/main/RRL_Penay.jpg?raw=true)

### Пример дашборда "Гистограммы и мин, макс, средние значения".
![alt Гистограммы и мин, макс, средние значения.](https://github.com/edwan70/Datasets/blob/main/RRL_Gelen_hist.jpg?raw=true)

### Пример настройка запроса дашборда "Уровни сигнала РРЛ".
![alt Уровни сигнала РРЛ - настройка запроса.](https://github.com/edwan70/Datasets/blob/main/RRL_Gelen_sql.jpg?raw=true)

### Пример дашборда "Уровни сигнала РРЛ" таблица текущих значений.
![alt Уровни сигнала РРЛ таблица текущих значений.](https://github.com/edwan70/Datasets/blob/main/RRL_tables.jpg?raw=true)

### Пример дашборда "Тревоги АИС".
![alt Тревоги АИС.](https://github.com/edwan70/Datasets/blob/main/ais_alarms.jpg?raw=true)

### Пример дашборда "Тревоги АДГ".
![alt Тревоги АДГ.](https://github.com/edwan70/Datasets/blob/main/adg_alarms.jpg?raw=true)

### Примеры дашбордов "Посещение РТП, открытие дверей".
`РТП Ю. Озереевка энергоконтейнер.`
![alt РТП Ю. Озереевка энергоконтейнер.](https://github.com/edwan70/Datasets/blob/main/door_energo_alarms.jpg?raw=true)
`РТП Геленджик радарконтейнер.`
![alt РТП Геленджик радарконтейнер.](https://github.com/edwan70/Datasets/blob/main/door_alarms.jpg?raw=true)

### Пример дашборда "Температуры и напряжения".
![alt Температуры и напряжения.](https://github.com/edwan70/Datasets/blob/main/Ozer_T_U.jpg?raw=true)

### Пример дашборда "Переключение и работа сплитсистем" таблица.
![alt Переключение и работа сплитсистем.](https://github.com/edwan70/Datasets/blob/main/cond_intervals.jpg?raw=true)

### Пример дашборда "Информация о БД".
![alt Информация о БД.](https://github.com/edwan70/Datasets/blob/main/db_info.jpg?raw=true)
