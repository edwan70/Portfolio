USE [SystemMonitoring]

GO
-- Кондиционирование РТП

-- Сплиты и температуры РТП Пенай

  --РТП Пенай

select *
from
(SELECT TOP 3
       DateTime AS Start_DateTime
	  ,CONVERT(char(20),'РК') AS 'РТП Пенай'
	  ,LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc) AS End_DateTime
      ,CONVERT(char(36),[Message]) AS Message
	  ,datediff(MINUTE,  DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60/24%24 AS 'Дни'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60%24 AS 'Часы'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Минуты'
	  ,DATEDIFF(SECOND,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Секунды'
FROM [SystemMonitoring].[dbo].[MainMessageLog]
where [SystemMonitoring].[dbo].[MainMessageLog].[Object] = 'MonObj51'
and (cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) = 'Кондиционер №1, Работа: Включен'
or cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) ='Кондиционер №1, Работа: Выключен')
UNION ALL 
SELECT TOP 3
       DateTime AS Start_DateTime
	  ,CONVERT(char(20),'РК') AS 'РТП Пенай'
	  ,LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc) AS End_DateTime
      ,CONVERT(char(36),[Message]) AS Message
	  ,datediff(MINUTE,  DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60/24%24 AS 'Дни'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60%24 AS 'Часы'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Минуты'
	  ,DATEDIFF(SECOND,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Секунды'
FROM [SystemMonitoring].[dbo].[MainMessageLog]
where [SystemMonitoring].[dbo].[MainMessageLog].[Object] = 'MonObj51'
and (cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) = 'Кондиционер №2, Работа: Включен'
or cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) ='Кондиционер №2, Работа: Выключен')
) AS tmp_penay
order by 1 desc, 4 asc;

SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Пенай'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj51' AND [SampleID] = '32')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Пенай'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj51' AND [SampleID] = '39')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Пенай'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj51' AND [SampleID] = '33');

-- Сплиты и температуры РТП Дооб

-- РТП Дооб

select *
from
(SELECT TOP 3
       DateTime AS Start_DateTime
	  ,CONVERT(char(20),'РК') AS 'РТП Дооб'
      ,LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc) AS End_DateTime
      ,CONVERT(char(36),[Message]) AS Message
	  ,datediff(MINUTE,  DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60/24%24 AS 'Дни'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60%24 AS 'Часы'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Минуты'
	  ,DATEDIFF(SECOND,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Секунды'
FROM [SystemMonitoring].[dbo].[MainMessageLog]
where [SystemMonitoring].[dbo].[MainMessageLog].[Object] = 'MonObj49'
and (cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) = 'Кондиционер №1, Работа: Включен'
or cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) ='Кондиционер №1, Работа: Выключен')
UNION ALL 
SELECT TOP 3
       DateTime AS Start_DateTime
	  ,CONVERT(char(20),'РК') AS 'РТП Дооб'
      ,LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc) AS End_DateTime
      ,CONVERT(char(36),[Message]) AS Message
	  ,datediff(MINUTE,  DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60/24%24 AS 'Дни'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))/60%24 AS 'Часы'
	  ,DATEDIFF(MINUTE,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Минуты'
	  ,DATEDIFF(SECOND,DateTime, LAG(DateTime, 1, GETDate()) OVER(order by DateTime desc))%60 AS 'Секунды'
FROM [SystemMonitoring].[dbo].[MainMessageLog]
where [SystemMonitoring].[dbo].[MainMessageLog].[Object] = 'MonObj49'
and (cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) = 'Кондиционер №2, Работа: Включен'
or cast([SystemMonitoring].[dbo].[MainMessageLog].[Message] as char(40)) ='Кондиционер №2, Работа: Выключен')
) AS tmp_doob
order by 1 desc, 4 asc;

SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Дооб'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj49' AND [SampleID] = '32')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Дооб'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj49' AND [SampleID] = '38')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20),'РК') AS 'РТП Дооб'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit] AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj49' AND [SampleID] = '33');

-- Сплиты и температуры РТП Геледжик
with gelen_cond as
  (SELECT TOP 1000 [id]
      ,[device_id]
      ,[date]
      --,[t]
      --,[U]
      ,[cond1Status]
      ,[cond2Status]
      --,[cond3Status]
	  ,LAG([cond1Status], 1, [cond1Status]) OVER(order by date desc) AS Previos_Cond1
	  ,LAG([cond2Status], 1, [cond2Status]) OVER(order by date desc) AS Previos_Cond2
  FROM [SRK].[dbo].[MonitorData]
  WHERE[SRK].[dbo].[MonitorData].[device_id] = 1
  AND cond1Status in ('2','3')
  AND cond2Status in ('2','3')
  AND date >= GETDate()-3
  order by id desc),
  gelen_finish as 
  (select [date] as DateTime
         , CONVERT(char(20), 'РК') as 'РТП Геленджик'
		 , LAG(date, 1, GETDate()) OVER(order by date desc) AS Previos_DateTime
		 --, [cond1Status] as cond1
         --, [cond2Status] as cond2
		 ,case when cond1Status = '2' then 'Вкл.' else 'Выкл.' end as 'Сплит1'
         ,case when cond2Status = '2' then 'Вкл.' else 'Выкл.' end as 'Сплит2'
         --,datediff(hour,  date, LAG(date, 1, GETDate()) OVER(order by date desc)) AS 'Интервал(часы)'
		 ,datediff(MINUTE,  date, LAG(date, 1, GETDate()) OVER(order by date desc))/60/24%24 AS 'Дни'
		 ,DATEDIFF(MINUTE,date, LAG(date, 1, GETDate()) OVER(order by date desc))/60%24 AS 'Часы'
	     ,DATEDIFF(MINUTE,date, LAG(date, 1, GETDate()) OVER(order by date desc))%60 AS 'Минуты'
	     ,DATEDIFF(SECOND,date, LAG(date, 1, GETDate()) OVER(order by date desc))%60 AS 'Секунды'
    from gelen_cond
	WHERE ([cond1Status] != Previos_Cond1
	AND  [cond2Status] != Previos_Cond2)
	OR ([cond1Status] = [cond2Status]
	AND  Previos_Cond1 != Previos_Cond2))
select * 
from gelen_finish
;
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20), 'РК') AS 'РТП Геленджик'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , CONVERT(char(10),[DataSamples].[Unit]) AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj84' AND [SampleID] = '35')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20), 'РК') AS 'РТП Геленджик'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , CONVERT(char(10), [DataSamples].[Unit]) AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj84' AND [SampleID] = '34')
UNION ALL
SELECT TOP 1 --[id]
       [date] as 'DateTime'
      , CONVERT(char(20),'СРК') as 'РТП Геленджик'
      , CONVERT(real, [t]) as 'Значение'
      , CONVERT(char(20),'Температура') AS 'Параметр'
	  , CONVERT(char(10),'°C') AS 'Единица измерения'
	  --,[U]
FROM [SRK].[dbo].[MonitorData]
WHERE [SRK].[dbo].[MonitorData].[id] = 
     (SELECT max([SRK].[dbo].[MonitorData].[id])
      FROM [SRK].[dbo].[MonitorData]
      WHERE [SRK].[dbo].[MonitorData].[device_id] = 1)
AND [SRK].[dbo].[MonitorData].[device_id] = 1
order by 4 asc, 2 asc;

-- Сплиты и температуры РТП Ю.Озереевка радар-контейнер
  with ozer_cond as
  (SELECT TOP 1000 [id]
      ,[device_id]
      ,[date]
      --,[t]
      --,[U]
      ,[cond1Status]
      ,[cond2Status]
      --,[cond3Status]
	  ,LAG([cond1Status], 1, [cond1Status]) OVER(order by date desc) AS Previos_Cond1
	  ,LAG([cond2Status], 1, [cond2Status]) OVER(order by date desc) AS Previos_Cond2
  FROM [SRK].[dbo].[MonitorData]
  WHERE[SRK].[dbo].[MonitorData].[device_id] = 0
  AND cond1Status in ('2','3')
  AND cond2Status in ('2','3')
  AND date >= GETDate()-3
  order by id desc),
  ozer_finish as 
  (select [date] as DateTime
         , CONVERT(char(20), 'РК') as 'РТП Ю.Озереевка'
		 , LAG(date, 1, GETDate()) OVER(order by date desc) AS Previos_DateTime
		 --, [cond1Status] as cond1
         --, [cond2Status] as cond2
		 ,case when cond1Status = '2' then 'Вкл.' else 'Выкл.' end as 'Сплит1'
         ,case when cond2Status = '2' then 'Вкл.' else 'Выкл.' end as 'Сплит2'
         --,datediff(hour,  date, LAG(date, 1, GETDate()) OVER(order by date desc)) AS 'Интервал(часы)'
		 ,datediff(MINUTE,  date, LAG(date, 1, GETDate()) OVER(order by date desc))/60/24%24 AS 'Дни'
		 ,DATEDIFF(MINUTE,date, LAG(date, 1, GETDate()) OVER(order by date desc))/60%24 AS 'Часы'
	     ,DATEDIFF(MINUTE,date, LAG(date, 1, GETDate()) OVER(order by date desc))%60 AS 'Минуты'
	     ,DATEDIFF(SECOND,date, LAG(date, 1, GETDate()) OVER(order by date desc))%60 AS 'Секунды'
    from ozer_cond
	WHERE ([cond1Status] != Previos_Cond1
	AND  [cond2Status] != Previos_Cond2)
	OR ([cond1Status] = [cond2Status]
	AND  Previos_Cond1 != Previos_Cond2))
select *
from ozer_finish
;
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20), 'РК') AS 'РТП Ю.Озереевка'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , CONVERT(char(10), [DataSamples].[Unit]) AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj4' AND [SampleID] = '26')
UNION ALL
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20), 'РК') AS 'РТП Ю.Озереевка'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , CONVERT(char(10), [DataSamples].[Unit]) AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj4' AND [SampleID] = '25')
UNION ALL
SELECT TOP 1 --[id]
       [date] as 'DateTime'
      , CONVERT(char(20), 'СРК') as 'РТП Ю.Озереевка'
      , CONVERT(real, [t]) as 'Значение'
      , CONVERT(char(20), 'Температура') AS 'Параметр'  
	  , CONVERT(char(10), '°C') AS 'Единица измерения'
	  --,[U]
  FROM [SRK].[dbo].[MonitorData]
  WHERE [SRK].[dbo].[MonitorData].[id] = 
        (SELECT max([SRK].[dbo].[MonitorData].[id])
        FROM [SRK].[dbo].[MonitorData]
        WHERE [SRK].[dbo].[MonitorData].[device_id] = 0)
  AND [SRK].[dbo].[MonitorData].[device_id] = 0
  order by 4 asc, 2 asc;

-- Сплиты и температуры РТП Ю.Озереевка энергоконтейнер
SELECT TOP 1
     --[DataLog].[Id]
     --, [DataLog].[SampleID]
       [DataLog].[DateTime]
     , CONVERT(char(20), 'Энергоконтейнер') AS 'РТП Ю.Озереевка'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , [DataLog].[ValuePerSec] AS 'Значение'
	 , CONVERT(char(20), [DataSamples].[SourceElement]) AS 'Параметр'
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , CONVERT(char(10),[DataSamples].[Unit]) AS 'Единица измерения'
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
 FROM [dbo].[DataLog]
 INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
 WHERE [SystemMonitoring].[dbo].[DataLog].[Id] = 
        (SELECT max([SystemMonitoring].[dbo].[DataLog].[Id])
        FROM [dbo].[DataLog]
        WHERE [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj5' AND [SampleID] = '27')


  GO