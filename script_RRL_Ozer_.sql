
USE [SystemMonitoring]

GO
-- Выборка уровень сигнала по РРЛ Южная Озереевка


-- РТП "Ю. Озереевка"

-- Сайт Центр1 - РТПС1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Центр-КРТПЦ'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj25'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '13'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='7')
  -- ORDER BY [DataLog].[DateTime] DESC 
UNION ALL
-- Сайт Центр2 - РТПС2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Центр2-КРТПЦ2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj25'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '14'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='8')
  -- ORDER BY [DataLog].[DateTime] DESC

-- Сайт РТПС1 - Центр1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'КРТПЦ-Центр'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj28'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '15'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='9')
  -- ORDER BY [DataLog].[DateTime] DESC
UNION ALL
-- Сайт РТПС2 - Центр2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'КРТПЦ2-Центр2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj28'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '16'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='10')
  -- ORDER BY [DataLog].[DateTime] DESC

-- Сайт РТПС1 - Абрау1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'КРТПЦ-Абрау'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj28'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '17'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='11')
  --ORDER BY [DataLog].[DateTime] DESC
UNION ALL
-- Сайт РТПС2 - Абрау2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'КРТПЦ2-Абрау2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj28'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '18'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='12')
  -- ORDER BY [DataLog].[DateTime] DESC

-- Сайт Абрау1 - РТПС1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Абрау-КРТПЦ'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj27'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '19'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='9')
  -- ORDER BY [DataLog].[DateTime] DESC
UNION ALL
-- Сайт Абрау2 - РТПС2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Абрау2-КРТПЦ2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
	 --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj27'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '20'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='10')
  -- ORDER BY [DataLog].[DateTime] DESC

  -- Сайт Абрау1 - Озереевка1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Абрау-Озер'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
	 --, [DataSamples].[Type]
	 --, [DataSamples].[Description]
     , [DataSamples].[Unit]
	 --, [RecKinds].[Name]
	 --, [RecKinds].[Unit]
         --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj27'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '21'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='11')
  -- ORDER BY [DataLog].[DateTime] DESC 
UNION ALL
-- Сайт Абрау2 - Озереевка2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Абрау2-Озер2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
        --, [DataSamples].[Type]
        --, [DataSamples].[Description]
     , [DataSamples].[Unit]
        --, [RecKinds].[Name]
        --, [RecKinds].[Unit]
        --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj27'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '22'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='12')
  -- ORDER BY [DataLog].[DateTime] DESC
  
-- Сайт Озереевка1 - Абрау1
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Озер-Абрау'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
       --, [DataSamples].[Type]
       --, [DataSamples].[Description]
     , [DataSamples].[Unit]
       --, [RecKinds].[Name]
       --, [RecKinds].[Unit]
       --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj26'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '23'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='7')
  -- ORDER BY [DataLog].[DateTime] DESC  
UNION ALL
-- Сайт Озереевка2 - Абрау2
SELECT TOP 1 
     --[DataLog].[Id]
     --[DataLog].[SampleID]
       [DataLog].[DateTime]
     , [DataLog].[Object] AS 'Озер2-Абрау2'
     --, [DataLog].[Type]
     --, [DataLog].[Value]
     , CONVERT(decimal(10, 1),[DataLog].[ValuePerSec]) AS 'Значение'
     , [DataSamples].[SourceElement]
      --, [DataSamples].[Type]
      --, [DataSamples].[Description]
     , [DataSamples].[Unit]
      --, [RecKinds].[Name]
      --, [RecKinds].[Unit]
      --, [RecKinds].[Element]
FROM [dbo].[DataLog]
INNER JOIN [SystemMonitoring].[dbo].[DataSamples] on [SystemMonitoring].[dbo].[DataLog].[SampleID] = [SystemMonitoring].[dbo].[DataSamples].[Id] 
WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] = 
(SELECT max([SystemMonitoring].[dbo].[DataLog].[DateTime]) 
FROM [dbo].[DataLog]
  WHERE [SystemMonitoring].[dbo].[DataLog].[DateTime] <= GETDATE()
  AND [SystemMonitoring].[dbo].[DataLog].[Object] = 'MonObj26'
  AND [SystemMonitoring].[dbo].[DataLog].[SampleID] = '24'
  AND [SystemMonitoring].[dbo].[DataLog].[Type] ='8')
  
  -- ORDER BY [DataLog].[DateTime] DESC  
GO
  
