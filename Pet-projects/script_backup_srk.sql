use SRK
--path - переменная, в которой хранится путь к файлу бэкапа
--ШАГ 1 - Создание бэкапа в указанном каталоге
declare @path varchar(max)=N'C:\Backup_SRK_Monitoring\srk\SRK_backup_'+convert(varchar(max),getdate(),112) + N'.bak'
BACKUP DATABASE [SRK] TO  DISK = @path WITH NOFORMAT, NOINIT,  NAME = N'SRK-Полная База данных Резервное копирование', SKIP, NOREWIND, NOUNLOAD,  STATS = 10
GO
--ШАГ 2 - Проверка резервной копии
declare @backupSetId as int
declare @path varchar(max)=N'C:\Backup_SRK_Monitoring\srk\SRK_backup_'+convert(varchar(max),getdate(),112) + N'.bak'
select @backupSetId = position from msdb..backupset where database_name=N'SRK' and backup_set_id=(select max(backup_set_id) from msdb..backupset where database_name=N'SRK' )
if @backupSetId is null begin raiserror(N'Ошибка верификации. Сведения о резервном копировании для базы данных "SRK" не найдены.', 16, 1) end
RESTORE VERIFYONLY FROM  DISK = @path WITH  FILE = @backupSetId,  NOUNLOAD,  NOREWIND
--Вместо DATABASE_NAME нужно указать имя вашей базы данных, а также указать свой путь к каталогу с файлами бэкапов.
GO