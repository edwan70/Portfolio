
@ECHO OFF

ECHO Runing SQL script C:\Backup_SRK_Monitoring\script_backup_monitoring.sql
 
ECHO See file C:\Backup_SRK_Monitoring\log_monitoring.txt

del C:\Backup_SRK_Monitoring\log_monitoring.txt

sqlcmd -S OCTOPUS\SQLEXPRESS_2012 -i C:\Backup_SRK_Monitoring\script_backup_monitoring.sql -o C:\Backup_SRK_Monitoring\log_monitoring.txt

rem notepad.exe C:\Backup_SRK_Monitoring\log_monitoring.txt

exit