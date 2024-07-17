
@ECHO OFF

ECHO Runing SQL script C:\Backup_SRK_Monitoring\script_backup_srk.sql
 
ECHO See file C:\Backup_SRK_Monitoring\log_srk.txt

del C:\Backup_SRK_Monitoring\log_srk.txt

sqlcmd -S OCTOPUS\SQLEXPRESS_2012 -i C:\Backup_SRK_Monitoring\script_backup_srk.sql -o C:\Backup_SRK_Monitoring\log_srk.txt

rem notepad.exe C:\Backup_SRK_Monitoring\log_srk.txt

exit