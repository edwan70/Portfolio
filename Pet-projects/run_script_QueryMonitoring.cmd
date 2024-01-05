
@ECHO OFF

ECHO Runing SQL script c:\run_SQL_scripts\script_QueryMonitoring.sql
ECHO See file c:\sql\my_QueryMonitoring.txt



sqlcmd -S OCTOPUS\SQLEXPRESS_2012 -i c:\run_SQL_scripts\script_QueryMonitoring.sql -o c:\sql\my_QueryMonitoring.txt

notepad.exe c:\sql\my_QueryMonitoring.txt