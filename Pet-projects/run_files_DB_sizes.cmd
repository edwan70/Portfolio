
@ECHO OFF

ECHO Runing SQL script c:\run_SQL_scripts\files_DB_sizes.sql
ECHO See file c:\sql\files_DB_sizes.txt

sqlcmd -S OCTOPUS\SQLEXPRESS_2012 -i c:\run_SQL_scripts\files_DB_sizes.sql -o c:\sql\files_DB_sizes.txt

notepad.exe c:\sql\files_DB_sizes.txt