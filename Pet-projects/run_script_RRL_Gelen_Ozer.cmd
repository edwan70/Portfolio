
@ECHO OFF

ECHO Runing SQL script c:\run_SQL_scripts\script_RRL_Gelen_Ozer.sql 
ECHO See file c:\sql\my_RRL_Gelen_Ozer.txt



sqlcmd -S OCTOPUS\SQLEXPRESS_2012 -i c:\run_SQL_scripts\script_RRL_Gelen_Ozer.sql -o c:\sql\my_RRL_Gelen_Ozer.txt

notepad.exe c:\sql\my_RRL_Gelen_Ozer.txt