@echo off
set JAVA_HOME=C:\Alaska\jdk1.5.0_09
set PATH=%JAVA_HOME%\bin;%PATH%

REM #### SOURCE DATABASE ####
set SOURCE_LOC=Specify Source Dir
set FIELD_DELIMITER="|"
set RECORD_DELIMITER="$$$"

REM #### TARGET DATABASE ####
set TARGET_LOC=DataBase Host/IP
set TARGET_PORT=Port No
set TARGET_SID=Sid
set TARGET_CATALOG=""
set TARGET_LOGIN=""
set TARGET_PW=""

java -Xms128m -Xmx512m -Dsourcedb.loc=%SOURCE_LOC% -Dfield.delimiter=%FIELD_DELIMITER% -Drecord.delimiter=%RECORD_DELIMITER% -Dtarget.host=%TARGET_LOC% -Dtarget.port=%TARGET_PORT% -Dtarget.sid=%TARGET_SID% -Dtarget.catalog=%TARGET_CATALOG% -Dtarget.login=%TARGET_LOGIN% -Dtarget.pw=%TARGET_PW% -jar bulkloader-1.0.jar