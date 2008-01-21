@echo off
set NB_HOME=<Set Netbeans Home e.g. D:\JavaCAPS52\netbeans>
set JAVA_HOME=<Set Java Home e.g. D:\Software\jre1.5.0_11>
set PATH=%JAVA_HOME%\bin;%PATH%

set BLK=%CD%
set USER_LIBS=%BLK%\lib\avalon-framework-4.1.3.jar;%BLK%\lib\axion-1.0.jar;%BLK%\lib\etl-editor-1.0.jar;%BLK%\lib\etl-engine-1.0.jar;%BLK%\lib\ETLEngineInvoker-1.0.jar;%BLK%\lib\i18n-1.0.jar;%BLK%\lib\ojdbc14-10.1.0.2.0.jar;%BLK%\lib\org-netbeans-modules-db-1.0.jar;%BLK%\lib\velocity-1.4.jar;%BLK%\lib\velocity-dep-1.4.jar;%BLK%\bulkloader-1.0.jar

set OPENIDE_LIB_MODULE=%NB_HOME%\platform7\modules
set OPENIDE_LIB_LIB=%NB_HOME%\platform7\lib
set OPENIDE_LIB_CORE=%NB_HOME%\platform7\core
set OPENIDE_LIBS=%OPENIDE_LIB_MODULE%\org-openide-nodes.jar;%OPENIDE_LIB_MODULE%\org-openide-text.jar;%OPENIDE_LIB_MODULE%\org-openide-loaders.jar;%OPENIDE_LIB_MODULE%\org-openide-windows.jar;%OPENIDE_LIB_MODULE%\org-openide-dialogs.jar;%OPENIDE_LIB_MODULE%\org-openide-awt.jar;%OPENIDE_LIB_CORE%\org-openide-filesystems.jar;%OPENIDE_LIB_LIB%\org-openide-util.jar

set ALL_LIBS=%USER_LIBS%;%OPENIDE_LIBS%;%CLASSPATH%

REM #### SOURCE DATABASE ####
set SOURCE_LOC=<Specify Source Dir e.g. D:\temp\mural\masterindextest>
set FIELD_DELIMITER="|"
set RECORD_DELIMITER="$$$"

REM #### TARGET DATABASE ####
set TARGET_LOC=<DataBase Host/IP e.g. localhost>
set TARGET_PORT=<Specify Port No e.g. 1521>
set TARGET_SID=<Specify Sid e.g. orcl>
set TARGET_CATALOG=<Specify Catalog e.g. OE, "">
set TARGET_LOGIN=<Specify Target DB Login>
set TARGET_PW=<Specify Target DB Passwd>

java -Xms128m -Xmx512m -Dsourcedb.loc=%SOURCE_LOC% -Dfield.delimiter=%FIELD_DELIMITER% -Drecord.delimiter=%RECORD_DELIMITER% -Dtarget.host=%TARGET_LOC% -Dtarget.port=%TARGET_PORT% -Dtarget.sid=%TARGET_SID% -Dtarget.catalog=%TARGET_CATALOG% -Dtarget.login=%TARGET_LOGIN% -Dtarget.pw=%TARGET_PW% -cp %ALL_LIBS% com.sun.dm.di.bulkloader.LoaderMain