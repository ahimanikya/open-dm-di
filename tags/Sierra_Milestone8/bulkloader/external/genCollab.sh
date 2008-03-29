#!/bin/sh

# ********************************
# * BULK LOADER SETTINGS [START] *
# ********************************
	# #### NETBEANS AND JAVA ####
NB_HOME=D:/LatestNetBeans/NetBeans6.0.1
JAVA_HOME=D:/Software/jre1.5.0_11
DB_DRIVER_PATH=D:/JavaCAPS6/glassfish/javadb/lib
DB_DRIVER_NAME=derbyclient.jar

	# #### SOURCE DATABASE ####
SOURCE_LOC=D:/temp/bl_derbytest/data
FIELD_DELIMITER="|"
RECORD_DELIMITER="$$$"

	# #### TARGET DATABASE ####
# Specify from following options (ORACLE=1, DERBY=2)
    TARGET_DB_TYPE=2
TARGET_LOC=localhost
TARGET_PORT=1527
# Note : Specify ID as 'SID'(SystemId) for Oracle, 'DB Name' for Derby
    TARGET_ID=sample
TARGET_SCHEMA=APP
TARGET_CATALOG=
TARGET_LOGIN=app	
TARGET_PW=app
# ********************************
# * BULK LOADER SETTINGS [END] *
# ********************************

# *****************************
#   DO NOT EDIT THIS [START]
# *****************************
RUNSTAT=START
DB_DRIVER_JAR=$DB_DRIVER_PATH/$DB_DRIVER_NAME
if NOT exist $DB_DRIVER_JAR$ GOTO GetExit
copy $DB_DRIVER_JAR ./lib/$DB_DRIVER_NAME
PATH=$JAVA_HOME/bin;$PATH
BLK=$CD
USER_LIBS=$BLK/lib/avalon-framework-4.1.3.jar;$BLK/lib/axion-1.0.jar;$BLK/lib/etl-editor-1.0.jar;$BLK/lib/etl-engine-1.0.jar;$BLK/lib/ETLEngineInvoker-1.0.jar;$BLK/lib/i18n-1.0.jar;$BLK/lib/ojdbc14-10.1.0.2.0.jar;$BLK/lib/org-netbeans-modules-db-1.0.jar;$BLK/lib/velocity-1.4.jar;$BLK/lib/velocity-dep-1.4.jar;$BLK/bulkloader-1.0.jar
OPENIDE_LIB_MODULE=$NB_HOME/platform7/modules
OPENIDE_LIB_LIB=$NB_HOME/platform7/lib
OPENIDE_LIB_CORE=$NB_HOME/platform7/core
OPENIDE_LIB_IDE8_MOD=$NB_HOME/ide8/modules
OPENIDE_LIBS=$OPENIDE_LIB_MODULE/org-openide-nodes.jar;$OPENIDE_LIB_MODULE/org-openide-text.jar;$OPENIDE_LIB_MODULE/org-openide-loaders.jar;$OPENIDE_LIB_MODULE/org-openide-windows.jar;$OPENIDE_LIB_MODULE/org-openide-dialogs.jar;$OPENIDE_LIB_MODULE/org-openide-awt.jar;$OPENIDE_LIB_CORE/org-openide-filesystems.jar;$OPENIDE_LIB_LIB/org-openide-util.jar;$OPENIDE_LIB_IDE8_MOD/org-netbeans-modules-db.jar
ALL_LIBS=$USER_LIBS;$OPENIDE_LIBS;$DB_DRIVER_JAR;$CLASSPATH
java -Xms128m -Xmx512m -Dsourcedb.loc=$SOURCE_LOC -Dfield.delimiter=$FIELD_DELIMITER -Drecord.delimiter=$RECORD_DELIMITER -Dtarget.type=$TARGET_DB_TYPE -Dtarget.host=$TARGET_LOC -Dtarget.port=$TARGET_PORT -Dtarget.id=$TARGET_ID -Dtarget.schema=$TARGET_SCHEMA -Dtarget.catalog=$TARGET_CATALOG -Dtarget.login=$TARGET_LOGIN -Dtarget.pw=$TARGET_PW -cp $ALL_LIBS com.sun.dm.di.bulkloader.LoaderMain
RUNSTAT=SUCCESS
:GetExit
if ($RUNSTAT) == (START) echo Unable to Locate Database Driver on Specified Path. Check Path : $DB_DRIVER_JAR
# *****************************
#   DO NOT EDIT THIS [END]
# *****************************
