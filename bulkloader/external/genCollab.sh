#!/bin/sh
clear

# ********************************
# * BULK LOADER SETTINGS [START] *
# ********************************

	# **** NETBEANS AND JAVA ****
NB_HOME=<Set Netbeans Home e.g. /openesb/netbeans-6.1-build-200803100002>
JAVA_HOME=<Set Java Home e.g. /usr/jdk/instances/jdk1.5.0/jre/bin>
DB_DRIVER_PATH=<Set DB Driver Path e.g. /openesb/glassfish-v2-ur1-b09d-patch-20080310/javadb/lib>
DB_DRIVER_NAME=<Set DB Driver Name e.g. derbyclient.jar>

	# **** SOURCE DATABASE ****
SOURCE_LOC=<Specify Source Dir e.g. /bl_derbytest/DATA>
FIELD_DELIMITER=|
RECORD_DELIMITER=$$$

	# **** TARGET DATABASE ****
# Specify from following options (ORACLE=1, DERBY=2)
    TARGET_DB_TYPE=2
TARGET_LOC=<DataBase Host/IP e.g. localhost>
TARGET_PORT=<Specify Port No e.g. 1527>
# Note : Specify ID as 'SID'(SystemId) for Oracle, 'DB Name' for Derby
    TARGET_ID=<Specify DB Name e.g. sample>
TARGET_SCHEMA=<Specify Schema e.g. APP, Blank for null>
TARGET_CATALOG=<Specify Catalog e.g. APP, Blank for null>
TARGET_LOGIN=<Specify Target DB Login>
TARGET_PW=<Specify Target DB Passwd>
# ********************************
# * BULK LOADER SETTINGS [END]   *
# ********************************

# *****************************
#   DO NOT EDIT THIS [START]
# *****************************

DB_DRIVER_JAR="$DB_DRIVER_PATH/$DB_DRIVER_NAME";
if [ -f $DB_DRIVER_JAR ] ; then

	cp $DB_DRIVER_JAR ./lib/$DB_DRIVER_NAME
	BLK=`pwd`

	USER_LIBS="$BLK/lib/avalon-framework-4.1.3.jar:$BLK/lib/axion-1.0.jar:$BLK/lib/etl-editor-1.0.jar:$BLK/lib/etl-engine-1.0.jar:$BLK/lib/ETLEngineInvoker-1.0.jar:$BLK/lib/i18n-1.0.jar:$BLK/lib/ojdbc14-10.1.0.2.0.jar:$BLK/lib/org-netbeans-modules-db-1.0.jar:$BLK/lib/velocity-1.4.jar:$BLK/lib/velocity-dep-1.4.jar:$BLK/bulkloader-1.0.jar"

	OPENIDE_LIB_MODULE=$NB_HOME/platform8/modules
	OPENIDE_LIB_LIB=$NB_HOME/platform8/lib
	OPENIDE_LIB_CORE=$NB_HOME/platform8/core
	OPENIDE_LIB_IDE8_MOD=$NB_HOME/ide9/modules

	OPENIDE_LIBS="$OPENIDE_LIB_MODULE/org-openide-nodes.jar:$OPENIDE_LIB_MODULE/org-openide-text.jar:$OPENIDE_LIB_MODULE/org-openide-loaders.jar:$OPENIDE_LIB_MODULE/org-openide-windows.jar:$OPENIDE_LIB_MODULE/org-openide-dialogs.jar:$OPENIDE_LIB_MODULE/org-openide-awt.jar:$OPENIDE_LIB_CORE/org-openide-filesystems.jar:$OPENIDE_LIB_LIB/org-openide-util.jar:$OPENIDE_LIB_IDE8_MOD/org-netbeans-modules-db.jar"
	ALL_LIBS="$USER_LIBS:$OPENIDE_LIBS:$DB_DRIVER_JAR:$CLASSPATH"
	
	$JAVA_HOME/java -Xms128m -Xmx512m -Dsourcedb.loc=$SOURCE_LOC -Dfield.delimiter=$FIELD_DELIMITER -Drecord.delimiter=$RECORD_DELIMITER -Dtarget.type=$TARGET_DB_TYPE -Dtarget.host=$TARGET_LOC -Dtarget.port=$TARGET_PORT -Dtarget.id=$TARGET_ID -Dtarget.schema=$TARGET_SCHEMA -Dtarget.catalog=$TARGET_CATALOG -Dtarget.login=$TARGET_LOGIN -Dtarget.pw=$TARGET_PW -Dmyjava.home=$JAVA_HOME -cp $ALL_LIBS com.sun.dm.di.bulkloader.LoaderMain

    else
	echo " Unable to locate Database Driver on Specified Path.";
	echo " Check Path - ${DB_DRIVER_JAR}";
	exit;
fi

# *****************************
#   DO NOT EDIT THIS [END]
# *****************************
