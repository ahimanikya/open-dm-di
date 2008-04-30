/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2003-2007 Sun Microsystems, Inc. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the Common 
 * Development and Distribution License ("CDDL")(the "License"). You 
 * may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * https://open-dm-mi.dev.java.net/cddl.html
 * or open-dm-mi/bootstrap/legal/license.txt. See the License for the 
 * specific language governing permissions and limitations under the  
 * License.  
 *
 * When distributing the Covered Code, include this CDDL Header Notice 
 * in each file and include the License file at
 * open-dm-mi/bootstrap/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the 
 * fields enclosed by brackets [] replaced by your own identifying 
 * information: "Portions Copyrighted [year] [name of copyright owner]"
 */
package com.sun.dm.di.bulkloader.util;

import java.io.File;
import net.java.hulp.i18n.Logger;
import com.sun.sql.framework.utils.ScEncrypt;

/**
 *
 * @author Manish
 */
public class CreateTriggers {

    String dumppath = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(CreateTriggers.class.getName());
    private static Localizer sLoc = Localizer.get();

    public CreateTriggers() {
        sLog.info(sLoc.x("LDR421: Creating eTL Invoker Triggers ..."));
        dumppath = BLConstants.getCWD() + BLConstants.fs + BLConstants.toplevelpkg;
    }

    public void createTriggers() {
        int target_type_code = Integer.parseInt(System.getProperty("target.type"));
        createBAT(target_type_code);
        createSH(target_type_code);
        createConfig();
    }

    private void createConfig() {
        StringBuilder sb = new StringBuilder();
        // Place contents here
        String fs = BLConstants.fs;
       
        
        String dbsources = "." + BLConstants.fs + BLConstants.toplevelrt;
        File dirs = new File(dbsources);
        String[] dirnames = dirs.list();

        //Create Disable Constraints Strings
        
       
        // It may happen that there are multiple engine files to be executed, take care of this.
        for (int i = 0; i < dirnames.length; i++) {
            sb.append(dbsources + "\\" + dirnames[i] + "\\DefaultETL_engine.xml\n");
        }

        
        // Contents end.
        BLTools.writeIntoFile(dumppath, "files.config", sb.toString());
    }

    
    private void createBAT(int target_db_type) {
        StringBuilder sb = new StringBuilder();
        // Place contents here
        String fs = BLConstants.fs;
        sb.append("REM  *********** DO NOT EDIT TEXT BELOW ***********\n");
        sb.append("@echo off\n");
        sb.append("cls\n");
        sb.append("set LIB=.\\lib\n");
        sb.append("set AXION_JAR=%LIB%" + fs + "axion-1.0.jar\n");
        sb.append("set AXION_DEPENDENCIES_JARS=%LIB%\\commons-primitives-1.0.jar;%LIB%\\commons-logging-1.1.jar;%LIB%\\commons-collections-2.0.jar;%LIB%\\commons-codec-1.3.jar\n");
        switch (target_db_type) {
            case 1: //ORACLE
                sb.append("set DB_DRIVER=%LIB%" + fs + "ojdbc14-10.1.0.2.0.jar\n");
                sb.append("set TRGT_SCHEMA=" + System.getProperty("target.schema").toUpperCase() + "\n");
                break;
            case 2: //DERBY
                sb.append("set DB_DRIVER=%LIB%" + fs + "derbyclient.jar\n");
                sb.append("set TRGT_SCHEMA=" + System.getProperty("target.schema") + "\n");
                break;
            case 3: //SQL Server
                sb.append("set DB_DRIVER=%LIB%" + fs + "sqljdbc.jar\n");
                sb.append("set TRGT_SCHEMA=" + System.getProperty("target.schema") + "\n");
                break;                
            default:
                sb.append("set DB_DRIVER=%LIB%" + fs + "<Unknown Database Type (Code : " + target_db_type + ")>\n");
        }
        sb.append("set INVOKER_JARS=%LIB%" + fs + "etlengineInvoker-1.0.jar;%LIB%" + fs + "etl-engine-1.0.jar\n");
        sb.append("set CP=.;%AXION_JAR%;%AXION_DEPENDENCIES_JARS%;%DB_DRIVER%;%INVOKER_JARS%\n");
        sb.append("set TRGT_DB_CONN=" + BLConstants.getTrgtConnInfo() + "\n");
        sb.append("REM  *********** DO NOT EDIT TEXT ABOVE ***********\n\n");

        sb.append("REM ---- Target Database Passwd Is Encrypted. Regenerate the package if passwd is changed ----\n");
        sb.append("set TRGT_DB_LOGIN=" + System.getProperty("target.login") + "\n");
        sb.append("set TRGT_DB_PW=" + ScEncrypt.encrypt("soabi", System.getProperty("target.pw")) + "\n");
        sb.append("REM ----- ----- ----- ----- ----- ----- ----- ----- -----\n");
	sb.append("set JAVA_OPTS=\n");
        sb.append("set JAVA_PATH=" + System.getProperty("myjava.path") + "\n");

        String dbsources = "." + BLConstants.fs + BLConstants.toplevelrt;
        File dirs = new File(dbsources);
        String[] dirnames = dirs.list();

        //Create Disable Constraints Strings
        /*sb.append("\nREM    ### Disable Target Table Constraints ###\n");
        for (int i = 0; i < dirnames.length; i++) {
            sb.append("%JAVA_PATH%" + fs + "java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% com.sun.etl.engine.bulkloader.TargetDBOperations " + "%TRGT_SCHEMA% " + dirnames[i] + " %TRGT_DB_CONN%" + " %TRGT_DB_LOGIN%" + " %TRGT_DB_PW%" + " disable_constraint\n");
        }*/

        sb.append("\nREM    ### Execute eTL Commands ###\n");
        // It may happen that there are multiple engine files to be executed, take care of this.
        //for (int i = 0; i < dirnames.length; i++) {
            sb.append("%JAVA_PATH%" + fs + "java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% com.sun.dm.di.ETLEngineInvoker.ETLEngineInvoker .\\files.config\n");
        //}

        /*sb.append("\nREM    ### Enable Target Table Constraints ###\n");
        //Create enable Constraints Strings
        for (int i = 0; i < dirnames.length; i++) {
            sb.append("%JAVA_PATH%" + fs + "java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% com.sun.etl.engine.bulkloader.TargetDBOperations " + "%TRGT_SCHEMA% " + dirnames[i] + " %TRGT_DB_CONN%" + " %TRGT_DB_LOGIN%" + " %TRGT_DB_PW%" + " enable_constraint\n");
        }*/

        // Contents end. 
        BLTools.writeIntoFile(dumppath, "startLoad.bat", sb.toString());
    }

    private void createSH(int target_db_type) {
        StringBuilder sb = new StringBuilder();
        // Place contents here
        String fs = BLConstants.fs;
        sb.append("#!/bin/sh\n");
        sb.append("clear\n\n");
        sb.append("#  *********** DO NOT EDIT TEXT BELOW ***********\n");
        sb.append("LIB=./lib\n");
        sb.append("AXION_JAR=$LIB" + fs + "axion-1.0.jar\n");
        sb.append("AXION_DEPENDENCIES_JARS=\"$LIB/commons-primitives-1.0.jar:$LIB/commons-logging-1.1.jar:$LIB/commons-collections-2.0.jar:$LIB/commons-codec-1.3.jar\"\n");
        switch (target_db_type) {
            case 1: //ORACLE
                sb.append("DB_DRIVER=\"$LIB" + fs + "ojdbc14-10.1.0.2.0.jar\"\n");
                sb.append("TRGT_SCHEMA=\"" + System.getProperty("target.schema").toUpperCase() + "\"\n");
                break;
            case 2: //DERBY
                sb.append("DB_DRIVER=\"$LIB" + fs + "derbyclient.jar\"\n");
                sb.append("TRGT_SCHEMA=\"" + System.getProperty("target.schema") + "\"\n");
                break;
            case 3: //SQL Server
                sb.append("DB_DRIVER=\"LIB" + fs + "sqljdbc.jar\n");
                sb.append("TRGT_SCHEMA=\"" + System.getProperty("target.schema") + "\"\n");
                break;                
            default:
                sb.append("DB_DRIVER=\"$LIB" + fs + "<Unknown Database Type (Code : " + target_db_type + ")>\"\n");
        }
        sb.append("INVOKER_JARS=\"$LIB" + fs + "etlengineInvoker-1.0.jar:$LIB" + fs + "etl-engine-1.0.jar\"\n");
        sb.append("CP=\".:$AXION_JAR:$AXION_DEPENDENCIES_JARS:$DB_DRIVER:$INVOKER_JARS\"\n");
        sb.append("TRGT_DB_CONN=\"" + BLConstants.getTrgtConnInfo() + "\"\n");
        sb.append("#  *********** DO NOT EDIT TEXT ABOVE ***********\n\n");

        sb.append("# ---- Target Database Passwd Is Encrypted. Regenerate the package if passwd is changed ----\n");
        sb.append("TRGT_DB_LOGIN=" + System.getProperty("target.login") + "\n");
        sb.append("TRGT_DB_PW=" + ScEncrypt.encrypt("soabi", System.getProperty("target.pw")) + "\n");
        sb.append("# ----- ----- ----- ----- ----- ----- ----- ----- -----\n");
		sb.append("JAVA_OPTS=\n");
        sb.append("JAVA_PATH=" + System.getProperty("myjava.path") + "\n");

        String dbsources = "." + BLConstants.fs + BLConstants.toplevelrt;
        File dirs = new File(dbsources);
        String[] dirnames = dirs.list();

        //Create Disable Constraints Strings
//        sb.append("\n#    ### Disable Target Table Constraints ###\n");
//        for (int i = 0; i < dirnames.length; i++) {
//            sb.append("$JAVA_PATH" + fs + "java -cp $CP -Xms256M -Xmx1024M $JAVA_OPTS com.sun.etl.engine.bulkloader.TargetDBOperations " + "$TRGT_SCHEMA " + dirnames[i] + " $TRGT_DB_CONN" + " $TRGT_DB_LOGIN" + " $TRGT_DB_PW" + " disable_constraint\n");
//        }

//        sb.append("\n#    ### Execute eTL Commands ###\n");
        // It may happen that there are multiple engine files to be executed, take care of this.
//        for (int i = 0; i < dirnames.length; i++) {
            sb.append("$JAVA_PATH" + fs + "java -cp $CP -Xms256M -Xmx1024M $JAVA_OPTS ETLEngineInvoker ./files.config\n");
//        }

//        sb.append("\n#    ### Enable Target Table Constraints ###\n");
//        //Create enable Constraints Strings
//        for (int i = 0; i < dirnames.length; i++) {
//            sb.append("$JAVA_PATH" + fs + "java -cp $CP -Xms256M -Xmx1024M $JAVA_OPTS com.sun.etl.engine.bulkloader.TargetDBOperations " + "$TRGT_SCHEMA " + dirnames[i] + " $TRGT_DB_CONN" + " $TRGT_DB_LOGIN" + " $TRGT_DB_PW" + " enable_constraint\n");
//        }

        // Contents end.
        BLTools.writeIntoFile(dumppath, "startLoad.sh", sb.toString());
    }
}
