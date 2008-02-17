/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.util;

import java.io.File;
import net.java.hulp.i18n.Logger;

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
        createBAT();
        createSH();
    }

    private void createBAT() {
        StringBuilder sb = new StringBuilder();
        // Place contents here
        String fs = BLConstants.fs;
        sb.append("REM  *********** DO NOT EDIT TEXT BELOW ***********\n");
        sb.append("@echo off\n");
        sb.append("cls\n");
        sb.append("set LIB=.\\lib\n");
        sb.append("set AXION_JAR=%LIB%" + fs + "axion-1.0.jar\n");
        sb.append("set AXION_DEPENDENCIES_JARS=%LIB%\\commons-primitives-1.0.jar;%LIB%\\commons-logging-1.1.jar;%LIB%\\commons-collections-2.0.jar;%LIB%\\commons-codec-1.3.jar\n");
        sb.append("set ORACLE_DRIVER=%LIB%" + fs + "ojdbc14-10.1.0.2.0.jar\n");
        sb.append("set INVOKER_JARS=%LIB%" + fs + "ETLEngineInvoker-1.0.jar;%LIB%" + fs + "etl-engine-1.0.jar\n");
        sb.append("set CP=.;%AXION_JAR%;%AXION_DEPENDENCIES_JARS%;%ORACLE_DRIVER%;%INVOKER_JARS%\n");
        sb.append("set JAVA_OPTS=\n");
        sb.append("set TRGT_DB_CONN=" + BLConstants.getTrgtConnInfo()  + "\n");
        sb.append("set TRGT_DB_LOGIN=<Set Target DB Login. Blank for null>\n");
        sb.append("set TRGT_DB_PW=<Set Target DB PW. Blank for null>\n");
        sb.append("REM  *********** DO NOT EDIT TEXT ABOVE ***********\n");
        
        String dbsources = ".\\" + BLConstants.toplevelrt;
        File dirs = new File(dbsources);
        String[] dirnames = dirs.list();
        
        //Create Disable Constraints Strings
        sb.append("\nREM    ### Disable Target Table Constraints ###\n");
        for (int i = 0; i < dirnames.length; i++) {
            sb.append("java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% com.sun.etl.engine.bulkloader.TargetDBOperations " + "OE "  + dirnames[i] + " %TRGT_DB_CONN%" + " %TRGT_DB_LOGIN%" + " %TRGT_DB_PW%" +  " disable_constraint\n");
        }        

        sb.append("\nREM    ### Execute eTL Commands ###\n");
        // It may happen that there are multiple engine files to be executed, take care of this.
        for (int i = 0; i < dirnames.length; i++) {
            sb.append("java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% ETLEngineInvoker " + dbsources + "\\" + dirnames[i] + "\\DefaultETL_engine.xml\n");
        }
        
        sb.append("\nREM    ### Enable Target Table Constraints ###\n");
        //Create enable Constraints Strings
        for (int i = 0; i < dirnames.length; i++) {
            sb.append("java -cp %CP% -Xms256M -Xmx1024M %JAVA_OPTS% com.sun.etl.engine.bulkloader.TargetDBOperations " + "OE "  + dirnames[i] + " %TRGT_DB_CONN%" + " %TRGT_DB_LOGIN%" + " %TRGT_DB_PW%" +  " enable_constraint\n");
        }        

        // Contents end.
        BLTools.writeIntoFile(dumppath, "startLoad.bat", sb.toString());
    }

    private void createSH() {
        StringBuilder sb = new StringBuilder();
        // Place contents here
        sb.append("<echo>NOT IMPLEMENTED YET</echo>");
        // Contents end.
        BLTools.writeIntoFile(dumppath, "startLoad.sh", sb.toString());
    }
}
