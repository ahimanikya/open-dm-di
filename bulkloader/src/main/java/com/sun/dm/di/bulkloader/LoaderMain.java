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
package com.sun.dm.di.bulkloader;

import com.sun.dm.di.bulkloader.enginegen.ETLEngineFileGenerator;
import com.sun.dm.di.bulkloader.dbconnector.ConnectionFactory;
import com.sun.dm.di.bulkloader.dbconnector.DBConnection;
import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.CreateTriggers;
import com.sun.dm.di.bulkloader.util.CreateZip;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.BLTools;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;
import net.java.hulp.i18n.Logger;
import org.netbeans.modules.etl.codegen.ETLStrategyBuilder;

/**
 *
 * @author Manish
 */
public class LoaderMain {

    /**
     * Logger instance
     */
    private static transient final Logger mLogger = Logger.getLogger(LoaderMain.class);
    /**
     * i18n utility 
     */
    private static Logger sLog = LogUtil.getLogger(LoaderMain.class.getName());
    private static Localizer sLoc = Localizer.get();

    /** Creates a new instance of Main */
    public LoaderMain() {
    }

    private static String getPWString(String pw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pw.length(); i++) {
            //if ((i % 2) == 0) {
            sb.append("*");
        //} else {
        //    char pwchar = pw.charAt(i);
        //    sb.append(pwchar);
        //}
        }
        return sb.toString();
    }

    private static void confiugreLogger() {
        FileInputStream ins = null;
        try {
            LogManager logManager;
            String config = "config/logger.properties";
            File f = new File("./logs");

            if (f.exists() && f.isDirectory()) {
                mLogger.info(sLoc.x("logger dir exist"));
            } else {
                mLogger.info(sLoc.x("creating new logger dir"));
                f.mkdir();
            }

            logManager = LogManager.getLogManager();
            ins = new FileInputStream(config);
            logManager.readConfiguration(ins);
        } catch (IOException ex) {
            mLogger.infoNoloc(ex.getLocalizedMessage());
        } catch (SecurityException ex) {
            mLogger.infoNoloc(ex.getLocalizedMessage());
        } finally {
            try {
                ins.close();
            } catch (IOException ex) {
                mLogger.infoNoloc(ex.getLocalizedMessage());
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        sLog.info(sLoc.x("LDR001: Loader Start ..."));
        confiugreLogger();
        /*
        // Set the System Variables (Source)
        System.setProperty("sourcedb.loc", "D:\\temp\\mural\\masterindextest");
        System.setProperty("field.delimiter", "|");
        System.setProperty("record.delimiter", "$$$");
        // Set the System Variables (Target)
        System.setProperty("target.host", "localhost");
        System.setProperty("target.port", "1521");
        System.setProperty("target.sid", "orcl");
        System.setProperty("target.catalog", "OE");
        System.setProperty("target.login", "oe");
        System.setProperty("target.pw", "oe");
         */

        //Building up debug logs for parameters passed
        StringBuilder param_debug = new StringBuilder();
        param_debug.append("Parameters passed to the Bulk Loader ::\n");
        param_debug.append("  [1] Data Source\n");
        param_debug.append("\tSource DB Loation : " + System.getProperty("sourcedb.loc") + "\n");
        param_debug.append("\tSource Field Delimiter Type : " + System.getProperty("field.delimiter") + "\n");
        param_debug.append("\tSource Record Delimiter Type : " + System.getProperty("record.delimiter") + "\n");
        param_debug.append("  [2] Data Target\n");
        int target_type_code = Integer.parseInt(System.getProperty("target.type"));
        switch (target_type_code) {
            case 1:
                param_debug.append("\tTarget Database Type : ORACLE (Code : " + target_type_code + ")\n");
                break;
            case 2:
                param_debug.append("\tTarget Database Type : DERBY (Code : " + target_type_code + ")\n");
                break;
            case 3:
                param_debug.append("\tTarget Database Type : SQL LOADER (Code : " + target_type_code + ")\n");
                break;                
            default:
                param_debug.append("\tTarget Database Type : UNKNOWN (Code : " + target_type_code + ")\n");
                break;
        }
        param_debug.append("\tTarget Host name/ip : " + System.getProperty("target.host") + "\n");
        param_debug.append("\tTarget comm port : " + System.getProperty("target.port") + "\n");
        switch (target_type_code) {
            case 1:
                param_debug.append("\tTarget SID : " + System.getProperty("target.id") + "\n");
            case 2:
                param_debug.append("\tTarget DB NAME : " + System.getProperty("target.id") + "\n");
        }
        param_debug.append("\tTarget Schema : " + System.getProperty("target.schema") + "\n");
        param_debug.append("\tTarget Catalog : " + System.getProperty("target.catalog") + "\n");
        param_debug.append("\tTarget Login : " + System.getProperty("target.login") + "\n");
        param_debug.append("\tTarget PW : " + getPWString(System.getProperty("target.pw")) + "\n");
        param_debug.append("\tJava Home : " + System.getProperty("myjava.home") + "\n");
        sLog.info(sLoc.x("LDR002: {0}", param_debug.toString()));


        ConnectionFactory cfact = ConnectionFactory.getConnectionFactory();
        if (BLTools.validateDir(System.getProperty("sourcedb.loc"))) {
            File f = new File(System.getProperty("sourcedb.loc"));
            String[] datafiles = f.list();

            /*
            ArrayList<String> tableNames = new ArrayList<String>();
            for(String df :datafiles){
                tableNames.add(df.substring(0, df.indexOf(".")));
            }
            DBConnection target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[0], new ETLDefGenerator("ETLDEF_" + datafiles[0], ETLStrategyBuilder.EXEC_MODE_STAGING));
            try {
                System.out.println("Manish Enter 1 ......");
                getTableSequence(target.getDataBaseConnection(), System.getProperty("target.schema"), System.getProperty("target.catalog"), tableNames);
            } catch (SQLException ex) {
                java.util.logging.Logger.getLogger(LoaderMain.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(LoaderMain.class.getName()).log(Level.SEVERE, null, ex);
            }
            */
            
            for (int i = 0; i < datafiles.length; i++) {
                ETLDefGenerator etldefgen = new ETLDefGenerator("ETLDEF_" + datafiles[i], ETLStrategyBuilder.EXEC_MODE_STAGING);
                DBConnection cc_target = null;
                switch (target_type_code) {
                    case 1:
                        String dbschemaT = System.getProperty("target.schema").toUpperCase(); //Schema has to be uppercase
                        cc_target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), dbschemaT, System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                        break;
                    case 2:
                        cc_target = cfact.createTrgtDerbyConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                        break;
                    case 3:
                        cc_target = cfact.createTrgtSQLServerConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                        break;                        
                    default:
                        cc_target = null;
                        break;
                }

                if (BLConstants.getTrgtConnInfo() == null) {
                    BLConstants.setTrgtConnInfo(cc_target.getDBConnectionURI());
                }

                DBConnection cc_source = null;
                if (cc_target != null) {
                    if (cc_target.getDataBaseConnection() != null) {
                        cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), System.getProperty("target.schema"), System.getProperty("target.catalog"), cc_target, target_type_code, etldefgen);
                    }
                }

                if (cc_source != null) {
                    // Generate ETL Engine File
                    ETLEngineFileGenerator defgen = new ETLEngineFileGenerator();
                    defgen.generateETLEngineFile(etldefgen);
                } else {
                    sLog.severe(sLoc.x("LDR008: Source connection is null. Engine file gen aborted"));
                }

            //Try to disable Target Table Constraints
            //disableTargetTableConstrains(cc_target, datafiles[i]);
            }

            //Generate loader triggers and zip
            CreateTriggers gentriggers = new CreateTriggers();
            CreateZip ziputil = new CreateZip();
            gentriggers.createTriggers();
            ziputil.createZipPackage(BLConstants.getCWD());
        } else {
            sLog.severe(sLoc.x("LDR009: Loader will exit abnormally"));
            System.exit(0);
        }

        sLog.info(sLoc.x("LDR010: Loader Ends."));
    }
    
    public static List getTableSequence(Connection con, String schema, String catalog, List<String> tableNames) throws SQLException, IOException{
        System.out.println("Manish enter 2 .......");
        Map<String, List<String>> tables = new HashMap<String, List<String>>();
        //DBMetaDataFactory dbFactory = new DBMetaDataFactory();
        //dbFactory.connectDB(con);
        DatabaseMetaData dbmeta = con.getMetaData();
        System.out.println("size: " + tableNames.size());
        for (String tableName : tableNames) {
            ResultSet rs = null;

            rs = dbmeta.getImportedKeys(null, schema, tableName);
            List pkTables = new ArrayList<String>();
            if (!rs.next()) {
                tables.put(tableName, Collections.EMPTY_LIST);
                System.out.println("Parent: " + tableName);
            } else {
                //populate my parent list
                do {
                    pkTables.add(rs.getString("PKTABLE_NAME"));
                    System.out.println("Has Ref:: " +  tableName + "-->" +rs.getString("PKTABLE_NAME"));
                } while (rs.next());
                tables.put(tableName, pkTables);
            }
        }
        
        List<String> parents = new ArrayList<String>();
        
       for (int i =0; i< 5; i++) {
            Set<String> tableNamesSet = new HashSet<String>();
            tableNamesSet.addAll(tables.keySet());
            for (String tableName : tableNamesSet) {
                if (tables.get(tableName).isEmpty()) {
                    parents.add(tableName);
                    System.out.println("WN FK (Parents) : " + tableName);
                    tables.remove(tableName);
                } else if(parents.containsAll(tables.get(tableName))){
                    parents.add(tableName);
                    tables.remove(tableName);
                    System.out.println(" 2nd : " + tableName);
                }
                else{
                    System.out.println(tableName + "-->" + tables.get(tableName).get(0));
                }
            }
        }
        
        File file = new File(BLConstants.getCWD()+ BLConstants.fs + "TableOrder.txt");
        System.out.println("Manish tables :: " + file.getAbsolutePath());
        FileOutputStream fout = new FileOutputStream(file);
        for(String tblName: parents){
            fout.write((tblName+"\r\n").getBytes());
        }
        fout.flush();
        fout.close();
        return Collections.EMPTY_LIST;
    }    

}
