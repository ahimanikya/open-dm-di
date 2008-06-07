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

    private static transient final Logger mLogger = Logger.getLogger(LoaderMain.class);
    private static Logger sLog = LogUtil.getLogger(LoaderMain.class.getName());
    private static Localizer sLoc = Localizer.get();

    public LoaderMain() {
    }

    public static void main(String[] args) {

        sLog.info(sLoc.x("LDR001: Loader Start ..."));
        confiugreLogger();

        // keep this to enable local testing
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

        //Printing the information passed
        StringBuilder paraminfo = new StringBuilder();
        paraminfo.append("Parameters passed to the Bulk Loader :\n");
        paraminfo.append("  [1] Data Source\n");
        paraminfo.append("\tSource DB Location : " + System.getProperty("sourcedb.loc") + "\n");
        paraminfo.append("\tSource Field Delimiter Type : " + System.getProperty("field.delimiter") + "\n");
        paraminfo.append("\tSource Record Delimiter Type : " + System.getProperty("record.delimiter") + "\n");
        paraminfo.append("  [2] Data Target\n");
        int target_type_code = Integer.parseInt(System.getProperty("target.type"));
        
        switch (target_type_code) {
            case 1:
                paraminfo.append("\tTarget Database Type : ORACLE (Code : " + target_type_code + ")\n");
                paraminfo.append("\tTarget URL : " + BLConstants.URI_ORACLE_PRIFIX + "thin:@" + System.getProperty("target.host") + ":" + System.getProperty("target.port") + ":" + System.getProperty("target.id") + "\n");
                break;
            case 2:
                paraminfo.append("\tTarget Database Type : DERBY (Code : " + target_type_code + ")\n");
                paraminfo.append("\tTarget Host name/ip : " + System.getProperty("target.host") + "\n");
                break;
            case 3:
                paraminfo.append("\tTarget Database Type : SQL Server (Code : " + target_type_code + ")\n");
                paraminfo.append("\tTarget URL : " + BLConstants.URI_SQLSERVER_PRIFIX + "//" + System.getProperty("target.host") + ":" + System.getProperty("target.port") + ";databaseName=" + System.getProperty("target.id") + "\n");
                break;
            default:
                paraminfo.append("\tTarget Database Type : UNKNOWN (Code : " + target_type_code + ")\n");
                paraminfo.append("\tTarget Host name/ip : " + System.getProperty("target.host") + "\n");
                break;
        }

        paraminfo.append("\tTarget Schema : " + System.getProperty("target.schema") + "\n");
        paraminfo.append("\tTarget Catalog : " + System.getProperty("target.catalog") + "\n");
        paraminfo.append("\tTarget DB Username : " + System.getProperty("target.login") + "\n");
        paraminfo.append("\tTarget Password : " + getPWString(System.getProperty("target.pw")) + "\n");
        paraminfo.append("\tJava Path : " + System.getProperty("myjava.path") + "\n");
        paraminfo.append("\tDatabase Driver : " + System.getProperty("dbdriver.name") + "\n");
        sLog.info(sLoc.x("LDR002: {0}", paraminfo.toString()));


        ConnectionFactory cfact = ConnectionFactory.getConnectionFactory();
        if (BLTools.validateDir(System.getProperty("sourcedb.loc"))) {
            File f = new File(System.getProperty("sourcedb.loc"));
            String[] datafiles = f.list();

            // If Schema name supplied is null, treat user name as the schema name for oracle
            // Schema has to be uppercase
            String trgtdbschema = System.getProperty("target.schema");
            if (target_type_code == 1) {
                if ((trgtdbschema == null) || (trgtdbschema.length() == 0)) {
                    trgtdbschema = System.getProperty("target.login").toUpperCase();
                    sLog.info(sLoc.x("LDR012:Using target db user name [{0}] for the schema as it was not supplied.", trgtdbschema));
                } else {
                    trgtdbschema = trgtdbschema.toUpperCase();
                }
            }

            /*
            //Sequencing Database Loading based on Table relationships - Start
            ArrayList<String> tableNames = new ArrayList<String>();
            for (String df : datafiles) {
            tableNames.add(df.substring(0, df.indexOf(".")));
            }
            
            DBConnection target = null;
            switch (target_type_code) {
            case 1:
            String dbschemaT = System.getProperty("target.schema").toUpperCase(); //Schema has to be uppercase
            target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), dbschemaT, System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[0], new ETLDefGenerator(datafiles[0], ETLStrategyBuilder.EXEC_MODE_STAGING));
            break;
            case 2:
            target = cfact.createTrgtDerbyConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[0], new ETLDefGenerator(datafiles[0], ETLStrategyBuilder.EXEC_MODE_STAGING));
            break;
            case 3:
            target = cfact.createTrgtSQLServerConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[0], new ETLDefGenerator(datafiles[0], ETLStrategyBuilder.EXEC_MODE_STAGING));
            default:
            target = null;
            break;
            }
            
            try {
            getTableSequence(target.getDataBaseConnection(), System.getProperty("target.schema"), System.getProperty("target.catalog"), tableNames);
            } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(LoaderMain.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
            java.util.logging.Logger.getLogger(LoaderMain.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.exit(0);
            //Sequencing Database Loading based on Table relationships - End
             */

            if ((trgtdbschema != null) && (trgtdbschema.length() > 0)) {

                for (int i = 0; i < datafiles.length; i++) {
                    sLog.info(sLoc.x("\n\n LDR011: Processing Source [ {0} ] ", datafiles[i]));
                    ETLDefGenerator etldefgen = null;
                    DBConnection cc_target = null;
                    switch (target_type_code) {
                        case 1:
                            etldefgen = new ETLDefGenerator(datafiles[i].toUpperCase(), ETLStrategyBuilder.EXEC_MODE_STAGING);
                            cc_target = cfact.createTrgtOracleConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), trgtdbschema, System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                            break;
                        case 2:
                            etldefgen = new ETLDefGenerator(datafiles[i], ETLStrategyBuilder.EXEC_MODE_STAGING);
                            cc_target = cfact.createTrgtDerbyConn(System.getProperty("target.host"), Integer.parseInt(System.getProperty("target.port")), System.getProperty("target.id"), System.getProperty("target.schema"), System.getProperty("target.catalog"), System.getProperty("target.login"), System.getProperty("target.pw"), datafiles[i], etldefgen);
                            break;
                        case 3:
                            etldefgen = new ETLDefGenerator(datafiles[i], ETLStrategyBuilder.EXEC_MODE_STAGING);
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
                            switch (target_type_code) {
                                case 1:
                                    cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), trgtdbschema, System.getProperty("target.catalog"), cc_target, target_type_code, etldefgen);
                                    break;
                                case 2:
                                    cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), System.getProperty("target.schema"), System.getProperty("target.catalog"), cc_target, target_type_code, etldefgen);
                                    break;
                                case 3:
                                    cc_source = cfact.createSrcConn(System.getProperty("sourcedb.loc"), datafiles[i], System.getProperty("field.delimiter"), System.getProperty("record.delimiter"), System.getProperty("target.schema"), System.getProperty("target.catalog"), cc_target, target_type_code, etldefgen);
                                    break;
                                default:
                                    cc_target = null;
                                    break;
                            }
                        }
                    }

                    if (cc_source != null) {
                        // Generate ETL Engine File
                        ETLEngineFileGenerator defgen = new ETLEngineFileGenerator();
                        defgen.generateETLEngineFile(etldefgen);
                        //CLose the soutce conn when done
                        try {
                            cc_source.getDataBaseConnection().close();
                        } catch (SQLException ex) {
                            sLog.infoNoloc("Failed to close source conn after use : " + ex.getMessage());
                        }
                    } else {
                        sLog.severe(sLoc.x("LDR008: Source connection is null. Engine file generation aborted"));
                    }
                }

                //Generate loader bat/sc scripts and zip
                CreateTriggers gentriggers = new CreateTriggers();
                CreateZip ziputil = new CreateZip();
                gentriggers.createTriggers(trgtdbschema, System.getProperty("target.type"));
                ziputil.createZipPackage(BLConstants.getCWD());

            } else {
                sLog.severe(sLoc.x("LDR013: Target Database schema is not supplied."));
                System.exit(0);
            }
        } else {
            sLog.severe(sLoc.x("LDR009: Loader will exit abnormally"));
            System.exit(0);
        }

        sLog.info(sLoc.x("LDR010: Portable ETL Loader Generated Successfully.\n Please unzip " + BLConstants.getCWD() + BLConstants.fs + BLConstants.zipname + "to run the loader"));
    }

    public static List getTableSequence(Connection con, String schema, String catalog, List<String> tableNames) throws SQLException, IOException {
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
                    System.out.println("Has Ref:: " + tableName + "-->" + rs.getString("PKTABLE_NAME"));
                } while (rs.next());
                tables.put(tableName, pkTables);
            }
        }

        List<String> parents = new ArrayList<String>();

        for (int i = 0; i < 5; i++) {
            Set<String> tableNamesSet = new HashSet<String>();
            tableNamesSet.addAll(tables.keySet());
            for (String tableName : tableNamesSet) {
                if (tables.get(tableName).isEmpty()) {
                    parents.add(tableName);
                    System.out.println("WN FK (Parents) : " + tableName);
                    tables.remove(tableName);
                } else if (parents.containsAll(tables.get(tableName))) {
                    parents.add(tableName);
                    tables.remove(tableName);
                    System.out.println(" 2nd : " + tableName);
                } else {
                    System.out.println(tableName + "-->" + tables.get(tableName).get(0));
                }
            }
        }

        File file = new File(BLConstants.getCWD() + BLConstants.fs + "TableOrder.txt");
        FileOutputStream fout = new FileOutputStream(file);
        for (String tblName : parents) {
            fout.write((tblName + "\r\n").getBytes());
        }
        fout.flush();
        fout.close();
        return Collections.EMPTY_LIST;
    }

    private static void confiugreLogger() {
        FileInputStream ins = null;
        try {
            LogManager logManager;
            String config = "config/logger.properties";
            File f = new File("./logs");

            if (f.exists() && f.isDirectory()) {
            } else {
                mLogger.fine("creating new logger dir");
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

    private static String getPWString(String pw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pw.length(); i++) {
            sb.append("*");
        }
        return sb.toString();
    }
}
