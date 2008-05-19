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
package com.sun.dm.di.ETLEngineInvoker;

import com.sun.dm.di.ETLEngineInvoker.bulkloader.TargetDBOperations;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLEngineListener;
import com.sun.etl.engine.ETLEngineLogEvent;
import com.sun.etl.engine.impl.ETLEngineImpl;
import com.sun.sql.framework.utils.ScEncrypt;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;

/**
 *
 * @author admin
 */
public class ETLEngineInvoker {

    /**
     * Logger instance
     */
    private static transient final Logger mLogger = Logger.getLogger("ETLEngineInvoker");
    private static String TRGT_SCHEMA = null;
    private static String TRGT_DB_CONN = null;
    private static String TRGT_DB_LOGIN = null;
    private static String TRGT_DB_PW = null;
    private static String ENGINE_FILE = "C:\\ETL\\ETLEngineInvoker\\engine.xml";
    private static TargetDBOperations trgtdbopr = null;
    private static Connection trgtconn = null;
    private static int dbtype = 0;
    private ETLEngine engine = null;
    private ETLEngineListener listener = null;
    static LogManager logManager = null;

    public ETLEngineInvoker() {
    }

    /*private void startProcessing(File engineFile) {
    ETLWorkerThread worker = new ETLWorkerThread(engineFile);
    Thread workerThread = new Thread(worker);
    workerThread.start();
    }*/
    private void startProcessing(File engineFile) {

        executeEngine(engineFile);
    }

    public static void main(String[] args) {
        confiugreLogger();
        String engineFileName = "";
        try {
            if (args.length >= 5) {
                TRGT_SCHEMA = args[0];
                TRGT_DB_CONN = args[1];
                TRGT_DB_LOGIN = args[2];
                TRGT_DB_PW = args[3];
                ENGINE_FILE = args[4];
            }

            trgtconn = connectToDB(TRGT_DB_CONN, TRGT_DB_LOGIN, TRGT_DB_PW);

            if (trgtconn != null) {
                trgtdbopr = new TargetDBOperations();

                ArrayList<String> arrFiles = new ArrayList<String>();
                BufferedReader input = new BufferedReader(new FileReader(ENGINE_FILE));
                String fileName = "";
                while (fileName != null) {
                    fileName = input.readLine();
                    if (fileName != null && fileName.trim().length() > 0) {
                        arrFiles.add(fileName);
                    }
                }
                input.close();

                // Disable Constraints for All the Tables Being processed
                for (int i = 0; i < arrFiles.size(); i++) {
                    trgtdbopr.disableConstrains(TRGT_SCHEMA, trgtconn, arrFiles.get(i), dbtype);
                }

                // Load Data
                for (int i = 0; i < arrFiles.size(); i++) {
                    engineFileName = arrFiles.get(i);
                    mLogger.info("Start executing ETL Engine File :: " + engineFileName);
                    ETLEngineInvoker invoker = new ETLEngineInvoker();

                    invoker.startProcessing(new File(".\\ETLProcess\\" + arrFiles.get(i) + "\\DefaultETL_engine.xml"));
                    mLogger.info("End executing ETL Engine File :: " + engineFileName);
                }

                // Enable Constraints for All the Tables Being processed
                for (int i = 0; i < arrFiles.size(); i++) {
                    trgtdbopr.enableConstrains(TRGT_SCHEMA, trgtconn, arrFiles.get(i), dbtype);
                }
            } else {
                mLogger.severe("Unable to Connect to Target Database :: " + TRGT_DB_CONN);
            }

        } catch (Exception ex) {
            mLogger.severe(ex.getMessage());
        } finally {
            try {
                trgtconn.close();
            } catch (SQLException ex) {
                mLogger.warning("Unable to close target connection : " + ex.getMessage());
            }
        }
    }

    private void executeEngine(File engineFile) {

        try {
            System.out.println("Setting Engine context...");
            engine = new ETLEngineImpl();
            ETLEngineContext context = new ETLEngineContext();
            System.out.println("Parsing Engine File...");
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = factory.newDocumentBuilder();
            Document doc = docBuilder.parse(new FileInputStream(engineFile));
            engine.setContext(context);
            engine.setRunningOnAppServer(true);
            engine.parseXML(doc.getDocumentElement());
            System.out.println("Initializing engine listener...");
            listener = new ETLEngineListenerImpl();


            System.out.println("Transferring control to ETL Engine ...");
            engine.exec(listener);
            synchronized (listener) {
                listener.wait();

            }

        } catch (Exception ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }
    /*
    private class ETLWorkerThread implements Runnable {
    
    private File engineFile = null;
    
    public ETLWorkerThread(File file) {
    engineFile = file;
    init();
    }
    
    private void init() {
    try {
    System.out.println("Setting Engine context...");
    engine = new ETLEngineImpl();
    ETLEngineContext context = new ETLEngineContext();
    System.out.println("Parsing Engine File...");
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder = factory.newDocumentBuilder();
    Document doc = docBuilder.parse(new FileInputStream(engineFile));
    engine.setContext(context);
    engine.setRunningOnAppServer(true);
    engine.parseXML(doc.getDocumentElement());
    System.out.println("Initializing engine listener...");
    listener = new ETLEngineListenerImpl();
    } catch (Exception ex) {
    Logger.getLogger("global").log(Level.SEVERE, null, ex);
    }
    }
    
    public void run() {
    System.out.println("Transferring control to ETL Engine ...");
    engine.exec(listener);
    synchronized (listener) {
    try {
    listener.wait();
    } catch (Exception ex) {
    Logger.getLogger("global").log(Level.SEVERE, null, ex);
    }
    }
    }
    }
     */

    private static Connection connectToDB(String dbconnstr, String login, String pw) {
        pw = ScEncrypt.decrypt("soabi", pw);
        if (dbconnstr.indexOf("axion") != -1) {
            System.out.println("Init Axion Driver");
            try {
                Class.forName("org.axiondb.jdbc.AxionDriver");
                return DriverManager.getConnection(dbconnstr, login, pw);
            } catch (SQLException ex) {
                System.out.println("Axion driver SQL exception :: " + ex.getMessage());
            } catch (ClassNotFoundException ex) {
                System.out.println("Axion driver Class not found exception :: " + ex.getMessage());
            }
        } else if (dbconnstr.indexOf("oracle") != -1) {
            dbtype = 1;
            System.out.println("Init Oracle Driver");
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                return DriverManager.getConnection(dbconnstr, login, pw);
            } catch (SQLException ex) {
                System.out.println("Oracle SQL exception :: " + ex.getMessage());
            } catch (ClassNotFoundException ex) {
                System.out.println("Oracle driver Class not found exception :: " + ex.getMessage());
            }
        } else if (dbconnstr.indexOf("sqlserver") != -1) {
            dbtype = 3;
            System.out.println("Init SQL Server Driver");
            try {
                Class.forName("com.SeeBeyond.sqlserver.jdbc.sqlserver.SQLServerDriver");
                return DriverManager.getConnection(dbconnstr, login, pw);
            } catch (SQLException ex) {
                System.out.println("SQL Server SQL exception :: " + ex.getMessage());
            } catch (ClassNotFoundException ex) {
                System.out.println("SQL Server driver Class not found exception :: " + ex.getMessage());
            }
        }

        return null;

    }

    private class ETLEngineListenerImpl implements ETLEngineListener {

        public void executionPerformed(ETLEngineExecEvent event) {
            if ((event.getStatus() == ETLEngine.STATUS_COLLAB_COMPLETED) || (event.getStatus() == ETLEngine.STATUS_COLLAB_EXCEPTION)) {
                engine.stopETLEngine();
                Timestamp endTime = engine.getContext().getStatistics().getCollabFinishTime();
                Timestamp startTime = engine.getContext().getStatistics().getCollabStartTime();
                int rowsExtracted = 0;
                int rowsRejected = 0;
                int rowsInserted = 0;
                Iterator<String> it = engine.getContext().getStatistics().getKnownTableNames().iterator();
                System.out.println("");
                System.out.println("Tables processed:");
                int i = 1;
                while (it.hasNext()) {
                    String tblName = it.next();
                    System.out.println("");
                    System.out.println("\t" + i++ + "." + tblName);
                    //System.out.println("\t\tRows Extracted: " + engine.getContext().getStatistics().getRowsExtractedCount(tblName));
                    System.out.println("\t\tRows Inserted: " + engine.getContext().getStatistics().getRowsInsertedCount(tblName));
                    //System.out.println("\t\tRows Rejected: " + engine.getContext().getStatistics().getRowsRejectedCount(tblName));
                    rowsExtracted += engine.getContext().getStatistics().getRowsExtractedCount(tblName);
                    rowsRejected += engine.getContext().getStatistics().getRowsRejectedCount(tblName);
                    rowsInserted += engine.getContext().getStatistics().getRowsInsertedCount(tblName);
                }
                System.out.println("");
                //System.out.println("Total Rows Extracted for " + (i - 1) + " table(s) :" + rowsExtracted);
                System.out.println("Total Rows Inserted for " + (i - 1) + " table(s) :" + rowsInserted);
                //System.out.println("Total Rows Rejected for " + (i - 1) + " table(s) :" + rowsRejected);
                System.out.println("");
                System.out.println("Start time:" + startTime.toString());
                System.out.println("End time:" + endTime.toString());
                System.out.println("");
                long diff = endTime.getTime() - startTime.getTime();
                float timeTaken = diff / 1000;
                //System.out.println("Total time taken:" + String.valueOf(diff) + " milli seconds");
                System.out.println("Total time taken:" + String.valueOf(timeTaken) + " seconds");
                System.out.println("");
                System.out.println("Exiting ETL Engine...");
                synchronized (listener) {
                    listener.notifyAll();
                }
            }
        }

        public void updateOutputMessage(ETLEngineLogEvent event) {
        }
    }

    public static void confiugreLogger() {
        System.out.println("Configuring the logger ...");
        FileInputStream ins = null;
        try {
            //LogManager logManager;
            String config = "config/logger.properties";
            File f = new File("./logs");

            if (f.exists() && f.isDirectory()) {
                //mLogger.info("logger dir exist");
            } else {
                //mLogger.info("creating new logger dir");
                f.mkdir();
            }
            if (logManager == null) {
                logManager = LogManager.getLogManager();
                ins = new FileInputStream(config);
                logManager.readConfiguration(ins);
            }
        } catch (IOException ex) {
            mLogger.severe(ex.getLocalizedMessage());
        } catch (SecurityException ex) {
            mLogger.severe(ex.getLocalizedMessage());
        } finally {
            try {
                if (ins != null) {
                    ins.close();
                }
            } catch (IOException ex) {
                mLogger.severe(ex.getLocalizedMessage());
            }
        }
    }
}
