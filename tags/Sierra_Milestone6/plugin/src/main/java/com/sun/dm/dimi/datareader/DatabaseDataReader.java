/*
 * DBDataReader.java
 *
 * Created on Jul 31, 2007, 4:13:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.util.PluginConstants;
import com.sun.dm.dimi.dataobject.metadata.MetaDataManager;
import com.sun.dm.dimi.dataobject.metadata.MetaDataService;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import com.sun.dm.dimi.qmquery.SelectableFilter;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 * Title:         CLASS DatabaseDataReader.java
 * Description:   This class connects to Database, creates query, creates resultset, normalises resultset rows
 *                and generates data objects for the data analysis/bulk loader.
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class DatabaseDataReader extends BaseDBDataReader {

    MetaDataService mdservice = MetaDataManager.getMetaDataManager().getMetaDataService();
    private String mDbUri = null;
    Connection conn = null;
    private ArrayList filteredSelects = null;
    private DataBaseQuerySlave qslave = null;
    //Thread status
    private boolean isMasterSleeping = false;
    private Thread mainthread = Thread.currentThread();
    //Logger
    private static Logger sLog = LogUtil.getLogger(DatabaseDataReader.class.getName());
    Localizer sLoc = Localizer.get();

    /**
     * Constructor for the DatabaseDataReader
     */
    public DatabaseDataReader() {
    }

    /**
     * Constructor for the DatabaseDataReader
     * @param specialMode
     */
    public DatabaseDataReader(boolean specialMode) {
        super(specialMode);
        try {
            Class.forName(PluginConstants.DB_DRIVER);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Constructor for the DatabaseDataReader
     * @param dburi
     * @param filter - Object for overriding selectables from the database (null if not required)
     * @param specialMode
     */
    public DatabaseDataReader(String dbDir, SelectableFilter filter, boolean specialMode) {
        this(specialMode);

        if (validateDBPath(dbDir)) {

            // Generate DB uri from location
            String dburi = buildURI(dbDir);

            if (dburi != null) {
                //Check Database connection
                if (checkConnection(dburi)) {
                    this.mDbUri = dburi;

                    //Validate Seletable filters
                    if (filter != null) {
                        filteredSelects = new ArrayList();
                        validateSeletableFilters(filter);
                    }

                    //Start Thread
                    qslave = new DataBaseQuerySlave(this.conn, this.doLinkedList, filteredSelects, this);
                    qslave.start();

                    // Tempory check to ensure slave started running
                    if (!this.qslave.isSlaveRunning()) {
                        sleep(PluginConstants.slave_init_time); //Assuming x sec is good enough for the slave to start , else increase this.
                    }
                }
            }
        }
    }

    /**
     * Constructor for the DatabaseDataReader
     * @param dbName
     * @param dbDir
     * @param filter - Object for overriding selectables from the database (null if not required)
     * @param specialMode
     */
    public DatabaseDataReader(String dbName, String dbDir, SelectableFilter filter, boolean specialMode) {
        this(specialMode);
        if (validateDBPath(dbDir)) {
            if (validateDBName(dbDir, dbName)) {
                sLog.info(sLoc.x("PLG009: Database {0} Exists", dbName));
                String uri = PluginConstants.URI_PRIFIX + PluginConstants.PS + dbName + PluginConstants.PS + dbDir;
                if (checkConnection(uri)) {
                    this.mDbUri = uri;

                    //Validate Seletable filters
                    if (filter != null) {
                        filteredSelects = new ArrayList();
                        validateSeletableFilters(filter);
                    }

                    //Start Thread
                    qslave = new DataBaseQuerySlave(this.conn, this.doLinkedList, filteredSelects, this);
                    qslave.start();

                    // Tempory check to ensure slave started running
                    if (!this.qslave.isSlaveRunning()) {
                        sleep(PluginConstants.slave_init_time); //Assuming x sec is good enough for the slave to start , else increase this.
                    }
                }
            }
        }
    }

    /**
     * Checks if the database can be connected
     * @param dburi
     * @return boolean
     */
    private boolean checkConnection(String dburi) {
        boolean conntest = false;
        try {
            this.conn = DriverManager.getConnection(dburi);
            if (this.conn != null) {
                conntest = true;
            }
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("PLG008: Unable to connect to Database. Check your exact URI: {0} \n{1}", dburi, ex));
            stopObjectFinalizer();
        }
        return conntest;
    }

    /**
     * Reads the DataObject from the list created by Database reader slave thread.
     * @return DataObject
     * @throws com.sun.mdm.index.dataobject.InvalidRecordFormat
     */
    public DataObject readDataObject() throws InvalidRecordFormat {
        DataObject dos = null;
        while (!this.doLinkedList.isEmpty()) {
            dos = this.doLinkedList.remove(0);
            return dos;
        }

        if (this.qslave.isSlaveRunning()) {
            // Wake up slave immideately so that it can generate more data data
            this.qslave.wakeUpSlave();

            // Perform iterations and wake up master if more data objects are available
            for (int retry = 1; retry <= PluginConstants.master_retry; retry++) {
                if (!this.doLinkedList.isEmpty()) {
                    dos = this.doLinkedList.remove(0);
                    return dos;
                } else {
                    if (this.qslave.isSlaveRunning()) {
                        sLog.fine("QueryManager is busy producing Data Objects ... Retry [" + retry + "/" + PluginConstants.master_retry + "]");
                        //sLog.info(LocalizedString.valueOf(sLoc.t("PLG010: QueryManager is busy producing Data Objects ... Retry [{0}/{1}]", retry, PluginConstants.master_retry)));
                        sleep(PluginConstants.master_retry_freq);
                        // This is a condition when DataBase is probably hung and is unable to getch records before the time out
                        if (retry >= PluginConstants.master_retry) {
                            if (this.doLinkedList.isEmpty()) {
                                // Shut down master and slave gracefully
                                this.qslave.stopSlaveThread();
                                dos = null;
                                sLog.severe(sLoc.x("PLG011: DataBase did not respond to Query in [{0}] sec. Plugin will Exit.\n" +
                                        "Change Configuration parameters and re-try", ((int) (PluginConstants.master_retry * PluginConstants.master_retry_freq)) / 1000));
                                System.exit(0);
                            }
                        }
                    } else {
                        dos = null;
                        if (!qslave.getAbnormalHaltStatus()) {
                            sLog.info(sLoc.x("PLG012: All Data Objects fetched successfully. Master Process will end"));
                        }
                        break;
                    }
                }
            }
        } else {
            // Data Has been processed
            if (!qslave.getAbnormalHaltStatus()) {
             sLog.info(sLoc.x("PLG012: All Data Objects fetched successfully. Master Process will end"));   
            }
            else{
                sLog.info(sLoc.x("PLG032: Unable to fetch Data Objects.Master Process will end"));   
            }
            dos = null;
        }
        return dos;
    }

    /**
     * Closes the open Database connection
     * @throws java.lang.Exception
     */
    public void close() throws Exception {
        this.qslave.stopSlaveThread();
    //this.conn.close();
    }

    /*
     * Validate if the DB with the name exists
     */
    private boolean validateDBName(String dbDir, String dbName) {
        boolean ret = false;
        File dbfile = new File(dbDir + PluginConstants.fs + dbName.toUpperCase() + PluginConstants.AXION_DB_VERSION);
        if (dbfile.exists()) {
            if (dbfile.isFile()) {
                ret = true;
            } else {
                sLog.severe(LocalizedString.valueOf("PLG014: DataBase file [" + dbName + ".VER] does not exist in dir : " + dbDir));
            }
        } else {
            sLog.severe(sLoc.x("PLG015: DataBase File [{0}] does not exist", dbfile));
        }
        return ret;
    }

    /*
     * Validate if the DB path is correct
     */
    private boolean validateDBPath(String dbDir) {
        boolean ret = false;
        File dbdir = new File(dbDir);
        if (dbdir.exists()) {
            if (dbdir.isDirectory()) {
                ret = true;
            } else {
                sLog.severe(LocalizedString.valueOf("PLG016: DataBase Directory is not a dir : " + dbDir));
            }
        } else {
            sLog.severe(sLoc.x("PLG017: DataBase Directory {0} does not exist", dbDir));
        }
        return ret;
    }

    /**
     * Validates the user provided seletables before using it for the query
     */
    private void validateSeletableFilters(SelectableFilter filter) {
        ArrayList selectables = filter.getAddedSelectables();
        if (selectables != null) {
            for (int i = 0; i < selectables.size(); i++) {
                boolean foundflag = false;
                String qualifiedFieldUser = selectables.get(i).toString();
                String qualifiedTableNameUser = qualifiedFieldUser.substring(0, qualifiedFieldUser.lastIndexOf("."));

                // Get the field map for the table
                HashMap tablefieldsmap = (HashMap) mdservice.getLookupMap().get(qualifiedTableNameUser);

                if (tablefieldsmap != null) {
                    Iterator itr = tablefieldsmap.keySet().iterator();
                    while (itr.hasNext()) {
                        String lookupfield = qualifiedTableNameUser + "." + itr.next();
                        if (lookupfield.equalsIgnoreCase(qualifiedFieldUser)) {
                            sLog.fine("Field Filter Qualified field (exact case match) : " + lookupfield);
                            foundflag = true;
                            filteredSelects.add(PluginConstants.QualifiedPathPrefix + lookupfield);
                            break;
                        }
                    }
                } else {
                    // Check for possibility when exact string (case sensitive) has not been supplied
                    HashMap tablekeysmap = mdservice.getLookupMap();
                    Iterator itr = tablekeysmap.keySet().iterator();
                    while (itr.hasNext()) {
                        String qualTableName = (String) itr.next();
                        if (qualTableName.equalsIgnoreCase(qualifiedTableNameUser)) {
                            HashMap fields = (HashMap) tablekeysmap.get(qualTableName);
                            if (fields != null) {
                                Iterator itr1 = fields.keySet().iterator();
                                while (itr1.hasNext()) {
                                    String lookupfield = qualTableName + "." + itr1.next();
                                    if (lookupfield.equalsIgnoreCase(qualifiedFieldUser)) {
                                        sLog.fine("Field Filter Qualified field (case insensitive match) : " + lookupfield);
                                        foundflag = true;
                                        filteredSelects.add(PluginConstants.QualifiedPathPrefix + lookupfield);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if (!foundflag) {
                    sLog.info(sLoc.x("PLG018: \nDropping field [ {0}] from the Database Query as it does not comply with the eViewDefinition. Pls correct the error\n", qualifiedFieldUser));
                }
            }
        }
    }

    private void sleep(long time) {
        try {
            sLog.fine(" Master Thread will sleep [" + time / 1000 + "] seconds till interrupted ...");
            this.isMasterSleeping = true;
            Thread.sleep(time);
            this.isMasterSleeping = false;
        } catch (InterruptedException ex) {
            this.isMasterSleeping = false;
            sLog.fine(" Master Thread Interrupted. Will fetch remaining data objects if availablle");
        }
    }

    public void wakeUpMaster() {
        if (isMasterSleeping) {
            sLog.fine("Master waking up ... ");
            mainthread.interrupt();
        }
    }

    /**
     * Return the type of Database source
     * @return String - Data Type Source
     */
    public int getDataSourceType() {
        return PluginConstants.JDBC_DATASOURCE;
    }

    private String buildURI(String dbDir) {
        String uri = null;
        File dbdir = new File(dbDir);

        if (dbdir.isDirectory()) {
            String DBNAME = "<Unable to Find DBNAME>";
            String[] children = dbdir.list();
            for (int i = 0; i < children.length; i++) {
                if (children[i].indexOf(".VER") != -1) {
                    //AXIONDB
                    DBNAME = children[i].substring(0, children[i].indexOf(".VER"));
                    sLog.info(sLoc.x("PLG009: Database {0} Exists", DBNAME));
                    uri = PluginConstants.URI_PRIFIX + PluginConstants.PS + DBNAME + PluginConstants.PS + dbDir;
                    sLog.fine("Axion DB URI IS : " + uri);
                    return uri;
                }
            }
        }
        return uri;
    }
}
