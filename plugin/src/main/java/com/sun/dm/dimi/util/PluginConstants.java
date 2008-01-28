/*
 * Constants.java
 * 
 * Created on Sep 24, 2007, 12:31:10 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.util;

import java.io.File;

/**
 *
 * @author Manish
 */
public class PluginConstants {
    
    // File System Constants
    public static final String USER_DIR = System.getProperty("user.dir");
    public static final String PS = ":"; //Path Separator
    public static final String fs = System.getProperty("file.separator"); //File Separator
    public static File EVIEW_CONFIG_FILE = null;
    
    //Data Source Types
    public static final int JDBC_DATASOURCE = 0;
    public static final int GOOD_FILE_DATASOURCE = 1;
    public static final int REJECT_FILE_DATASOURCE = 2;
    public static final int RAW_FILE_DATASOURCE = 3;
    
    //Standardization and Phonetization patterns
    public static final String STAN_PATTERN = "_Std";
    public static final String PHON_PATTERN = "_Phon";
    
    //Axion Database Constants
    public static final String AXION_KEY_TABLE = "AXION_KEYS";
    public static final String DB_DRIVER = "org.axiondb.jdbc.AxionDriver";
    public static final String URI_PRIFIX = "jdbc" + PS +  "axiondb";
    public static final String AXION_DB_VERSION = ".VER";
    
    //DO Writer Constants
    public static final int flushfreq = 25000;
    
    //Default Columns Constants
    public static final String datatype = "string";
    public static final int datasize = 32;
    public static final boolean isRequired = true;
    public static final boolean isUpdatable = false;
    
    //Query Manager Specific Constants
    public static final String QueryManagerTablePrefix = "SBYN_";
    public static final String QualifiedPathPrefix = "Enterprise.SystemObject.";
    public static final int QueryBatchSize = 1; // No of PKs to be queried
    
    
    //Threads
    public static final long slave_init_time = 1000;//Slave Init time
    public static final long slave_retry_freq = 200; // 0.2 SEC
    public static final int available_do = 15000;
    // Sets 1 Min of Timeout before (4 X 15)
    public static final long master_retry_freq = 4000; //  4 SEC between each retry
    public static final int master_retry = 15;    

    //Object Finalizer
    public static final long nodata_sleep = 1000; // 1 sec    
    
    public PluginConstants() {
    }
    
    public static final void setEViewConfigFilePath(File f){
        EVIEW_CONFIG_FILE = f;
    }
}
