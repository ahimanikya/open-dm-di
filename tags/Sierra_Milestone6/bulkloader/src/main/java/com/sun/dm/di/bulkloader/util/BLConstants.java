/*
 * BLConstants.java
 * 
 * Created on Nov 12, 2007, 10:29:51 AM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.di.bulkloader.util;

import java.io.File;
import java.io.IOException;
import org.netbeans.modules.sql.framework.model.SQLConstants;

/**
 *
 * @author Manish
 */
public class BLConstants {

    // File System Constants
    public static final String USER_DIR = System.getProperty("user.dir");
    public static final String PS = ":"; //Path Separator
    public static final String fs = System.getProperty("file.separator"); //File Separator
    
    //Axion Database Constants
    public static final String AXION_KEY_TABLE = "AXION_KEYS";
    public static final String DB_AXION_DRIVER = "org.axiondb.jdbc.AxionDriver";
    public static final String URI_AXION_PRIFIX = "jdbc" + PS +  "axiondb" + PS;
    public static final String AXION_DB_VERSION = ".VER";
    public static final String EXTDB_PREFIX = "DB_";
    public static final String AXION_PRODUCT_NAME = "AxionDB";
    
    // Oracle DB Constants
    public static final String DB_ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    public static final String URI_ORACLE_PRIFIX = "jdbc" + PS +  "oracle" + PS;
    public static final String ORACLE_PRODUCT_NAME = "Oracle";
    
    //ETL Constants
    public static final int SOURCE_TABLE_TYPE = SQLConstants.SOURCE_TABLE;
    public static final int TARGET_TABLE_TYPE = SQLConstants.TARGET_TABLE;
    public static final String DEFAULT_MODEL_NAME = "DefaultETLModel";
    public static final String DEFAULT_ENGINE_NAME = "DefaultETL";
    public static final String toplevelrt = "ETLProcess";
    public static final String toplevelpkg = "ETLLoader";
    public static final String zipname = "etl-loader.zip";
    
    //Current Working dir
    public static String getCWD(){
        try {
            return new File(".").getCanonicalPath();
        } catch (IOException ex) {
            
        }
        return null;
    }
    
    public static final String artiTop = getCWD() + fs + toplevelrt + fs;
    
    public BLConstants() {
    }

}
