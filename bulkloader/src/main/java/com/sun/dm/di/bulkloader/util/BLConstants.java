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
    
    //Derby DB Constants
    public static final String DB_DERBY_DRIVER = "org.apache.derby.jdbc.ClientDriver";
    public static final String URI_DERBY_PRIFIX = "jdbc" + PS +  "derby" + PS;
    public static final String DERBY_PRODUCT_NAME = "Apache Derby";    
    
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
    
    
    //Target DB Connection Information
    public static void setTrgtConnInfo(String trgtConnStr){
        trgtconn = trgtConnStr;
    }
    
    public static String getTrgtConnInfo(){
        return trgtconn;
    }
    
    private static String trgtconn = null;
    
    public BLConstants() {
    }

}
