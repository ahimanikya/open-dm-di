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
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.BLTools;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.util.HashMap;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class ConnectionFactory {

    private static ConnectionFactory connfactory = null;
    private HashMap sourceconns = new HashMap<String, DBConnection>();
    private HashMap targetconns = new HashMap<String, DBConnection>();
    String etlDefName = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(ConnectionFactory.class.getName());
    private static Localizer sLoc = Localizer.get();

    private ConnectionFactory() {
    }

    private ConnectionFactory(String etlDefDisplayName) {
        sLog.fine("ETL Definition Display Name :" + etlDefDisplayName);
        etlDefName = etlDefDisplayName;
    }

    public static ConnectionFactory getConnectionFactory() {
        if (connfactory == null) {
            connfactory = new ConnectionFactory();
        }
        return connfactory;
    }

    public DBConnection createSrcConn(String filelocation, String filename, String field_delimiter, String record_delimiter, String schema, String catalog, DBConnection target_inf, int trgt_type, ETLDefGenerator etldefgen) {
        DBConnection dbconnection = null;
        if (BLTools.validatePath(filelocation, filename)) {
            String newfileloc = BLTools.copySrcDBFileToClassPath(filelocation, filename);
            dbconnection = new FlatFileDBConnector(etldefgen, newfileloc, filename, field_delimiter, record_delimiter, schema, catalog, target_inf, trgt_type, BLConstants.SOURCE_TABLE_TYPE);
            sLog.fine("Adding New Source Conn for file [" + filename + "]");
            sourceconns.put(filename, dbconnection);
        }
        return dbconnection;
    }

    public DBConnection createTrgtAxionConn(String dblocation, String tablename, ETLDefGenerator etldefgen) {
        DBConnection dbconnection = null;
        if (BLTools.validateDir(dblocation)) {
            String dbname = BLTools.validateAxionDB(dblocation);
            if (dbname != null) {
                dbconnection = new AxionDBConnector(etldefgen, dblocation, dbname, tablename, BLConstants.TARGET_TABLE_TYPE);
                targetconns.put(dblocation, dbconnection.getDataBaseConnection());
            } else {
                sLog.infoNoloc("Error: Target AxionDB does not contain .VER file");
            }
        }
        return dbconnection;
    }

    public DBConnection createTrgtOracleConn(String host, int port, String sid, String schema, String catalog, String login, String pw, String tablename, ETLDefGenerator etldefgen) {
        // Check if DB Connection is already available. In such a case, do not connect again
        DBConnection dbconnection = null;
        if (targetconns.containsKey(host + "_" + port)) {
            dbconnection = (DBConnection) targetconns.get(host + "_" + port);
            sLog.fine("Connection Already Exists to [" + host + ":" + port + "]");
            dbconnection.addDBModelToDEF(etldefgen, dbconnection.getDataBaseConnection(), schema, catalog, BLConstants.TARGET_TABLE_TYPE, login, pw, tablename);
        } else {
            dbconnection = new OracleDBConnector(etldefgen, host, port, sid, schema, catalog, login, pw, tablename, BLConstants.TARGET_TABLE_TYPE);
            sLog.fine("Adding New Oracle Target Conn for host [" + host + ":" + port + "]");
            targetconns.put(host + "_" + port, dbconnection);
        }
        return dbconnection;
    }
    
    public DBConnection createTrgtDerbyConn(String host, int port, String dbname, String schema, String catalog, String login, String pw, String tablename, ETLDefGenerator etldefgen) {
        // Check if DB Connection is already available. In such a case, do not connect again
        DBConnection dbconnection = null;
        if (targetconns.containsKey(host + "_" + port)) {
            dbconnection = (DBConnection) targetconns.get(host + "_" + port);
            sLog.fine("Connection Already Exists to [" + host + ":" + port + "]");
            dbconnection.addDBModelToDEF(etldefgen, dbconnection.getDataBaseConnection(), schema, catalog, BLConstants.TARGET_TABLE_TYPE, login, pw, tablename);
        } else {
            dbconnection = new DerbyDBConnector(etldefgen, host, port, dbname, schema, catalog, login, pw, tablename, BLConstants.TARGET_TABLE_TYPE);
            sLog.fine("Adding New Derby Target Conn for host [" + host + ":" + port + "]");
            targetconns.put(host + "_" + port, dbconnection);
        }
        return dbconnection;
    }    
    
}
