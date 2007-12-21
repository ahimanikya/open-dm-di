/*
 * ConnectionFactory.java
 *
 * Created on Nov 11, 2007, 9:32:47 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.dbconnector.DBConnection;
import com.sun.dm.di.bulkloader.dbconnector.OracleDBConnector;
import com.sun.dm.di.bulkloader.dbconnector.FlatFileDBConnector;
import com.sun.dm.di.bulkloader.dbconnector.AxionDBConnector;
import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.BLTools;
import com.sun.dm.di.bulkloader.util.BLTools;
import java.util.HashMap;

/**
 *
 * @author Manish
 */
public class ConnectionFactory {

    private static ConnectionFactory connfactory = null;
    private HashMap sourceconns = new HashMap<String, DBConnection>();
    private HashMap targetconns = new HashMap<String, DBConnection>();
    String etlDefName = null;

    private ConnectionFactory() {
    }

    private ConnectionFactory(String etlDefDisplayName) {
        etlDefName = etlDefDisplayName;
    }

    public static ConnectionFactory getConnectionFactory() {
        if (connfactory == null) {
            connfactory = new ConnectionFactory();
        }
        return connfactory;
    }

    public DBConnection createSrcConn(String filelocation, String filename, String field_delimiter, String record_delimiter, DBConnection target_inf, ETLDefGenerator etldefgen) {
        DBConnection dbconnection = null;
        if (BLTools.validatePath(filelocation, filename)) {
            String newfileloc = BLTools.copySrcDBFileToClassPath(filelocation, filename);
            dbconnection = new FlatFileDBConnector(etldefgen, newfileloc, filename, field_delimiter, record_delimiter, target_inf, BLConstants.SOURCE_TABLE_TYPE);
            //System.out.println("Adding New Source Conn for file [" + filename + "]");
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
                System.out.println("Error: Target AxionDB does not contain .VER file");
            }
        }
        return dbconnection;
    }

    public DBConnection createTrgtOracleConn(String host, int port, String sid, String schema, String catalog, String login, String pw, String tablename, ETLDefGenerator etldefgen) {
        // Check if DB Connection is already available. In such a case, do not connect again
        DBConnection dbconnection = null;
        if (targetconns.containsKey(host + "_" + port)) {
            dbconnection = (DBConnection)targetconns.get(host + "_" + port);
            //System.out.println("Connection Already Exists to [" + host + ":" + port + "]");
            dbconnection.addDBModelToDEF(etldefgen, dbconnection.getDataBaseConnection(), schema, catalog, BLConstants.TARGET_TABLE_TYPE, login, pw, tablename);
            
        } else {
            dbconnection = new OracleDBConnector(etldefgen, host, port, sid, schema, catalog, login, pw, tablename, BLConstants.TARGET_TABLE_TYPE);
            //System.out.println("Adding New Oracle Target Conn for host [" + host + ":" + port + "]");
            targetconns.put(host + "_" + port, dbconnection);
        }
        return dbconnection;
    }

    /*
    public Connection getSrcConn() {
    return sourceConn;
    }
    public Connection getTrgtConn() {
    return targetConn;
    }
     */
}
