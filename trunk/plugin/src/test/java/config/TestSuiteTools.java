/*
 * TestSuiteTools.java
 *
 * Created on Nov 19, 2007, 2:53:14 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Manish
 */
public class TestSuiteTools {
    
    public static Connection jdbcDbConn = null;
    public static TestSuiteTools instance = null;
    
    public static Logger logger = Logger.getLogger(TestSuiteTools.class.getName());
    
    public TestSuiteTools() {
    }
    
    public static TestSuiteTools getInstance(){
        if (instance == null){
            createJdbcSampleConn();
            instance = new TestSuiteTools();
        }
        return instance;
    }
    
    public static void createJdbcSampleConn(){
        try {
            Class.forName("org.axiondb.jdbc.AxionDriver");
            String uri = "jdbc:axiondb:" + TestConfig.DBNAME + ":" + TestConfig.DBLOC;
            jdbcDbConn = DriverManager.getConnection(uri);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            logger.severe("Class Not Found");
        }
    }
    
    
    public Connection getTestDBConnection(){
        return jdbcDbConn;
    }
        
}
