/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package config;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Manish
 */
public class DBGenerator {

    Connection conn = null;
    
    public DBGenerator() {
    }

    private void createAxionTable() {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            System.out.println("Creating SBYN_PATIENTVIEW Axion Test Table...");
            stmt.execute("create table if not exists SBYN_PATIENTVIEW ( " +
                    " PATIENTVIEWID varchar(20), " +
                    " SSN varchar(9), " +
                    " FIRSTNAME varchar(32), " +
                    " FIRSTNAME_STD varchar(32), " +
                    " FIRSTNAME_PHON varchar(32), " +
                    " LASTNAME varchar(32), " +
                    " LASTNAME_STD varchar(32), " +
                    " LASTNAME_PHON varchar(32), " +
                    " AGE varchar(32), " +
                    " GENDER varchar(32), " +
                    " CONSTRAINT pk_patient " +
                    " PRIMARY KEY (PATIENTVIEWID)" +
                    ")");

            System.out.println("Creating SBYN_ADDRESS Axion Test Table...");
            stmt.execute("create table if not exists SBYN_ADDRESS ( " +
                    " ADDRESSID varchar(20), " +
                    " PATIENTVIEWID varchar(20), " +
                    " ADDRESSTYPE varchar(32), " +
                    " ADDRESSLINE varchar(32), " +
                    " ADDRESSLINE_HOUSENO varchar(10), " +
                    " ADDRESSLINE_STDIR varchar(5), " +
                    " ADDRESSLINE_STNAME varchar(40), " +
                    " ADDRESSLINE_STPHON varchar(8), " +
                    " ADDRESSLINE_STTYPE varchar(5), " +
                    " CITY varchar(32)," +
                    " STATE varchar(32)," +
                    " ZIP varchar(32)," +
                    " CONSTRAINT pk_pidadd " +
                    " PRIMARY KEY(ADDRESSID), " +
                    " CONSTRAINT fk_pidadd " +
                    " FOREIGN KEY(PATIENTVIEWID) " +
                    " REFERENCES SBYN_PATIENTVIEW(PATIENTVIEWID) " +
                    ")");
        } catch (SQLException ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex.getMessage());
        } finally {
            try {
                stmt.close();
            } catch (Exception e) {
                System.out.println("Error Closing Statement : " + e.getMessage());
            }
        }
    }

    private void populateAxionTable() {
        PreparedStatement prepstmt = null;
        Statement stmt = null;
        try {
            System.out.println("Populating SBYN_PATIENTVIEW Table ...");
            prepstmt = conn.prepareStatement("INSERT INTO SBYN_PATIENTVIEW " +
                    "( PATIENTVIEWID," +
                    " SSN," +
                    " FIRSTNAME," +
                    " FIRSTNAME_STD," +
                    " FIRSTNAME_PHON," +
                    " LASTNAME," +
                    " LASTNAME_STD," +
                    " LASTNAME_PHON," +
                    " AGE," +
                    " GENDER) " +
                    " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            int k = 0;
            for (int i = 0; i < TestConfig.HOWMANY; i++) {
                k = i + 1;
                if (k % 1000 == 0) {
                    System.out.println("Generated [" + k + "] SBYN_PATIENTVIEW Table records ..");
                    conn.commit();
                }
                prepstmt.setString(1, "0000000" + k);
                prepstmt.setString(2, "SN" + k);
                prepstmt.setString(3, "MYFNAME_" + k);
                prepstmt.setString(4, "");
                prepstmt.setString(5, "");
                prepstmt.setString(6, "MYLNAME_" + k);
                prepstmt.setString(7, "");
                prepstmt.setString(8, "");
                prepstmt.setString(9, "AGE_" + k);
                prepstmt.setString(10, "M_F");
                prepstmt.executeUpdate();
            }
            conn.commit();


            System.out.println("Populating SBYN_ADDRESS Table ...");
            stmt = conn.createStatement();
            int j = 0;
            for (int i = 0; i < TestConfig.HOWMANY; i++) {
                j = i + 1;
                if (j % 1000 == 0) {
                    System.out.println("Generated [" + j + "] SBYN_ADDRESS Table records ...");
                    conn.commit();
                }
                stmt.executeUpdate("INSERT INTO SBYN_ADDRESS VALUES ('0000" + j + "', '0000000" + j + "', 'ADDType_" + j + "', 'MY ADDRESS BANGALORE PIN-" + j + "','a','b','c','d','e', 'f','g','h')");
            }
            conn.commit();

        } catch (SQLException e) {
            System.out.println("SQL Exception : " + e.getMessage());
        } finally {
            try {
                prepstmt.close();
                stmt.close();
            } catch (Exception e) {
                System.out.println("Error Closing Statement : " + e.getMessage());
            }
        }
        generateflag();
    }
    
    private void generateflag(){
        try {
            File f = new File(TestConfig.DBLOC, "flag.data");
            f.createNewFile();
        } catch (IOException ex) {
            Logger.getLogger(DBGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void createDBConnection() {
        try {
            Class.forName(TestConfig.DB_DRIVER);
            conn = DriverManager.getConnection(TestConfig.DBCONNURI);
            conn.setAutoCommit(false);
        } catch (SQLException ex) {
            Logger.getLogger(DBGenerator.class.getName()).log(Level.SEVERE, null, ex.getMessage());
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DBGenerator.class.getName()).log(Level.SEVERE, null, ex.getMessage());
        }
    }

    private void closeConn() {
        try {
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBGenerator.class.getName()).log(Level.SEVERE, null, ex.getMessage());
        }
    }

    private void createGoodFileDB() {
        File f = new File(TestConfig.FILEDBLOC);
        if (!f.exists()) {
            try {
                f.mkdir();
                File goodfile = new File(TestConfig.FILEDBLOC, TestConfig.GOODFILEDBNAME);
                goodfile.createNewFile();
            } catch (IOException ex) {
                System.out.println("IOException : " + ex.getMessage());
            }
        }
    }

    public void generate() {
        createDBConnection();
        createAxionTable();
        File f = new File(TestConfig.DBLOC, "flag.data");
        if (!f.exists()){
            populateAxionTable();
        }
        else{
            System.out.println("Skipping Populating Tables as data already exists");
        }
        closeConn();
        createGoodFileDB();
    }

    public static void main(String[] args) {
        DBGenerator dbgen = new DBGenerator();
        dbgen.generate();
    }
}
