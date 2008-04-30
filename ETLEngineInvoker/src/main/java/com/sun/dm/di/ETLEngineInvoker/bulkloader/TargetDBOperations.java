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
package com.sun.dm.di.ETLEngineInvoker.bulkloader;

import com.sun.sql.framework.utils.ScEncrypt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public class TargetDBOperations {

    private static String ENABLE_CONSTRAINT = "enable_constraint";
    private static String DISABLE_CONSTRAINST = "disable_constraint";

    private static ArrayList getNamesOfTableConstraints(String cat, String table, Statement stmt) {
        System.out.print("Collecting constraints for table " + table + " ...");
        ArrayList conslist = null;
        StringBuilder query = new StringBuilder();
        query.append("SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE ");
        query.append("TABLE_NAME='" + table + "' ");
        if (cat != null) {
            query.append(" AND OWNER='" + cat + "'");
        }

        try {
            ResultSet rs = stmt.executeQuery(query.toString());
            if (rs != null) {
                conslist = new ArrayList();
            }
            while (rs.next()) {
                conslist.add(rs.getObject(1));
            }
        } catch (SQLException ex) {
            System.out.println("SQL Exception while user table query : " + ex.getMessage());
        }
        System.out.println("Done.");
        return conslist;
    }

    public static void enableConstrains(String catalog, String table_name, String dbconnstr, String login, String pw) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = connectToDB(dbconnstr, login, pw);
            if (conn != null) {
                stmt = conn.createStatement();
                //Get the names of table constrainst
                ArrayList cons_names = getNamesOfTableConstraints(catalog, table_name, stmt);
                for (int i = 0; i < cons_names.size(); i++) {
                    //Query All Constraints from the target table being processed
                    StringBuilder enablesql = new StringBuilder();
                    enablesql.append("ALTER TABLE ");
                    if (catalog != null) {
                        enablesql.append(catalog + "." + table_name);
                    } else {
                        enablesql.append(table_name);
                    }
                    enablesql.append(" ENABLE CONSTRAINT " + cons_names.get(i));
                    System.out.println("Enable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                    stmt.executeUpdate(enablesql.toString());
                }
            }
        } catch (SQLException ex) {
            System.out.println("SQL Exception while trying to enabling constraints" + ex.getMessage());
            System.out.println("Pls. evaluate and enable constrainst for this table manually");
        } finally {
            try {
                stmt.close();
            } catch (SQLException ex) {
                System.out.println("SQL Exception while trying to closing statement : " + ex.getMessage());
            }

            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println("SQL Exception while trying to closing db connection : " + ex.getMessage());
            }
        }
    }

    public static void disableConstrains(String catalog, String table_name, String dbconnstr, String login, String pw) {
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = connectToDB(dbconnstr, login, pw);
            if (conn != null) {
                stmt = conn.createStatement();
                //Get the names of table constrainst
                ArrayList cons_names = getNamesOfTableConstraints(catalog, table_name, stmt);
                for (int i = 0; i < cons_names.size(); i++) {
                    //Query All Constraints from the target table being processed
                    StringBuilder disablesql = new StringBuilder();
                    disablesql.append("ALTER TABLE ");
                    if (catalog != null) {
                        disablesql.append(catalog + "." + table_name);
                    } else {
                        disablesql.append(table_name);
                    }
                    disablesql.append(" DISABLE CONSTRAINT " + cons_names.get(i));
                    System.out.println("Disable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                    stmt.executeUpdate(disablesql.toString());
                }
            }
        } catch (SQLException ex) {
            System.out.println("SQL Exception while trying to disable constraints : " + ex.getMessage());
        } finally {
            try {
                stmt.close();
            } catch (SQLException ex) {
                System.out.println("SQL Exception while trying to closing statement : " + ex.getMessage());
            }

            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println("SQL Exception while trying to closing db connection" + ex.getMessage());
            }
        }
    }

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
            System.out.println("Init Oracle Driver");
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                return DriverManager.getConnection(dbconnstr, login, pw);
            } catch (SQLException ex) {
                System.out.println("Oracle SQL exception :: " + ex.getMessage());
            } catch (ClassNotFoundException ex) {
                System.out.println("Oracle driver Class not found exception :: " + ex.getMessage());
            }
        }

        return null;

    }

    private static String getPWString(String pw) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pw.length(); i++) {
            //    if ((i % 2) == 0) {
            sb.append("*");
        //    } else {
        //        char pwchar = pw.charAt(i);
        //        sb.append(pwchar);
        //    }
        }
        return sb.toString();
    }

    /**
     * 
     * @param args
     * args[0] - Target DB Catalog
     * args[1] - Target DB Table Name
     * args[2] - Target DB Connection String
     * args[3] - Target DB Login
     * args[4] - Target DB Passwd
     * args[5] - enable/disable constraint flag
     */
    public static void main(String[] args) {
        if (args.length == 6) {
            System.out.println("\n Target DB Catalog : " + args[0]);
            System.out.println(" Target DB Table Name : " + args[1]);
            System.out.println(" Target DB Connection String : " + args[2]);
            System.out.println(" Target DB Login : " + args[3]);
            System.out.println(" Target DB Passwd : " + getPWString(args[4]));
            System.out.println(" Target DB arg (enable or disable constraints) : " + args[5]);
            if (args[5].equalsIgnoreCase(ENABLE_CONSTRAINT)) {
                enableConstrains(args[0], args[1], args[2], args[3], args[4]);
            } else if (args[5].equalsIgnoreCase(DISABLE_CONSTRAINST)) {
                disableConstrains(args[0], args[1], args[2], args[3], args[4]);
            }
        }
    }
}
