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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public class TargetDBOperations {

    private ArrayList getNamesOfTableConstraints(String schema, String table, Statement stmt, int dbtype) {
        ArrayList conslist = null;
        StringBuilder query = new StringBuilder();
        switch (dbtype) {
            case 1:
                System.out.print("[Oracle]Collecting constraints for table " + table + " ...");
                query.append("SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE ");
                query.append("TABLE_NAME='" + table + "' ");
                if (schema != null) {
                    query.append(" AND OWNER='" + schema + "'");
                }
                break;
            case 3:
                System.out.print("[SQL Server]Collecting constraints for table " + table + " ...");

                query.append("SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE WHERE ");
                query.append("TABLE_NAME='" + table + "' ");
                if (schema != null) {
                    query.append(" AND TABLE_SCHEMA='" + schema + "'");
                }
                break;
            default:
                break;
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

    public void enableConstrains(String schema, Connection trgtconn, String table_name, int dbtype) {
        Statement stmt = null;
        try {
            stmt = trgtconn.createStatement();
            //Get the names of table constrainst
            ArrayList cons_names = getNamesOfTableConstraints(schema, table_name, stmt, dbtype);
            if (cons_names != null) {
                for (int i = 0; i < cons_names.size(); i++) {
                    //Query All Constraints from the target table being processed
                    StringBuilder enablesql = new StringBuilder();
                    switch (dbtype) {
                        case 1:
                            enablesql.append("ALTER TABLE ");
                            if (schema != null) {
                                enablesql.append(schema + "." + table_name);
                            } else {
                                enablesql.append(table_name);
                            }
                            enablesql.append(" ENABLE CONSTRAINT " + cons_names.get(i));
                            System.out.println("[Oracle]Enable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                            break;
                        case 3:
                            enablesql.append("ALTER TABLE ");
                            if (schema != null) {
                                enablesql.append(schema + "." + table_name);
                            } else {
                                enablesql.append(table_name);
                            }
                            enablesql.append(" CHECK CONSTRAINT " + cons_names.get(i));
                            System.out.println("[SQLServer]Enable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                            break;
                        default:
                    }
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
        }
    }

    public void disableConstrains(String schema, Connection trgtconn, String table_name, int dbtype) {
        Statement stmt = null;
        try {
            stmt = trgtconn.createStatement();
            //Get the names of table constrainst
            ArrayList cons_names = getNamesOfTableConstraints(schema, table_name, stmt, dbtype);
            if (cons_names != null) {
                for (int i = 0; i < cons_names.size(); i++) {
                    //Query All Constraints from the target table being processed
                    StringBuilder disablesql = new StringBuilder();
                    switch (dbtype) {
                        case 1:
                            disablesql.append("ALTER TABLE ");
                            if (schema != null) {
                                disablesql.append(schema + "." + table_name);
                            } else {
                                disablesql.append(table_name);
                            }
                            disablesql.append(" DISABLE CONSTRAINT " + cons_names.get(i));
                            System.out.println("[Oracle ]Disable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                            break;
                        case 3:
                            disablesql.append("ALTER TABLE ");
                            if (schema != null) {
                                disablesql.append(schema + "." + table_name);
                            } else {
                                disablesql.append(table_name);
                            }
                            disablesql.append(" NOCHECK CONSTRAINT " + cons_names.get(i));
                            System.out.println("[SQLServer]Disable Constraint [" + cons_names.get(i) + "] on table : " + table_name);
                            break;
                        default:
                    }
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
        }
    }

    private String getPWString(String pw) {
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
     * args[0] - Target DB Schema
     * args[1] - Target DB Table Name
     * args[2] - Target DB Connection String
     * args[3] - Target DB Login
     * args[4] - Target DB Passwd
     * args[5] - enable/disable constraint flag
     */
    public static void main(String[] args) {
        /*
        if (args.length == 6) {
        System.out.println("\n Target DB Schema : " + args[0]);
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
         */
    }
}
