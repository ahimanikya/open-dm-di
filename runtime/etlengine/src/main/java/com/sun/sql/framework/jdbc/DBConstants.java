/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)DBConstants.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.jdbc;

/**
 * Defines constant values that are used by all other SQL DB classes.
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public interface DBConstants {

    /** Enumerated constant value representing ANSI92 database type. */
    public static final int ANSI92 = 10;
    public static final int JDBC = 15;

    /** Enumerated constant value representing Axion database type. */
    public static final int AXION = 50;
    
    /** Enumerated constant value representing Derby database type. */
    public static final int DERBY = 80;    

	 /** Enumerated constant value representing Postgres database type. */
	public static final int POSTGRESQL = 85;

	/** Enumerated constant value representing Postgres database type. */
	public static final int MYSQL = 90;
    /** Enumerated constant value representing DB2 database type. */
    public static final int DB2V5 = 42;
    public static final int DB2V7 = 40;
    public static final int DB2V8 = 45;

    /** Enumerated constant value representing MS SQL Server database type. */
    public static final int MSSQLSERVER = 60;

    /** Enumerated constant value representing Oracle8 database type. */
    public static final int ORACLE8 = 20;

    /** Enumerated constant value representing Oracle9 database type. */
    public static final int ORACLE9 = 30;

    /** Enumerated constant value representing Sybase database type. */
    public static final int SYBASE = 70;

    /** Enumerated constant value representing ANSI92 database type. */
    public static final String ANSI92_STR = "ANSI92";

    /**
     * Generic DB, separate from ANSI or Base DB, so the it will allow us
     * customize for JDBC eWay w/o effecting other DB's .
     */
    public static final String JDBC_STR = "JDBCDB";

    /** Enumerated constant value representing Axion database type. */
    public static final String AXION_STR = "INTERNAL";
    
    /** Enumerated constant value representing Derby database type. */
    public static final String DERBY_STR = "DERBY";    

	/** Enumerated constant value representing Postgres database type. */
    public static final String POSTGRES_STR = "POSTGRES";

	/** Enumerated constant value representing Postgres database type. */
    public static final String MYSQL_STR = "MYSQL";
    /** Enumerated constant value representing DB2 database type. */
    public static final String DB2V5_STR = "DB2V5";
    public static final String DB2V7_STR = "DB2V7";
    public static final String DB2V8_STR = "DB2V8";
    public static final String DB2_STR = "DB2";

    /** Enumerated constant value representing MS SQL Server database type. */
    public static final String MSSQLSERVER_STR = "MSSQLSERVER";
    public static final String SQLSERVER_STR = "SQLSERVER";

    /** Enumerated constant value representing Oracle8 database type. */
    public static final String ORACLE8_STR = "ORACLE8";

    /** Enumerated constant value representing Oracle9 database type. */
    public static final String ORACLE9_STR = "ORACLE9";

    public static final String ORACLE_STR = "ORACLE";

    /** Enumerated constant value representing Sybase database type. */
    public static final String SYBASE_STR = "SYBASE";

    // Do not include JDBC! DBType as returned by eWay thru DBConnectionDefinition
    public static final String[] SUPPORTED_DB_TYPE_STRINGS = {ORACLE_STR, DB2_STR, 
        SQLSERVER_STR, SYBASE_STR, DERBY_STR,POSTGRES_STR,MYSQL_STR};

    public static final String JDBC_URL_PREFIX_ORACLE = "jdbc:oracle:";
	public static final String JDBC_URL_PREFIX_POSTGRES = "jdbc:postgres:";
    public static final String JDBC_URL_PREFIX_AXION = "jdbc:axiondb:";
    public static final String JDBC_URL_PREFIX_DERBY = "jdbc:derby:";	
    // Do not use seebeyond prefix for below as Data Direct Drivers may be rebranded.
    public static final String JDBC_URL_PREFIX_DB2 = ":db2:";
    public static final String JDBC_URL_PREFIX_SQLSERVER = ":sqlserver:";
    public static final String JDBC_URL_PREFIX_SYBASE = ":sybase:";
	public static final String JDBC_URL_PREFIX_MYSQL = "jdbc:mysql:";

    public static final String[] SUPPORTED_DB_URL_PREFIXES = {JDBC_URL_PREFIX_ORACLE,
                                                        JDBC_URL_PREFIX_DB2,
                                                        JDBC_URL_PREFIX_AXION,
                                                        JDBC_URL_PREFIX_SQLSERVER,
                                                        JDBC_URL_PREFIX_SYBASE,
		                                        JDBC_URL_PREFIX_POSTGRES,
                                                        JDBC_URL_PREFIX_DERBY,
							JDBC_URL_PREFIX_MYSQL};
}
