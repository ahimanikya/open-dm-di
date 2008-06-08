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
 * @(#)DBConnectionFactory.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.jdbc;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import javax.imageio.spi.ServiceRegistry;

import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.exception.DBSQLException;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.etl.engine.spi.DBConnectionProvider;

/**
 * DBConnectionFactory is used to serve out SQLDBSession The actual physical
 * connection handling is implemented by classes that extend this class. This
 * class is a singleton.
 * 
 * @author Sudhi Seshachala
 * @author Jonathan Giron
 * @version 
 */
public class DBConnectionFactory {
	/** Property name for dbType */
	public static final String PROP_DBTYPE = "dbType";

	/** Property for Driver Class * */
	public static final String PROP_DRIVERCLASS = "DRIVER";

	/** Property Otd Path */
	public static final String PROP_DS_JNDI_PATH = "dsJndiPath";

	/** Property Otd Path */
	public static final String PROP_OTD_PATH = "otdPathName";

	/** Property name for password */
	public static final String PROP_PASSWORD = "password";

	/** Property name for JDBC URL */
	public static final String PROP_URL = "url";

	/** Property name for username */
	public static final String PROP_USERNAME = "username";

	/** * an Instance of DBSessionFactory */
	private static volatile DBConnectionFactory INSTANCE = null;

	/* Log Category */
	private static final String LOG_CATEGORY = DBConnectionFactory.class.getName();

	private static final String PREFIX_LDAPS = "ldaps:";

	private static final String PREFIX_LDAP = "ldap:";

	/**
	 * Serves out service handles using the Singleton pattern. Enforces only one
	 * instance of this class in the system.
	 * 
	 * @return a service handle
	 */
	public static DBConnectionFactory getInstance() {
		if (INSTANCE == null) {
			synchronized (DBConnectionFactory.class) {
				if (INSTANCE == null) {
					if (INSTANCE == null) {
						INSTANCE = new DBConnectionFactory();
					}
				}
			}
		}
		return INSTANCE;
	}

	public static boolean isLDAPQuery(String ldapQueryStr) {
		boolean ret = false;
		String tmpStr = null;
		if (ldapQueryStr != null) {
			tmpStr = ldapQueryStr.trim().toLowerCase();
			if (tmpStr.startsWith(PREFIX_LDAP) || (tmpStr.startsWith(PREFIX_LDAPS))) {
				ret = true;
			}
		}
		return ret;
	}

	/**
	 * Constructor, keep it protected. the package.
	 */
	protected DBConnectionFactory() {
	}

	/**
	 * Releases the given connection
	 * 
	 * @param connectionName
	 *            name of connection
	 * @param con
	 *            Connection to be released
	 */
	public void closeConnection(Connection con) {
		DBConnectionProvider connectionProvider = findDBConnectionProvider();
		if (connectionProvider != null) {
			connectionProvider.closeConnection(con);
		}
		// if (con != null) {
		// try {
		// con.close();
		// } catch (Exception e) {
		// Logger.printThrowable(Logger.ERROR, LOG_CATEGORY,
		// "DBConnectionFactory", "Could not close connection ", e);
		// }
		// }
	}

	/**
	 * Releases DB resources.
	 * 
	 * @param con
	 * @param stmnt
	 * @param rs
	 * @param forceRollback
	 */
	public void closeDBResources(Connection con, Statement stmnt, ResultSet rs,
			boolean forceRollback) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {
				Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, "DBConnectionFactory",
						"Could not close ResultSet ", e);
			}
		}

		if (stmnt != null) {
			try {
				stmnt.close();
			} catch (Exception e) {
				Logger.printThrowable(Logger.WARN, LOG_CATEGORY, "DBConnectionFactory",
						"Could not close Statement ", e);
			}
		}

		if (con != null) {
			try {
				if (forceRollback) {
					try {
						con.rollback();
					} catch (Exception ex) {
						Logger.printThrowable(Logger.WARN, LOG_CATEGORY, "DBConnectionFactory",
								"Could not rollback. ", ex);
					}
				}
				con.close();
			} catch (Exception e) {
				Logger.printThrowable(Logger.WARN, LOG_CATEGORY, "DBConnectionFactory",
						"Could not close connection ", e);
			}
		}
	}

	/**
	 * Releases DB resources.
	 * 
	 * @param con
	 * @param stmnt
	 * @param rs
	 */
	public void closeDBResources(Connection con, Statement stmnt, ResultSet rs) {
		closeDBResources(con, stmnt, rs, false);
	}

	/**
	 * Checks URL to identify the supported Database.
	 * 
	 * @param connProps
	 * @return
	 * @throws BaseException
	 */
	private int getDatabaseVersionBasedOnURL(Properties connProps, boolean needLDAPLookup)
			throws BaseException {
		String jdbcURL = connProps.getProperty(PROP_URL);
		if (jdbcURL != null) {
			jdbcURL = jdbcURL.toLowerCase();
			if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_ORACLE) != -1) {
				return DBConstants.ORACLE9;
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_DB2) != -1) {
				return getDB2Version(connProps, needLDAPLookup);
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_SQLSERVER) != -1) {
				return DBConstants.MSSQLSERVER;
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_AXION) != -1) {
				return DBConstants.AXION;
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_SYBASE) != -1) {
				return DBConstants.SYBASE;
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_DERBY) != -1) {
				return DBConstants.DERBY;
			} else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_POSTGRES) != -1) {
				return DBConstants.POSTGRESQL;
			}else if (jdbcURL.indexOf(DBConstants.JDBC_URL_PREFIX_MYSQL) != -1) {
				return DBConstants.MYSQL;
			}else {
				return DBConstants.JDBC;
			}
		} else {
			return DBConstants.JDBC;
		}
	}

	/**
	 * Gets identity of database instance referenced by the given
	 * DBConnectionDefinition.
	 * 
	 * @param connProps
	 *            Properties instance containing connection parameters
	 * @param cl
	 *            ClassLoader to use incase we need to connect to DB to identify
	 *            version.
	 * @return enumerated identity of database; one of ORACLE8, ORACLE9, AXION,
	 *         or ANSI92
	 * @throws BaseException
	 *             if error occurs while querying database
	 */
	public int getDatabaseVersion(Properties connProps) throws BaseException {
		String strDBType = connProps.getProperty(PROP_DBTYPE);
		boolean needLDAPLookup = false;

		if (isLDAPQuery(connProps.getProperty(PROP_USERNAME))
				|| isLDAPQuery(connProps.getProperty(PROP_PASSWORD))) {
			// LDAP lookup is only supported by eGate in runtime environment, so
			// use
			// default versions.
			needLDAPLookup = true;
		}

		if (StringUtil.isNullString(strDBType)) {
			throw new BaseException("dbType string cannot be null"); // NOI18N
		} else {
			strDBType = strDBType.toUpperCase();
		}

		if (strDBType.indexOf(DBConstants.ORACLE_STR) != -1) {
			return getOracleVersion(connProps, needLDAPLookup);
		} else if (strDBType.indexOf(DBConstants.DB2_STR) != -1) {
			return getDB2Version(connProps, needLDAPLookup);
		} else if (strDBType.indexOf(DBConstants.SQLSERVER_STR) != -1) {
			return DBConstants.MSSQLSERVER;
		} else if (strDBType.indexOf(DBConstants.AXION_STR) != -1) {
			return DBConstants.AXION;
		} else if (strDBType.indexOf(DBConstants.SYBASE_STR) != -1) {
			return DBConstants.SYBASE;
		} else if (strDBType.indexOf(DBConstants.DERBY_STR) != -1) {
			return DBConstants.DERBY;
		} else if (strDBType.indexOf(DBConstants.POSTGRES_STR) != -1) {
			return DBConstants.POSTGRESQL;
		} else if (strDBType.indexOf(DBConstants.MYSQL_STR) != -1) {
				return DBConstants.MYSQL;
		}else {
			// Not supported or JDBC eWay.
			// Check URL if any DB is any of the one we already support.
			return getDatabaseVersionBasedOnURL(connProps, needLDAPLookup);
		}
	}

	/**
	 * Gets a new Connection using the given DBConnectionParameters for
	 * configuration data.
	 * 
	 * @param conDef
	 *            DBConnectionParameter containing connection configuration
	 *            info.
	 * @param cl
	 *            ClassLoader to use to load JDBC driver
	 * @return new Connection instance
	 * @throws BaseException
	 *             if error occurs while constructing connection
	 */
	public Connection getConnection(DBConnectionParameters conDef) throws BaseException {
		// String driver = conDef.getDriverClass();
		// String username = conDef.getUserName();
		// String password = conDef.getPassword();
		// String url = conDef.getConnectionURL();
		// return createConnection(driver, url, username, password);
		DBConnectionProvider connectionProvider = findDBConnectionProvider();
		if (connectionProvider != null) {
			return connectionProvider.getConnection(conDef);
		}

		return null;
	}

	/**
	 * Gets a new Connection using configuration parameters in the given
	 * Properties instance.
	 * 
	 * @param connProps
	 *            Properties instance containing connection configuration info
	 * @param cl
	 *            ClassLoader to use to load JDBC driver
	 * @return new Connection instance
	 * @throws BaseException
	 *             if error occurs while constructing connection
	 */
	public Connection getConnection(Properties connProps) throws BaseException {
		// String driver = connProps.getProperty(PROP_DRIVERCLASS);
		// String username = connProps.getProperty(PROP_USERNAME);
		// String password = connProps.getProperty(PROP_PASSWORD);
		// String url = connProps.getProperty(PROP_URL);
		// return createConnection(driver, url, username, password);
		DBConnectionProvider connectionProvider = findDBConnectionProvider();
		if (connectionProvider != null) {
			return connectionProvider.getConnection(connProps);
		}

		return null;
	}

	public boolean isAxionConnection(Connection con) throws SQLException {
		return (con.getMetaData().getURL().toLowerCase().indexOf("axiondb") != -1);
	}

	/**
	 * Invokes <code>shutdown</code> command on Connections made to the
	 * internal (i.e., AxionDB) database. Has no effect for Connections made to
	 * other databases.
	 * 
	 * @param con
	 *            Connection upon which <code>shutdown</code> is to be
	 *            invoked.
	 * @param defragFlag
	 *            true if the given defrag statement (Axion only) should be
	 *            executed
	 * @param defragStmt
	 *            defrag statement to be executed if defragFlag is true
	 */
	public void shutdown(Connection con, boolean defragFlag, String defragStmt) {
		if ((con == null) || (!defragFlag) || (defragStmt == null)) {
			return;
		}

		Statement stmt = null;
		try {
			if (!con.isClosed()) {
				if (isAxionConnection(con)) {
					stmt = con.createStatement();
					stmt.execute(defragStmt);
				}
			}
		} catch (Exception ignore) {
			// WT #63093: ignore Exception occurring due to shutdown.
			// Shutdown is akin to closing a JDBC connection at the end of a DB
			// session, so we don't have to deal with thrown exceptions.
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ignore) {
					// ignore
				}
			}
		}
	}

	public void setAutoCommit(Connection conn, boolean val) throws SQLException {
		String jdbcUrl = conn.getMetaData().getURL();

		if (jdbcUrl != null) {
			jdbcUrl = jdbcUrl.toLowerCase();
			if (jdbcUrl.indexOf(":sybase:") > -1) {
				// Data direct is OK, but jConnector cribs, so...
				conn.commit();
			}
		}

		conn.setAutoCommit(val);
	}

	/**
	 * Creates a {@link java.sql.Connection}using the given parameters.
	 * 
	 * @param driverName
	 *            class name of driver to use in establishing the connection
	 * @param url
	 *            JDBC URL representing the host and database to which we wish
	 *            to connect
	 * @param username
	 *            username to use in authenticating the connection
	 * @param password
	 *            password to use in authenticating the connection
	 * @param cl
	 *            ClasssLoader to use to load the JDBC driver.
	 * @return Connection to the database referenced by the given parameters
	 * @throws DBSQLException
	 */
//	private Connection createConnection(String driverName, String url, String username,
//			String password) throws DBSQLException {
//		try {
//			registerDriverInstance(driverName);
//			Connection conn = DriverManager.getConnection(url, username, password);
//			conn.setAutoCommit(false);
//			return conn;
//		} catch (Exception e) {
//			throw new DBSQLException("Failed to get DB connection using driver " + driverName, e);
//		}
//	}

	private int getDB2Version(Properties connProps, boolean needLDAPLookup) {
		if (needLDAPLookup) {
			return DBConstants.DB2V7;
		}

		Connection conn = null;
		try {
			conn = getConnection(connProps);
			if (conn.getMetaData().getDatabaseMajorVersion() >= 8
					&& conn.getMetaData().getDatabaseMinorVersion() >= 1) {
				return DBConstants.DB2V8; // Version V8.1.2+
			} else {
				if (conn.getMetaData().getDatabaseMajorVersion() == 5) {
					return DBConstants.DB2V5; // V5R1 and V5R3
				} else {
					return DBConstants.DB2V7; // Supported under V8 is DB2V7
				}
			}
		} catch (Exception ex) {
			return DBConstants.DB2V7; // Assume DB2V7
		} finally {
			closeConnection(conn);
		}
	}

	private int getOracleVersion(Properties connProps, boolean needLDAPLookup) {
		if (needLDAPLookup) {
			return DBConstants.ORACLE9;
		}

		// Execute the following statement and parse the result
		// to find out the major version.
		Connection conn = null;
		try {
			conn = getConnection(connProps);
			Statement stmt = conn.createStatement();
			String oracleVersionSQL = "select VALUE from nls_database_parameters "
					+ " where parameter ='NLS_RDBMS_VERSION'";
			ResultSet rs = stmt.executeQuery(oracleVersionSQL);
			if (rs.next()) {
				String version = rs.getString("VALUE"); // NOI18N
				int index = version.indexOf(".");
				String majorVersion = version.substring(0, index);
				int intVersion = StringUtil.getInt(majorVersion);
				return (intVersion >= 9) ? DBConstants.ORACLE9 : DBConstants.ORACLE8;
			} else {
				return DBConstants.ORACLE8;
			}
		} catch (Exception ex) {
			// If all else fails, assume Oracle 9i. Subsequent SQLExceptions
			// will at least
			// have some concrete and relevant message to display to the user.
			return DBConstants.ORACLE9;
		} finally {
			closeConnection(conn);
		}
	}

//	/**
//	 * Registers an instance of Driver associated with the given driver class
//	 * name. Does nothing if an instance has already been registered with the
//	 * JDBC DriverManager.
//	 * 
//	 * @param driverName
//	 *            class name of driver to be created
//	 * @return Driver instance associated with <code>driverName</code>
//	 * @throws Exception
//	 *             if error occurs while creating or looking up the desired
//	 *             driver instance
//	 */
//	private void registerDriverInstance(final String driverName) throws Exception {
//		synchronized (nameToDriverClassMap) {
//			Driver driver = (Driver) nameToDriverClassMap.get(driverName);
//			if (driver == null) {
//				Driver originalDriver = (Driver) Class.forName(driverName).newInstance();
//				driver = new JDBCProxyDriver(originalDriver);
//				DriverManager.registerDriver(driver);
//				nameToDriverClassMap.put(driverName, driver);
//			}
//		}
//	}

	private DBConnectionProvider findDBConnectionProvider() {

		// try with the default classloader i.e
		// Thread.currentThread().getContextClassLoader()
		Iterator<DBConnectionProvider> it = ServiceRegistry
				.lookupProviders(DBConnectionProvider.class);
		if (it.hasNext()) {
			return it.next();
		}

		/*
		 * TODO it might happen that the
		 * Thread.currentThread().getContextClassLoader() and the class loader
		 * which loads DBConnectionFactory.class are the same in that case one
		 * can just return 
		 */

		
		/*
		 * This gives the user/module/components that use etlengine DBConnection
		 * factory an option to associate a required class loader with the
		 * DBConnectionFactory class. Our requirement is to get the classLoader
		 * whose getResources() should be able to point to
		 * META-INF/services/com.sun.etl.engine.spi.DBConnectionProvider
		 */
		ClassLoader loader = DBConnectionFactory.class.getClassLoader();
		/*
		 * the default class loader used by the
		 * ServiceRegistry.lookupProviders() is the current thread Context
		 * ClassLoader, however this cause problem when we try to get the
		 * DBConnectionProvider from within a JBI component. For jbi component
		 * when the service request is handled,the class loader happens to be
		 * webAppClassloader (i.e.Thread.currentThread().getContextClassLoader()
		 * is webAppClassloader ) . This result in a failure of locating the SPI
		 * DBConnectionProvider. So in order to avoid using the
		 * webAppClassloader the above mechanism has been used
		 */
		it = ServiceRegistry.lookupProviders(DBConnectionProvider.class, loader);
		if (it.hasNext()) {
			return it.next();
		}

		return null;
	}
}
