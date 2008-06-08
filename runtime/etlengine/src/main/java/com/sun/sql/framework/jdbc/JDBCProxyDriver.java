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
 * @(#)JDBCProxyDriver.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Traditionally, the mechanism is that you put the JDBC driver somewhere in the classpath
 * and then use class.forName() to find and load the driver. One problem with this is that
 * it presumes that your driver is in the classpath. This means either packaging the
 * driver in your jar, or having to stick the driver somewhere (probably unpacking it
 * too), or modifying your classpath. But why not use something like URLClassLoader and
 * the overload of class.forName() that lets you specify the ClassLoader?" Because the
 * DriverManager will refuse to use a driver not loaded by the system ClassLoader. The
 * workaround for this is to create a shim class that implements java.sql.Driver. This
 * JDBCProxyDriver class will do nothing but call the methods of an instance of a JDBC
 * driver that we loaded dynamically works because DriverShim was loaded by the system
 * class loader, and the DriverManager doesn't care that it invokes a class that wasn't.
 * We must perform the registration on the instance ourselves, because although
 * Class.forName() will cause a registration to take place
 * 
 * @author Sudhendra Seshachala
 * @version 
 */
public class JDBCProxyDriver implements Driver {
    private Driver driver;

    JDBCProxyDriver(Driver d) {
        this.driver = d;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return this.driver.acceptsURL(url);
    }

    public Connection connect(String url, Properties info) throws SQLException {

        return this.driver.connect(url, info);
    }

    public int getMajorVersion() {
        return this.driver.getMajorVersion();
    }

    public int getMinorVersion() {
        return this.driver.getMinorVersion();
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return this.driver.getPropertyInfo(url, info);
    }

    public boolean jdbcCompliant() {
        return this.driver.jdbcCompliant();
    }

}
