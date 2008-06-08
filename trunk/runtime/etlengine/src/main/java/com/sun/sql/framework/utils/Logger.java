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
 * @(#)Logger.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.logging.Level;

/**
 * The Logger class initializes and appends to a simple log file, which includes the date,
 * priority, subsystem, methodeName, callingObject, the log message, and optionally a
 * Throwable exception. Each log is created at a particular level and is associated with a
 * category. There are 5 levels - DEBUG, ERROR, FATAL, INFO and WARN. <code>
 * String logCategory = MyClass.class.getName(); <br>
 * Logger.print(Logger.INFO, logCategory, "Our Log Msg"); <br>
 * Logger.printThrowable(Logger.ERROR, logCategory, null, null, myexception);
 * </code><br>
 * 
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 * @version :
 */
public class Logger {

    /**
     * Log Debug messages
     */
    public static final int DEBUG = 10000;

    /**
     * Log Error messages
     */
    public static final int ERROR = 40000;

    /**
     * Log Fatal messages
     */
    public static final int FATAL = 50000;

    /**
     * Log Info messages
     */
    public static final int INFO = 20000;

    /**
     * Log Warning messages
     */
    public static final int WARN = 30000;

    /**
     * user supplied configuration file.
     */
    private static String configFile = "";

    /**
     * Default logger configuration file.
     */
    private static final String DEFAULT_CONFIG_FILE = "com/sun/sql/framework/utils/logger.xml";

    /**
     * variable indicating if logger is initialized.
     */
    private static boolean isInitialized = false;

    /**
     * Get code corresponding to a message assigned to the debug level
     * 
     * @return int representing debug level
     */
    public static synchronized int debugLevel() {
        return DEBUG;
    }

    /**
     * Get code corresponding to a message assigned to the error level
     * 
     * @return int representing info level
     */
    public static synchronized int errorLevel() {
        return ERROR;
    }

    /**
     * Get code corresponding to a message assigned to the info level
     * 
     * @return int representing error level
     */
    public static synchronized int infoLevel() {
        return INFO;
    }

    /**
     * Check whether the supplied category is enabled for the DEBUG Level.
     * 
     * @param category The category of the massage (often the package name is used here)
     * @return boolean - true if this category is debug enabled, false otherwise.
     */
    public static synchronized boolean isDebugEnabled(String category) {
        return java.util.logging.Logger.getLogger(category).isLoggable(Level.FINE);
    }

    /**
     * Log a message.
     * 
     * @param level The log level of the message
     * @param category The category of the massage (often the package name is used here)
     * @param context The context the message is logged from
     * @param msg The string to log
     */
    public static synchronized void print(int level, String category, Object context, String msg) {
        printThrowable(level, category, context, msg, null);
    }

    /**
     * Log a message.
     * 
     * @param level The log level of the message
     * @param category The category of the massage (often the package name is used here)
     * @param context The context the message is logged from
     * @param cause The exception caused this
     */
    public static synchronized void print(int level, String category, Object context, Throwable cause) {
        printThrowable(level, category, context, cause.toString(), cause);
    }

    /**
     * Log a message.
     * 
     * @param level The log level of the message
     * @param category The category of the massage (often the package name is used here)
     * @param msg The string to log
     */
    public static synchronized void print(int level, String category, String msg) {
        printThrowable(level, category, null, msg, null);
    }

    /**
     * Log a message and the trace of an exception.
     * 
     * @param level The log level of the message
     * @param category The category of the massage (often the package name is used here)
     * @param context The context the message is logged from
     * @param msg The string to log
     * @param e The exception to trace
     */
    public static synchronized void printThrowable(int level, String category, Object aContext, String msg, Throwable e) {

        synchronized (Logger.class) {
            if (!isInitialized) {
                getInstance();
            }
        }

        String outputMsg = msg;
        try {
            if (e != null) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                outputMsg += " - " + sw;

                if (outputMsg.length() > 1900) {
                    outputMsg = outputMsg.substring(0, 1900);
                    outputMsg += aContext;
                    outputMsg += "\n MESSAGE WAS TRUNCATED \n";
                }
            }

            msg = outputMsg;

            java.util.logging.Logger logInstance = java.util.logging.Logger.getLogger(category);
            switch (level) {
                case Logger.FATAL:
                    logInstance.log(Level.SEVERE, msg);
                    break;

                case Logger.ERROR:
                    logInstance.log(Level.SEVERE, msg);
                    break;

                case Logger.INFO:
                    logInstance.log(Level.INFO, msg);
                    break;

                case Logger.WARN:
                    logInstance.log(Level.WARNING, msg);
                    break;

                case Logger.DEBUG:
                default:
                    logInstance.log(Level.FINE, msg);
                    break;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Set config file.
     * 
     * @param fileName Name of the logger configuration file.
     */
    public static void setConfigFile(String fileName) {
        if (fileName != null) {
            configFile = fileName;
        }
        getInstance();
    }

    private static boolean configureJdk14Logging() {
        // Do nothing for now...configuration on RTS should be handled via administration
        // tools, or in jdk/jre/lib/logging.properties
        return true;
    }

    /**
     * Initialize if it is not initialized yet and set the config file.
     */
    private static void getInstance() {
        if (configFile.equals("")) {
            configFile = DEFAULT_CONFIG_FILE;
        }

        if (!log4jIsConfigured()) {
            configureJdk14Logging();
        }

        isInitialized = true;
    }

    /**
     * Configures Log4J via reflection to avoid having any dependencies (build-time or
     * run-time) on log4j classes. When used in an appserver without log4j in its server
     * classpath, this method will return false.
     * 
     * @return true if Log4J is installed and successfuly configured; false otherwise.
     */
    private static boolean log4jIsConfigured() {
        boolean configured = false;

        try {
            URL url = Logger.class.getClassLoader().getResource(configFile);

            Class cfgClass = Class.forName("org.apache.log4j.xml.DOMConfigurator", true, Logger.class.getClassLoader());

            if (cfgClass != null) {
                // Java system property "run.mode" is set in netbeans/bin/runide.bat
                if ("debug".equalsIgnoreCase(System.getProperty("run.mode"))) {
                    System.out.println("[SQLFramework] Log4J is installed; attempting to read " + "configuration file.");
                }

                Method configureAndWatch = cfgClass.getMethod("configureAndWatch", new Class[] { String.class});
                if (url == null) {
                    System.out.println("ERROR: Can't find config file in Classpath: " + configFile + "; using file search");

                    File f = new File(configFile);

                    System.out.println("Config file " + configFile);
                    System.out.println("Config path " + f.getName());

                    Object[] configName = new Object[] { f.getName()};

                    Method configureStr = cfgClass.getMethod("configure", new Class[] { String.class});
                    if (configureStr != null) {
                        configureStr.invoke(null, configName);
                    }

                    if (configureAndWatch != null) {
                        configureAndWatch.invoke(null, configName);
                    }
                } else {
                    Method configureUrl = cfgClass.getMethod("configure", new Class[] { URL.class});

                    if (configureUrl != null) {
                        configureUrl.invoke(null, new Object[] { url});
                    }

                    if (configureAndWatch != null) {
                        configureAndWatch.invoke(null, new Object[] { url.getFile()});
                    }
                }
            }

            configured = true;
        } catch (Throwable t) {
            if (t instanceof ClassNotFoundException) {
                // Don't log ClassNotFoundException against log4j DOMConfigurator.
                String msg = t.getLocalizedMessage();
                if (msg.indexOf("org.apache.log4j.xml.DOMConfigurator") != -1) {
                    return false;
                }
            }

            System.out.println("Error while attempting to configure Log4J logger:");
            Throwable cause = t.getCause();
            if (cause != null) {
                cause.printStackTrace(System.out);
            } else {
                t.printStackTrace(System.out);
            }
        }

        return configured;
    }

    /**
     * Constructor
     */
    public Logger() {
        isInitialized = true;
    }
}
