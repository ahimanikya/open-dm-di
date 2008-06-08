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
 * @(#)ETLMBeanLoader.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.io.File;

import org.axiondb.service.AxionDBQuery;
import org.axiondb.service.AxionDBQueryImpl;


/**
 * @author Ritesh Adval
 * @version 
 */
public class ETLMBeanLoader  {

    public static final String StartupServiceNamePrefix = "ETLCollabMonitor";

    private static final String LOG_CATEGORY = ETLMBeanLoader.class.getName();

    private static java.util.logging.Logger sContextEnter = java.util.logging.Logger.getLogger("com.stc.EnterContext");

    private static java.util.logging.Logger sContextExit = java.util.logging.Logger.getLogger("com.stc.ExitContext");
    private String loggingContextName = null;

    private String mName = null;
    private ETLMBeanConfig config;

    public ETLMBeanLoader() throws Exception {
        super();

        // create a blank ETLMbeanConfig and set it in this loader
         config = new ETLMBeanConfig();
       
    }

    public Object createMbean() throws Exception {
        ETLMonitor mBean = null;

        try {
            loggingContextName = (mName != null) ? mName : config.getName();
            if (loggingContextName != null) {
                sContextEnter.info(loggingContextName);
            }

            mBean = new ETLMonitor((ETLMBeanConfig) this.getConfig());
            
        } catch (Exception e) {
            
        } finally {
            if (loggingContextName != null) {
                sContextExit.info(loggingContextName);
            }
        }

        return mBean;
    }

    private ETLMBeanConfig getConfig() {
		return config;
	}

	public String getName() {
        return mName;
    }

    public void setName(String name) {
        mName = name;
    }

    public void stop() throws Exception {
        try {
            shutdownMonitorDatabase();
        } finally {
            
        }
    }

    /**
     * Deletes the directory represented by the given String, including all of its
     * contents, its subdirectories and their files.
     * 
     * @param dbLocation String representing directory whose contents are to be deleted,
     *        including all subdirectories and their files.
     */
    private void deleteDirectory(String dbLocation) {
        File rootDir = new File(dbLocation);
        if (rootDir.isDirectory()) {
            //if (Logger.isDebugEnabled(LOG_CATEGORY)) {
            //    Logger.print(Logger.DEBUG, LOG_CATEGORY, "Deleting contents of dbLocation.");
            //}
            deleteRecursively(rootDir);
        }
    }

    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] fileList = f.listFiles();
            if (fileList != null) {
                for (int i = 0; i < fileList.length; i++) {
                    deleteRecursively(fileList[i]);
                }
                f.delete();
            }
        } else {
            if (!f.delete()) {
                f.deleteOnExit();
            }
        }
    }

    /**
     * Shuts down the monitor database.
     */
    private void shutdownMonitorDatabase() {
    	ETLMBeanConfig config = getConfig();
        if (config instanceof ETLMBeanConfig) {
            ETLMBeanConfig etlMbeanConfig = (ETLMBeanConfig) config;
            String dbName = etlMbeanConfig.getCollabName();
            String dbLocation = ETLMonitor.resolveDbLocation(etlMbeanConfig);

            try {
                // Set logging context for this request.
                if (loggingContextName != null) {
                    sContextEnter.info(loggingContextName);
                }

                AxionDBQuery axionQuery = new AxionDBQueryImpl();
                axionQuery.shutdown(dbName, dbLocation);
                deleteDirectory(dbLocation);
            } catch (Exception ex) {
                //Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Error while shutting down monitor database for " + dbName + ":\n", ex);
            } finally {
                // Notify logging system to pop our context off the stack and publish it.
                if (loggingContextName != null) {
                    sContextExit.info(loggingContextName);
                }
            }
        }
    }
}
