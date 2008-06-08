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
 * @(#)ETLMonitorHelper.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.sun.jbi.engine.etl.Localizer;
import javax.management.ObjectName;
import com.sun.etl.engine.ETLEngine;
import com.sun.jbi.engine.etl.EtlMapEntry;
import com.sun.jbi.engine.etl.monitor.mbeans.Agent;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMBeanConfig;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitor;
import com.sun.sql.framework.jdbc.DBConnectionParameters;

/**
 * @author Sujit Biswas
 * 
 */
public class ETLMonitorHelper {

	private static final String ETL_M_PATH = "/eTL/m/";
	private static final String ETL_MONITORING_COLLAB_OBJECTNAME_PREFIX = "ETL Monitoring:type=service,collab=";
	private static transient final Logger mLogger = Logger.getLogger(ETLMonitorHelper.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
	private ETLMBeanConfig config = new ETLMBeanConfig();

	public void registerMbean(EtlMapEntry etlMapEntry, String serviceUnitName) {

		ETLEngine engine = etlMapEntry.getETLEngine();
		List l = engine.getConnectionDefList();

		String monitorConnStr = null;
		Iterator iter = l.iterator();
		;
		while (iter.hasNext()) {
			DBConnectionParameters element = (DBConnectionParameters) iter.next();
			if (element.getConnectionURL().contains(ETL_M_PATH)) {
				monitorConnStr = element.getConnectionURL();
				break;
			}

		}

		if (monitorConnStr == null) {
			mLogger.log(Level.INFO,mLoc.loc("INFO775: No Monitoring support for the given engine file"));
			return;
		}

		String collab = setEtlMbeanConfig(monitorConnStr, serviceUnitName);

		ETLMonitor mbean = null;
		try {
			mbean = new ETLMonitor(config);
			ObjectName objName = new ObjectName(ETL_MONITORING_COLLAB_OBJECTNAME_PREFIX + collab);
			Agent a = new Agent();
			a.registerMBean(mbean, objName);
			etlMapEntry.setMbeanObjectName(objName);
			mLogger.log(Level.INFO,mLoc.loc("INFO776: Registered mbean: {0}",objName));
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",e.getMessage()));
		}

	}

	/**
	 * @param monitorConnStr
	 * @return
	 */
	private String setEtlMbeanConfig(String monitorConnStr, String serviceUnitName) {
		StringTokenizer st = new StringTokenizer(monitorConnStr, ":");
		String jdbc = st.nextToken();
		String axiondb = st.nextToken();
		String collab = st.nextToken();
		String monitorDBDir = st.nextToken();
                if (st.hasMoreTokens()) {
                    monitorDBDir = monitorDBDir + ":" + st.nextToken();
                }

		String oid = getOid(monitorDBDir);
		String summaryTable = getSummaryTable();

		config.setProjectName("ETL Project");
		config.setApplicationName("ETL App");
		config.setCollabName(collab);
		config.setDeployable("");
		config.setDeploymentName(collab);
		config.setUnConvertedProjName("ETL Project");
		config.setUnConvertedCollabName(collab);
		config.setCollaborationType("ETL Collaboration");

		config.setLogFolder(monitorDBDir);

		config.setSummaryTableName(summaryTable);
		return serviceUnitName+"-"+collab;
	}

	private String getSummaryTable() {
		/*
		 * AxionDB axionDb = (AxionDB)
		 * DBFactory.getInstance().getDatabase(DB.AXIONDB);
		 * AxionPipelineStatements pipeLineStmt =
		 * axionDb.getAxionPipelineStatements();
		 * config.setSummaryTableName(pipeLineStmt.getSummaryTableName());
		 */

		return "SUMMARY";
	}

	private String getOid(String monitorDBDir) {
		int i = monitorDBDir.indexOf(ETL_M_PATH) + ETL_M_PATH.length();
		String tmp = monitorDBDir.substring(i);
		int j = monitorDBDir.substring(i).indexOf("/");
		return tmp.substring(0, j);
	}

	public void deregisterAllMonitoringMbean() {
		try {
			ObjectName objName = new ObjectName(ETL_MONITORING_COLLAB_OBJECTNAME_PREFIX +"*" );
			Agent a = new Agent();
			Set mbeans = a.getMBeanServer().queryNames(objName, null);
			
			Iterator iter = mbeans.iterator();
			
			for (int i = 0; iter.hasNext(); i++) {
				ObjectName o = (ObjectName) iter.next();
				a.unregisterMBean(o);
			}
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",e.getMessage()));
		} 
		
		
	}

	public void unregisterMbean(EtlMapEntry etlMapEntry) {
		ObjectName on = etlMapEntry.getMbeanObjectName();
		try {
			if (on != null) {
				Agent a = new Agent();
				a.unregisterMBean(on);
				mLogger.log(Level.INFO,mLoc.loc("INFO507: un Registered mbean: ",on));
			}
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",e.getMessage()));	
		}
		
	}

}
