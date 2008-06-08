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
 * @(#)ETLMonitorMBean.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.util.Map;
import java.util.Properties;

import javax.management.DynamicMBean;
import javax.management.MBeanRegistration;

/**
 * @author Ritesh Adval
 * @author Girish Patil
 * @version 
 */
public interface ETLMonitorMBean extends DynamicMBean, MBeanRegistration{
	public static final String STATUS_UP = "Up" ;
	public static final String STATUS_DOWN = "Down" ;
	public static final String STATUS_NOT_DEPLOYED = "NotDeployed" ;
	public static final String STATUS_UNKNOWN = "Unknown" ;
    public static final String ATTR_STATUS = "Status" ;	
	
    Map deleteRecords(String anOlderThanDate);

    Map executeDetailCountQuery(String aTagetTableName, String aWhereCondition);

    Map executeDetailQuery(String aTargetTableName, String aWhereCondition, Integer aLimit, Integer anOffset);

    Map executeDetailQuery(String aTargetTableName, String aWhereCondition, String aGroupBy, String anOrderBy, Integer aLimit, Integer anOffset);

    Map executeSummaryCountQuery(String aWhereCondition);

    Map executeSummaryQuery(String aWhereCondition, Integer aLimit, Integer anOffset);

    Map executeSummaryQuery(String aWhereCondition, String aGroupBy, String anOrderBy, Integer aLimit, Integer anOffset);

    Map executeSummaryTotalQuery(String aWhereCondition);

    Map getDetailsTableContent(String aTagetTableName, String aWhereCondition, String aFlag);

    Map getPurgeInfo(String anOlderThanDate);

    Map getTableContent(String aTagetTableName, String aWhereCondition, String aFlag);
    
    // 
    // TODO Remove below methods once we eGate puts this interface in API jar and we 
    // extend EmManagementInterface
    //
    
	// EmManagementInterface methods
	/**
	 * Start method: Start the component- the semantics of this operation is left to implementation
	 */
	public void start();

	/**
	 * Restart method: Restart the component- the semantics of this operation is left to implementation
	 */
	public void restart();

	/**
	 * Stop method: Stop the component - the semantics of this operation is left to implementation 
	 */
	public void stop();

	/**
	 * Get status method: Return the status of the component e.g. Up/Down/Unknown
	 */
	public String getStatus();

	/**
	 * Get properties method: Return a list of properties: name-value pairs
	 */
	public Properties getProperties();

	/**
	 * isStartable method: This method will be used to determine whether a "start" button would be 
	 * presented to the user return true if the component can be started (remotely)
	 */
	public Boolean isStartable();

	/**
	 * isRestartable method: This method will be used to determine whether a "restart" button 
	 * would be presented to the user return true if the componennt can be restarted. 
	 */

	public Boolean isRestartable();

	/**
	 * isStoppable method: This method will be used to determine whether a "stop" button would 
	 * be presented to the user return true if the component can be stopped.
	 */
	public Boolean isStoppable();
	//  End of EmManagementInterface methods    
}
