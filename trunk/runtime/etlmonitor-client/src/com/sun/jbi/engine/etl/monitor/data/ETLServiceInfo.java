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
 * @(#)ETLServiceInfo.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.data;

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

/**
 * @author Ritesh Adval
 * ETLServiceInfo provides information about a given etl collaboration
 * in the EM.
 *
 * We create one ETLServiceInfo for each etl component string we get
 * when user click on a etl collboration in EM.
 * 
 * It keeps Summary and Details PageInfo objects so that same page can be
 * displayed which user has seen last time.
 * 
 * It also keeps the etl mbean object name for a given etl collaboration.
 */
public class ETLServiceInfo {
	
	private String component;
	private PageInfo summaryPageInfo;
	private Map detailsPageInfoMap = new HashMap();
	private ObjectName mBeanObjectName;
	private String currentExecutionId;
	private String currentTargetTableName;
	private int exportPageSize = 100;
	
	private int defaultPageSize;
	
	public ETLServiceInfo(String comp, ObjectName etlMbeanName, int pageSize, int anExportPageSize) {
		this.component = comp;
		this.mBeanObjectName = etlMbeanName;
		this.defaultPageSize = pageSize;
		this.summaryPageInfo = new PageInfo(pageSize);
		if (anExportPageSize > 0)
		    this.exportPageSize = anExportPageSize;
		
	}
	
	public PageInfo getSummaryPageInfo() {
		return this.summaryPageInfo;
	}
	
	public PageInfo getDetailsPageInfo(String executionId) {
		PageInfo pInfo = (PageInfo) this.detailsPageInfoMap.get(executionId);
		if(pInfo == null) {
			pInfo = new PageInfo(this.defaultPageSize);
			this.detailsPageInfoMap.put(executionId, pInfo);
		}
		
		return pInfo;
	}
	
	public String getComponent() {
		return this.component;
	}
	
	public ObjectName getETLMBeanObjectName() {
		return this.mBeanObjectName;
	}
	
	public void setCurrentExecutionId(String executionId) {
		this.currentExecutionId = executionId;
	}
	
	public String getCurrentExecutionId() {
		return this.currentExecutionId;
	}
	
	public void setCurrentTargetTable(String tagetTableName) {
		this.currentTargetTableName = tagetTableName;
	}
	
	public String getCurrentTargetTableName() {
		return this.currentTargetTableName;
	}
	
	public int getExportPageSize() {
	    return this.exportPageSize;
	}
}
