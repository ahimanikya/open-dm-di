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
 * @(#)DetailsController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;
import com.sun.jbi.engine.etl.monitor.data.PageInfo;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;

/**
 * Controller for Details Page. A details page shows rejected rows for a given execution of a target
 * table.
 *
 * @author Ritesh Adval
 */
public class DetailsController extends BaseController {
	private static final Logger LOGGER = Logger.getLogger(DetailsController.class.getName());
    private String detailsData = null;

    protected String perform() throws Exception {
        Map returnMap = null;

        detailsData = null;
        String component = getComponentName();

        ETLServiceInfo serviceInfo = (ETLServiceInfo) this.getCtx().getRequest().getSession()
                .getAttribute(component);

        String executionId = this.getCtx().getRequest().getParameter("executionId");
        String targetTableQName = this.getCtx().getRequest().getParameter("targetTableName");
        if (executionId != null) {
            serviceInfo.setCurrentExecutionId(executionId);
        } else {
            executionId = serviceInfo.getCurrentExecutionId();
        }

        if (targetTableQName != null) {
            serviceInfo.setCurrentTargetTable(targetTableQName);
        } else {
            targetTableQName = serviceInfo.getCurrentTargetTableName();
        }

        String whereCondition = ETLMonitorServerConstants.getWhereConditionWithExecutionId(executionId);

        try {
            PageInfo detailsPageInfo = processPageInfo(serviceInfo);

            Integer limit = new Integer(detailsPageInfo.getPageSize());
            Integer offset = new Integer(detailsPageInfo.getCurrentOffset());

            detailsData = null;
            Object[] params = new Object[] { targetTableQName, whereCondition, limit, offset };
            String[] paramTypes = new String[] { "java.lang.String",
                                                 "java.lang.String",
                                                 "java.lang.Integer",
                                                 "java.lang.Integer" };
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY, params, paramTypes);

            String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
            if (exceptionStr == null) {
                detailsData = (String) returnMap.get(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY_RESULT);
            } else {
                this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
            }
        } catch (Exception ex) {
            this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION, ex.getMessage());
        }

        if (detailsData == null) {
            return ERROR;
        }
        
        return SUCCESS;
    }

    private PageInfo processPageInfo(ETLServiceInfo serviceInfo) {
        Map returnMap = null;
        String targetTableQName = serviceInfo.getCurrentTargetTableName();
        String executionId = serviceInfo.getCurrentExecutionId();
        PageInfo detailsPageInfo = serviceInfo.getDetailsPageInfo(serviceInfo.getCurrentExecutionId());

        try {

            String whereCondition = ETLMonitorServerConstants.getWhereConditionWithExecutionId(
                                                                                       executionId);

            Object[] params = new Object[] { targetTableQName, whereCondition };
            String[] paramTypes = new String[] { "java.lang.String",
                                                 "java.lang.String" };

            // Now in summaryPage set the total number of pages available
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY,
		                                            params,
		                                            paramTypes);

            String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
            if (exceptionStr == null) {
                Integer totalRows = (Integer) returnMap.get(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY_RESULT);
                detailsPageInfo.setTotalRowCount(totalRows.intValue());
            } 
        } catch (Exception ex) {
        	LOGGER.log(Level.WARNING, "processPageInfo()", ex);
        }

        return detailsPageInfo;
    }

    public String getDetailsData() {
        return detailsData;
    }
}
