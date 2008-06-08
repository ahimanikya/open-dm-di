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
 * @(#)SummaryController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.ObjectName;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;
import com.sun.jbi.engine.etl.monitor.data.PageInfo;
import com.sun.jbi.engine.etl.monitor.data.SummaryTotalData;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;

/**
 * A Summary Controller for the summary page. On success, transfers to the page which renders
 * summary table content.
 *
 * @author Ritesh Adval
 * @author Girish Patil
 * @version 
 */
public class SummaryController extends BaseController {
    private static final Logger LOGGER = Logger.getLogger(SummaryController.class.getName());

    private SummaryTotalData summaryTotalData = new SummaryTotalData();
    private String summaryData = null;

    private PageInfo processPageInfo(ETLServiceInfo serviceInfo, String whereCondition) {
        Map returnMap = null;
        PageInfo summaryPageInfo = serviceInfo.getSummaryPageInfo();

        try {
            Object[] params = new Object[] { whereCondition };
            String[] paramTypes = new String[] { "java.lang.String" };

            // Now in summaryPage set the total number of pages available
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY,
                                                     params,
                                                     paramTypes);
            String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

            if (exceptionStr == null) {
                Integer totalRows = (Integer) returnMap.get(
                        ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY_RESULT);
                        summaryPageInfo.setTotalRowCount(totalRows.intValue());
            } else {
                this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION,
                                                        exceptionStr);
            }

        } catch (Exception ex) {
            this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION,
                                                    ex.getMessage());
        }

        return summaryPageInfo;
    }


    public String getSummaryData() {
        return summaryData;
    }

    protected String perform() throws Exception {
        Map returnMap = null;
        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
        String exceptionStr = null;

        if (objName == null) {
            summaryData = null;
        } else {
            String component = getComponentName();

            //set transform params which will be used in xsl to replace param values
            this.getCtx().setTransformParam(MBeanUtil.ATTR_COMPONENT, component);

            //set detailsPageCommand so that after encoding url a session is maintained
            this.getCtx().setTransformParam("detailsPageCommand",
                                this.getCtx().getResponse().encodeURL("showDetails.do"));

            this.getCtx().setTransformParam("summaryTotalPageCommand",
                                this.getCtx().getResponse().encodeURL("showSummaryTotal.do"));

            this.getCtx().setTransformParam("executePurgeCommand",
                                this.getCtx().getResponse().encodeURL("executePurge.do"));

            ETLServiceInfo serviceInfo = (ETLServiceInfo)
                                this.getCtx().getRequest().getSession().getAttribute(component);

            try {
                String whereCondition = null;
                String startDate = null;
                String endDate = null;
                PageInfo summaryPageInfo = serviceInfo.getSummaryPageInfo();

                String filterDateAction = this.getCtx().getRequest().getParameter("FilterDateAction");

                if (filterDateAction != null && filterDateAction.length() > 0) {
                    startDate = this.getCtx().getRequest().getParameter("startDate");
                    endDate = this.getCtx().getRequest().getParameter("endDate");
                    summaryPageInfo.setStartDate(startDate);
                    summaryPageInfo.setEndDate(endDate);
                    summaryPageInfo.setCurrentPage(1);
                } else {
                    if (summaryPageInfo != null){
                        startDate = summaryPageInfo.getStartDate();
                        endDate = summaryPageInfo.getEndDate();
                    }
                }

                whereCondition = ETLMonitorServerConstants.getWhereConditionWithStartAndEndDate(startDate, endDate);
                summaryPageInfo = processPageInfo(serviceInfo, whereCondition);

                // Get the Summary Totals
                Object[] totalParam = new Object[] { whereCondition};
                String[] totalParamTypes = new String[] { "java.lang.String"};

                returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY,
                                                        totalParam,
                                                        totalParamTypes);

                exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

                if (exceptionStr == null) {
                    String temp = (String) returnMap.get(
                                    ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY_RESULT);
                    this.summaryTotalData.unmarshal(temp);
                    this.getCtx().getRequest().getSession().setAttribute("SummaryTotalData",
                                                                         summaryTotalData);
                    if (filterDateAction != null && filterDateAction.length() > 0) {
                        summaryPageInfo.reset();
                    }

                    // Get the Summary
                    Integer limit = new Integer(summaryPageInfo.getPageSize());
                    Integer offset = new Integer(summaryPageInfo.getCurrentOffset());

                    Object[] params = new Object[] { whereCondition, limit, offset};
                    String[] paramTypes = new String[] { "java.lang.String",
                                                         "java.lang.Integer",
                                                         "java.lang.Integer"};

                    returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY,
                                                            params,
                                                            paramTypes);

                    exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

                    if (exceptionStr == null) {
                        summaryData = (String) returnMap.get(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY_RESULT);
                    } else {
                        this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
                    }
                } else {
                    this.getCtx().getRequest().setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
                }

                // Populate request attributes required
                String status = getStatus();
                boolean isStartable = isStartable();
                boolean isStoppable = isStoppable();

                this.getCtx().getRequest().setAttribute("status", status);
                if (!isStartable){
                    this.getCtx().getRequest().setAttribute("btnStartDisable", "true");
                }

                if (!isStoppable){
                    this.getCtx().getRequest().setAttribute("btnStopDisable", "true");
                }

                this.getCtx().getRequest().setAttribute(MBeanUtil.ATTR_COMPONENT, component);

                if ((!isStartable) && (!isStoppable)){
                    this.getCtx().getRequest().setAttribute("inboundDoesNotSupportStartStop", "Inbound connector does not support START and STOP operations.");
                }
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING, "While displaying summary data", ex);
            }
        }

        if (summaryData == null || exceptionStr != null) {
            return ERROR;
        }

        return SUCCESS;
    }
}
