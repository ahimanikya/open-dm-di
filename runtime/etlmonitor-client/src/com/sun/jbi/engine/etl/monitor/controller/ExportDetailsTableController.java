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
 * @(#)ExportDetailsTableController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;


import java.io.OutputStream;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.infohazard.maverick.ctl.ThrowawayBean2;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;

/**
 * Provides functionality to export rejected rows for a given execution of a target table.
 *  
 * @author Ritesh Adval 
 */
public class ExportDetailsTableController extends ThrowawayBean2 {
    private String detailsTableContent;

    protected String perform() throws Exception {
        Map returnMap = null;
        int localTotalRows = 0;
        int localCurrentPageNumber = 1;
        int localLimit = 100;
        int localOffset = 0;
        boolean moreRecordsToProcess = true;
        String flag = null;
        byte[] buffData = null;
        OutputStream out = null;
        String exceptionStr = null;
        String component = (String) this.getCtx().getRequest().getSession().getAttribute("component");
        
        //set transform params which will be used in xsl to replace param values
        ETLServiceInfo serviceInfo = (ETLServiceInfo) this.getCtx().getRequest().getSession().getAttribute(component);
        if (serviceInfo != null) {
            localLimit = serviceInfo.getExportPageSize();
        }
        
        String executionId = this.getCtx().getRequest().getParameter("executionId");
        String whereCondition = null;
        String targetTableQName = this.getCtx().getRequest().getParameter("targetTableName");
        String fileName = "RejectedData" + System.currentTimeMillis() + ".txt";
        localTotalRows = this.getTotalRows(targetTableQName, executionId);
        
        try {
            MBeanUtil mBeanUtil = new MBeanUtil();
            ObjectName objName = mBeanUtil.getETLMBeanObjectName(this.getCtx());
            if (objName == null || localTotalRows == 0) {
                detailsTableContent = null;
            } else {
                MBeanServerConnection mBeanServer = mBeanUtil.getMBeanServer();
                
                this.getCtx().getResponse().setContentType("application/octet-stream");
                this.getCtx().getResponse().setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
                out = this.getCtx().getResponse().getOutputStream();
                
                while(moreRecordsToProcess) {
                    localOffset = (localCurrentPageNumber - 1) * localLimit;
	                if (executionId != null) {
	                    whereCondition = "executionId = " + executionId;
	                }
	                whereCondition = whereCondition + " LIMIT " + localLimit + " OFFSET " + localOffset;
	                if (localCurrentPageNumber == 1) {
	                    flag = "FirstTime";
	                } else {
	                    flag = "";
	                }
	                
	                Object[] params = new Object[] { targetTableQName, whereCondition, flag};
	                String[] paramTypes = new String[] { "java.lang.String", "java.lang.String", "java.lang.String" };
	
	                returnMap = (Map) mBeanServer.invoke(objName, ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT, params, paramTypes);
	                
	                exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
	                if (exceptionStr == null) {
	                    detailsTableContent = (String) returnMap.get(ETLMonitorServerConstants.GET_TABLE_CONTENT_RESULT);
	                } else {
	                    this.getCtx().getRequest().getSession().setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
	                    detailsTableContent = null;
	                }
	                
	                if (detailsTableContent == null || detailsTableContent.length() <= 0) {
	                    moreRecordsToProcess = false;
	                } else {
	                    buffData = detailsTableContent.getBytes();
	                    out.write(buffData);
	                    out.flush();
	                }
	                localCurrentPageNumber++;
	                
                }
                out.close();
            }
            
        } catch (Exception ex) {

            ex.printStackTrace();
            if (out != null) {
                out.close();
            }
        }

        if (localTotalRows > 0 && exceptionStr != null) {
            return ERROR;
        }
        return SUCCESS;
    }

    public String getDetailsTableContent() {
        return this.detailsTableContent;
    }
    
    private int getTotalRows(String targetTableQName, String executionId) {
        Integer totalRows = null;
        Map returnMap = null;
        
        int retRows = 0;
        try {
            MBeanUtil mBeanUtil = new MBeanUtil();
            ObjectName objName = mBeanUtil.getETLMBeanObjectName(this.getCtx());
            MBeanServerConnection mBeanServer = mBeanUtil.getMBeanServer();

            String whereCondition = ETLMonitorServerConstants.getWhereConditionWithExecutionId(executionId);
            
            Object[] params = new Object[] { targetTableQName, whereCondition };
            String[] paramTypes = new String[] { "java.lang.String", "java.lang.String" };

            //now in summaryPage set the total number of pages available
            returnMap = (Map) mBeanServer.invoke(objName, ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY,
                    params, paramTypes);
            
            String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
            if (exceptionStr == null) {
                totalRows = (Integer) returnMap.get(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY_RESULT);
            } else {
                
            }
            retRows = totalRows.intValue();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return retRows; 
    }
    
}
