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
 * @(#)PurgeController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.ObjectName;
import javax.servlet.http.HttpServletRequest;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;
import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;


/**
 * A Summary Controller for the summary page. On success, transfers to the page which renders
 * summary table content.
 *
 * @author Ritesh Adval
 * @author Girish Patil
 * @version   
 */
public class PurgeController extends BaseController{
	private static final Logger LOGGER = Logger.getLogger(PurgeController.class.getName());
	
    private static final String DELETE_OK = "deleteOk";
    private static final String DELETE_FAILED = "deleteFailed";
    private static final String SUMMARY_TABLE_NAME = "SUMMARY";
    private static final String JAVA_LANG_STRING = "java.lang.String";
    private static final String PURGE_FILE_EXTENSION = ".txt";
    private static final String PURGE_FILE_PREFIX = "PurgeData";

    // Request Param constants
    private static final String RQ_PARAM_FILTER_BY_TGT_TABLE = "filterByTgtTable";
    private static final String RQ_PARAM_DELETE_BUTTON = "deleteButton";
    private static final String RQ_PARAM_SAVE_BUTTON = "saveButton";
    private static final String RQ_PARAM_PURGE_BUTTON = "purgeButton";
    private static final String RQ_PARAM_OLDER_THAN_DATE = "OlderThanDate";
    private static final String RQ_PARAM_PURGE_ALL = "purgeAll";

    // Request Attributes
    public static final String RQ_ATTR_TGT_TABLE_SET = "targetTableList" ;
    private static final String RQ_ATTR_DETAIL_TOTAL_ROWS = "DetailTotalRows";
    private static final String RQ_ATTR_SUMMARY_TOTAL_ROWS = "SummaryTotalRows";

    private Map toPurgeInfo(String olderThanDate) {
        return toPurgeInfo(olderThanDate, null);
    }
    
    private Map toPurgeInfo(String olderThanDate, String filterByTgtTable) {
        Map returnMap = null;
        Map targetTableMap = null;
        String exceptionStr = null;
        Integer tempSummaryTotalRows = new Integer(0);
        Integer tempDetailTotalRows = new Integer(0);
        HttpServletRequest request =  this.getCtx().getRequest();
        String component = getComponentName();

        // Set transform params which will be used in xsl to replace param values
        this.getCtx().setTransformParam(MBeanUtil.ATTR_COMPONENT, component);

        try {

            // Get the Purge info
            Object[] purgeInfoParams = new Object[] { olderThanDate,
                                                      filterByTgtTable };

            String[] purgeInfoParamTypes = new String[] { JAVA_LANG_STRING,
                                                          JAVA_LANG_STRING};
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.GET_PURGE_INFO,
                                                 purgeInfoParams,
                                                 purgeInfoParamTypes);

            exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

            if (exceptionStr == null) {
                tempSummaryTotalRows = (Integer) returnMap.get(
                                            ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT);
                tempDetailTotalRows = (Integer) returnMap.get(
                                            ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT);

                if (tempSummaryTotalRows != null) {
                    request.setAttribute(RQ_ATTR_SUMMARY_TOTAL_ROWS,
                                         tempSummaryTotalRows.toString());
                }

                if (tempDetailTotalRows != null) {
                    request.setAttribute(RQ_ATTR_DETAIL_TOTAL_ROWS,
                                         tempDetailTotalRows.toString());
                }
                request.setAttribute(RQ_PARAM_OLDER_THAN_DATE, olderThanDate);

                targetTableMap = (Map) returnMap.get(
                                      ETLMonitorServerConstants.GET_PURGE_INFO_TARGET_TABLE_MAP);

                if ((targetTableMap != null) && (targetTableMap.size() > 0)) {
                    Set keySet = targetTableMap.keySet();
                    request.setAttribute(RQ_ATTR_TGT_TABLE_SET,keySet);
                }else {
                    request.setAttribute(RQ_ATTR_TGT_TABLE_SET, Collections.EMPTY_SET);
                }

                request.setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
           } else {
                request.setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
           }

        } catch (Exception ex) {
        	LOGGER.log(Level.WARNING, "toPurgeInfo():", ex);
        }
        return returnMap;
    }


    private void toSaveAction(String olderThanDate, String filterByTgtTable) {
        Map targetTableMap = null;
        String targetTableName = null;
        String exceptionStr = null;
        Map purgeInfoMap = null;
        String tempHeader = null;
        OutputStream out = null;
        String fileName = PURGE_FILE_PREFIX + System.currentTimeMillis() + PURGE_FILE_EXTENSION;
        Integer tempSummaryTotalRows = new Integer(0);
        Integer tempDetailTotalRows = new Integer(0);
        int summaryTotalRows = 0;
        int detailTotalRows = 0;
        String summaryWhereCondition = null;
        String detailWhereCondition = null;
        HttpServletRequest request =  this.getCtx().getRequest();
        String component = (String) request.getSession().getAttribute(MBeanUtil.ATTR_COMPONENT);

        // Set transform params which will be used in xsl to replace param values
        this.getCtx().setTransformParam(MBeanUtil.ATTR_COMPONENT, component);
        ETLServiceInfo serviceInfo = (ETLServiceInfo) request.getSession().getAttribute(component);

        try {
        	if (olderThanDate != null){
        		if ("all".equalsIgnoreCase(filterByTgtTable)) {
        			summaryWhereCondition = ETLMonitorServerConstants.getPurgeInfoSummaryWhereCondition(
                                                                                     olderThanDate);
        		}else {
        			summaryWhereCondition = ETLMonitorServerConstants.getPurgeInfoSummaryWhereCondition(
                                                                   olderThanDate, filterByTgtTable);
        		}        		
        		detailWhereCondition = ETLMonitorServerConstants.getPurgeInfoDetailWhereCondition(summaryWhereCondition);
            }

        	purgeInfoMap = this.toPurgeInfo(olderThanDate, filterByTgtTable);
            exceptionStr = (String) purgeInfoMap.get(ETLMonitorServerConstants.EXECEPTION);

            if (exceptionStr == null) {
                // Get the Summary Row count
                tempSummaryTotalRows = (Integer) purgeInfoMap.get(
                                            ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT);
                if (tempSummaryTotalRows != null) {
                    summaryTotalRows = tempSummaryTotalRows.intValue();
                }

                tempDetailTotalRows = (Integer) purgeInfoMap.get(
                                            ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT);

                if (tempDetailTotalRows != null) {
                    detailTotalRows = tempDetailTotalRows.intValue();
                }

                if (summaryTotalRows > 0 || detailTotalRows > 0) {

                    this.getCtx().getResponse().setContentType("application/octet-stream");
                    this.getCtx().getResponse().setHeader("Content-Disposition",
                                                    "attachment; filename=\"" + fileName + "\"");
                    out = this.getCtx().getResponse().getOutputStream();

                    // When user wants to save all the Table details.
                    if ((filterByTgtTable != null) && (filterByTgtTable.equalsIgnoreCase("all"))) {
                        tempHeader = "Detail Records of " + SUMMARY_TABLE_NAME + " table\n";
                        out.write(tempHeader.getBytes());
                        out.flush();
                        this.getDetailData(summaryTotalRows, out,
                                            summaryWhereCondition, SUMMARY_TABLE_NAME,
                                            ETLMonitorServerConstants.GET_TABLE_CONTENT,
                                            serviceInfo.getExportPageSize());

                        targetTableMap = (Map) purgeInfoMap.get(ETLMonitorServerConstants.
                                                                  GET_PURGE_INFO_TARGET_TABLE_MAP);

                        if (targetTableMap != null) {
                            Iterator iterator = targetTableMap.keySet().iterator();
                            if (iterator != null) {
                                while(iterator.hasNext()) {
                                    targetTableName = (String) iterator.next();
                                    tempHeader = "\n\nDetail Records of " + targetTableName +
                                                 " table\n";
                                    out.write(tempHeader.getBytes());
                                    out.flush();
                                    this.getDetailData(detailTotalRows, out,
                                                detailWhereCondition, targetTableName,
                                                ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT,
                                                serviceInfo.getExportPageSize());
                                }
                                out.close();
                            }
                        }
                    } else {
                        // User wants to save just one table details, no summary records.
                        tempHeader = "Detail Records of " + filterByTgtTable + " table\n";
                        out.write(tempHeader.getBytes());
                        out.flush();
                        this.getDetailData( detailTotalRows, out,
                                           detailWhereCondition, filterByTgtTable,
                                           ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT,
                                           serviceInfo.getExportPageSize());
                    }
                }
                request.setAttribute(RQ_ATTR_SUMMARY_TOTAL_ROWS,
                                     tempSummaryTotalRows.toString());
                request.setAttribute(RQ_ATTR_DETAIL_TOTAL_ROWS,
                                     tempDetailTotalRows.toString());
                request.setAttribute(RQ_PARAM_OLDER_THAN_DATE, olderThanDate);
            }
        } catch (Exception ex) {
        	LOGGER.log(Level.WARNING, "toSaveAction():", ex);
            if (out != null) {
                try {
                    out.close();
                } catch (Exception ex1) {
                	LOGGER.log(Level.FINER, "toSaveAction():Closing Stream:", ex);
                }
            }
        }

    }

    private void getDetailData(int totalRows, OutputStream out, String inputWhereCondition,
                               String targetTableQName, String methodName, int anExportPageSize)
                                                                            throws Exception {
        Map returnMap = null;
        int localCurrentPageNumber = 1;
        int localLimit = anExportPageSize;
        int localOffset = 0;
        boolean moreRecordsToProcess = true;
        String flag = null;
        byte[] buffData = null;
        String tableContent = null;
        String whereCondition = null;

        while(moreRecordsToProcess) {
            localOffset = (localCurrentPageNumber - 1) * localLimit;
            if (inputWhereCondition == null){
            	inputWhereCondition = " (1=1) " ;
            }
            
        	whereCondition = inputWhereCondition + " LIMIT " + localLimit + " OFFSET " + localOffset;
            
            if (localCurrentPageNumber == 1) {
                flag = ETLMonitorServerConstants.FIRST_TIME;
            } else {
                flag = "";
            }
            Object[] params = new Object[] { targetTableQName, whereCondition, flag};
            String[] paramTypes = new String[] { JAVA_LANG_STRING,
                                                 JAVA_LANG_STRING,
                                                 JAVA_LANG_STRING };
            returnMap = (Map) invokeMonitorMethod(methodName, params, paramTypes);            
            tableContent = (String) returnMap.get(ETLMonitorServerConstants.GET_TABLE_CONTENT_RESULT);
            if (tableContent == null || tableContent.length() <= 0) {
                moreRecordsToProcess = false;
            } else {
                buffData = tableContent.getBytes();
                out.write(buffData);
                out.flush();
            }
            localCurrentPageNumber++;
        }
    }


    private void toDeleteAction(String olderThanDate) {
        Map returnMap = null;
        String exceptionStr = null;
        Integer tempSummaryTotalRows = new Integer(0);
        Integer tempDetailTotalRows = new Integer(0);
        HttpServletRequest request =  this.getCtx().getRequest();
        String component = (String) request.getSession().getAttribute(MBeanUtil.ATTR_COMPONENT);

        // Set transform params which will be used in xsl to replace param values
        this.getCtx().setTransformParam(MBeanUtil.ATTR_COMPONENT, component);

        try {
            //Get the Purge info
            Object[] deleteParams = new Object[] { olderThanDate };
            String[] deleteParamTypes = new String[] { JAVA_LANG_STRING };
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.DELETE_RECORDS,
                                                 deleteParams,
                                                 deleteParamTypes);

            exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
            if (exceptionStr == null) {
                tempSummaryTotalRows = (Integer) returnMap.get(ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT);
                tempDetailTotalRows = (Integer) returnMap.get(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT);
                if (tempSummaryTotalRows != null)
                    request.setAttribute(RQ_ATTR_SUMMARY_TOTAL_ROWS, tempSummaryTotalRows.toString());
                if (tempDetailTotalRows != null)
                    request.setAttribute(RQ_ATTR_DETAIL_TOTAL_ROWS, tempDetailTotalRows.toString());
                request.setAttribute(RQ_PARAM_OLDER_THAN_DATE, olderThanDate);
            } else {
                request.setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
            }

        } catch (Exception ex) {
        	LOGGER.log(Level.WARNING, "toDeleteAction():", ex);
        }
    }

    private void toTruncate() {
        Map returnMap = null;
        String exceptionStr = null;
        HttpServletRequest request =  this.getCtx().getRequest();
        String component = (String) request.getSession().getAttribute(MBeanUtil.ATTR_COMPONENT);
        Integer tempSummaryTotalRows = new Integer(0) ;
        Integer tempDetailTotalRows = tempSummaryTotalRows ;
        
        // Set transform parameter which will be used in XSL to replace parameter values
        this.getCtx().setTransformParam(MBeanUtil.ATTR_COMPONENT, component);

        try {
            // Truncate Detail and Summary rows
            returnMap = (Map) invokeMonitorMethod(ETLMonitorServerConstants.TRUNCATE_RECORDS,
                                                 null,
                                                 null);

            exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
            if (exceptionStr == null) {
                tempSummaryTotalRows = (Integer) returnMap.get(ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT);
                tempDetailTotalRows = (Integer) returnMap.get(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT);
                if (tempSummaryTotalRows != null) {
                	request.setAttribute(RQ_ATTR_SUMMARY_TOTAL_ROWS,  tempSummaryTotalRows.toString());
                }
                    
                if (tempDetailTotalRows != null){
                		request.setAttribute(RQ_ATTR_DETAIL_TOTAL_ROWS, tempDetailTotalRows.toString());
                }
            }else{
                request.setAttribute(ETLMonitorServerConstants.EXECEPTION, exceptionStr);            	
            }

        } catch (Exception ex) {
        	LOGGER.log(Level.WARNING, "toTruncate():", ex);
        }
    }
    
    protected String perform() throws Exception {
        String ret = SUCCESS;

        HttpServletRequest request =  this.getCtx().getRequest();
        String olderThanDate = request.getParameter(RQ_PARAM_OLDER_THAN_DATE);
        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
        String purgeAction = request.getParameter(RQ_PARAM_PURGE_BUTTON);
        String purgeAll = request.getParameter(RQ_PARAM_PURGE_ALL);
        String saveAction = request.getParameter(RQ_PARAM_SAVE_BUTTON);
        String deleteAction = request.getParameter(RQ_PARAM_DELETE_BUTTON);
        String filterByTgtTable = request.getParameter(RQ_PARAM_FILTER_BY_TGT_TABLE);

        if (objName == null) {
            ret =  ERROR;
        } else {
        	if (saveAction != null && saveAction.length() > 0) {
            	if ("yes".equals(purgeAll)){
                    this.toSaveAction(null, filterByTgtTable);
            	}else{
                    this.toSaveAction(olderThanDate, filterByTgtTable);            		
            	}
            } else if (deleteAction != null && deleteAction.length() > 0) {
            	if ("yes".equals(purgeAll)){
            		this.toTruncate();
            	}else{
                    this.toDeleteAction(olderThanDate);            		
            	}

                if (request.getAttribute(ETLMonitorServerConstants.EXECEPTION) != null) {
                    ret = DELETE_FAILED;
                }else {
                    ret = DELETE_OK;
                }
            }else if (purgeAction != null && purgeAction.length() > 0) {
            	if ("yes".equals(purgeAll)){
            		this.toPurgeInfo(null);            		
            	}else{
            		this.toPurgeInfo(olderThanDate);
            	}
            	            		
            } 
        }
        
        if ("yes".equals(purgeAll)){
        	request.setAttribute(RQ_PARAM_PURGE_ALL, "yes");        	
        }
        
        return ret;
    }    
}
