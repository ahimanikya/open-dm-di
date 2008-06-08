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
 * @(#)ETLMonitorServerConstants.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * @author Venu Venkataraman
 * @version 
 */
public class ETLMonitorServerConstants {

    public static final String DELETE_RECORDS = "deleteRecords";
    public static final String TRUNCATE_RECORDS = "truncateRecords";
    public static final String TRUNCATE_RECORDS_DESC = "Truncate Summary and Detail tables";    
    public static final String DELETE_RECORDS_DESCRIPTION = "deleteRecords";
    public static final String DELETE_RECORDS_RESULT = "DeleteRecordsResult";
    public static final String ETL_MONITOR_DESCRIPTION = "ETL Monitor MBean";

    public static final String EXECEPTION = "Exception";

    public static final String EXECUTE_DETAIL_COUNT_QUERY = "executeDetailCountQuery";
    public static final String EXECUTE_DETAIL_COUNT_QUERY_DESCRIPTION = "executeDetailCountQuery";
    public static final String EXECUTE_DETAIL_COUNT_QUERY_RESULT = "DetailRowCount";

    public static final String EXECUTE_DETAIL_QUERY = "executeDetailQuery";
    public static final String EXECUTE_DETAIL_QUERY_DESCRIPTION = "executeDetailQuery";
    public static final String EXECUTE_DETAIL_QUERY_RESULT = "DetailQueryResult";

    public static final String EXECUTE_SUMMARY_COUNT_QUERY = "executeSummaryCountQuery";
    public static final String EXECUTE_SUMMARY_COUNT_QUERY_DESCRIPTION = "executeSummaryCountQuery";
    public static final String EXECUTE_SUMMARY_COUNT_QUERY_RESULT = "DetailRowCount";

    public static final String EXECUTE_SUMMARY_QUERY = "executeSummaryQuery";
    public static final String EXECUTE_SUMMARY_QUERY_DESCRIPTION = "executeSummaryQuery";
    public static final String EXECUTE_SUMMARY_QUERY_RESULT = "SummaryQueryResult";

    public static final String EXECUTE_SUMMARY_TOTAL_QUERY = "executeSummaryTotalQuery";
    public static final String EXECUTE_SUMMARY_TOTAL_QUERY_DESCRIPTION = "executeSummaryTotalQuery";
    public static final String EXECUTE_SUMMARY_TOTAL_QUERY_RESULT = "SummaryTotalQueryResult";

    public static final String FIRST_TIME = "FirstTime";

    public static final String GET_DETAILS_TABLE_CONTENT = "getDetailsTableContent";
    public static final String GET_DETAILS_TABLE_CONTENT_DESCRIPTION = "getDetailsTableContent";

    public static final String GET_PROPERTIES = "getProperties";
    public static final String GET_PROPERTIES_DESCRIPTION = "getProperties";

    public static final String GET_PURGE_INFO = "getPurgeInfo";
    public static final String GET_PURGE_INFO_DESCRIPTION = "getPurgeInfo";
    public static final String GET_PURGE_INFO_DETAIL_COUNT = "PurgeInfoDetailCount";
    public static final String GET_PURGE_INFO_SUMMARY_COUNT = "PurgeInfoSummaryCount";
    public static final String GET_PURGE_INFO_TARGET_TABLE_MAP = "PurgeInfoTargetTableMap";    

    public static final String GET_STATUS = "getStatus";
    public static final String GET_STATUS_DESCRIPTION = "getStatus";

    public static final String GET_TABLE_CONTENT = "getTableContent";
    public static final String GET_TABLE_CONTENT_DESCRIPTION = "getTableContent";
    public static final String GET_TABLE_CONTENT_RESULT = "TableContentResult";

    public static final String SUMMARY_ENDDATE_CNAME = "ENDDATE";
    public static final String SUMMARY_EXCEPTION_MSG = "EXCEPTION_MSG";

    public static final String SUMMARY_EXECUTIONID_CNAME = "EXECUTIONID";
    public static final String SUMMARY_EXTRACTED_CNAME = "EXTRACTED";
    public static final String SUMMARY_LOADED_CNAME = "LOADED";
    public static final String SUMMARY_REJECTED_CNAME = "REJECTED";
    public static final String SUMMARY_STARTDATE_CNAME = "STARTDATE";
    public static final String SUMMARY_TARGETTABLE_CNAME = "TARGETTABLE";
    
    public static final String IS_STARTABLE = "isStartable" ;
    public static final String IS_STARTABLE_DESC = "Executes isStartable()" ;    
    public static final String IS_STOPPABLE = "isStoppable" ;
    public static final String IS_STOPPABLE_DESC = "Executes isStoppable()" ;
    public static final String START = "start" ;
    public static final String START_DESC = "Executes start()" ;    
    public static final String STOP = "stop" ;
    public static final String STOP_DESC = "Executes stop()" ;

    public static final String[] SUMMARY_COLUMN_NAMES = { SUMMARY_EXECUTIONID_CNAME, SUMMARY_TARGETTABLE_CNAME, SUMMARY_STARTDATE_CNAME,
            SUMMARY_ENDDATE_CNAME, SUMMARY_EXTRACTED_CNAME, SUMMARY_LOADED_CNAME, SUMMARY_REJECTED_CNAME, SUMMARY_EXCEPTION_MSG};

    /**
     * Gets the entire exception stack.
     * 
     * @param throwable a throwable object
     * @return a string contains the throwable stack recursively
     */
    public static String getAllAsString(Throwable throwable) {
        String temp;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            getAllRecursively(throwable, ps);
            ps.flush();
            temp = baos.toString();
            ps.close();
            baos.close();
        } catch (Exception ex) {
            temp = throwable.getMessage() + " (Unable to print the stack from the original Exception)";
        }
        return temp;
    }

    public static String getPurgeInfoDetailWhereCondition(String summaryWhereCondition) {
        StringBuffer detailBuff = new StringBuffer();
        detailBuff.append(SUMMARY_EXECUTIONID_CNAME + " IN( ");
        detailBuff.append("SELECT " + SUMMARY_EXECUTIONID_CNAME + " FROM SUMMARY WHERE ");
        detailBuff.append(summaryWhereCondition);
        detailBuff.append(")");
        return detailBuff.toString();
    }

    public static String getPurgeInfoSummaryWhereCondition(String olderThanDate) {
        return getPurgeInfoSummaryWhereCondition(olderThanDate, null);
    }

    public static String getPurgeInfoSummaryWhereCondition(String olderThanDate, String filterByTgtTable) {
        StringBuffer summaryBuff = new StringBuffer();
        if (olderThanDate != null){
        	summaryBuff.append("(");
            summaryBuff.append(SUMMARY_STARTDATE_CNAME);
            summaryBuff.append(" < CHARTODATE('");
            summaryBuff.append(olderThanDate);
            summaryBuff.append("', 'MM/DD/YYYY') )");        	
        }
        
        if ((filterByTgtTable != null) && (!"".equals(filterByTgtTable.trim()))) {
            summaryBuff.append(" AND (");
            summaryBuff.append(SUMMARY_TARGETTABLE_CNAME);
            summaryBuff.append(" = '");
            summaryBuff.append(filterByTgtTable);
            summaryBuff.append("')");
        }
        return summaryBuff.toString();
    }

    public static String getWhereConditionWithExecutionId(String anExecutionId) {
        if (anExecutionId != null) {
            String whereCondition = SUMMARY_EXECUTIONID_CNAME + " = " + anExecutionId;
            return whereCondition;
        } else {
            return null;
        }
    }

    public static String getWhereConditionWithStartAndEndDate(String aStartDate, String anEndDate) {
        String startDate = null;
        String endDate = null;
        String whereClause = null;
        StringBuffer buff = new StringBuffer();

        if (aStartDate != null && aStartDate.length() > 0) {
            startDate = aStartDate;
        }
        if (anEndDate != null && anEndDate.length() > 0) {
            endDate = anEndDate;
        }

        if (startDate != null && endDate == null) {
            buff.append(SUMMARY_STARTDATE_CNAME + " >= CHARTODATE('");
            buff.append(startDate);
            buff.append("', 'MM/DD/YYYY')");
            whereClause = buff.toString();
        } else if (startDate != null && endDate != null) {
            buff.append(SUMMARY_STARTDATE_CNAME + " >= CHARTODATE('");
            buff.append(startDate);
            buff.append("', 'MM/DD/YYYY') and ");
            buff.append(SUMMARY_ENDDATE_CNAME + " <= DATEADD(DAY, 1, CHARTODATE('");
            buff.append(endDate);
            buff.append("', 'MM/DD/YYYY'))");
            whereClause = buff.toString();
        } else if (startDate == null && endDate != null) {
            buff.append(SUMMARY_ENDDATE_CNAME + " <= DATEADD(DAY, 1, CHARTODATE('");
            buff.append(endDate);
            buff.append("', 'MM/DD/YYYY'))");
            whereClause = buff.toString();
        }
        return whereClause;
    }

    /**
     * Retrieves recursively the stack trace of a throwable as a string.
     *
     * @param throwable the originating Throwable.
     * @param ps print stream object for output
     */
    private static void getAllRecursively(Throwable throwable, PrintStream ps) {
        ps.println("Message:");
        ps.println(throwable.getMessage());
        ps.println("Stack:");
        throwable.printStackTrace(ps);
        if (throwable.getCause() != null) {
            getAllRecursively(throwable.getCause(), ps);
        }
    }
}
