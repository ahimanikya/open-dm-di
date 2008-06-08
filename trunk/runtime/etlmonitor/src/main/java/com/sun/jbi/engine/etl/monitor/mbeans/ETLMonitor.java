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
 * @(#)ETLMonitor.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;

import org.axiondb.service.AxionDBQuery;
import org.axiondb.service.AxionDBQueryImpl;


/**
 * @author Ritesh Adval
 * @author Girish Patil
 * @version 
 */
public class ETLMonitor implements ETLMonitorMBean, Serializable {
    public  static final String LOG_CATEGORY = ETLMonitor.class.getName();
    private static final String SQL_TABLE_EXITS_1 = "SELECT COUNT(*) FROM AXION_TABLES WHERE UPPER(TABLE_NAME) = '";
    private static final String SQL_TABLE_EXITS_2 = "'";
    private static final boolean FORCE_START_STOP = true; 

    private static MBeanOperationInfo[] mBeanOperationInfos = new MBeanOperationInfo[18];
    private static transient final Logger mLogger = Logger.getLogger(ETLMonitor.class.getName());
    private static transient final Localizer mLoc = Localizer.get();
    private static java.util.logging.Logger sContextEnter = java.util.logging.Logger.getLogger("com.stc.EnterContext");
    private static java.util.logging.Logger sContextExit = java.util.logging.Logger.getLogger("com.stc.ExitContext");
    
    private static final Map VALID_STATUS_MAP = new HashMap();
    static {
    	VALID_STATUS_MAP.put(ETLMonitorMBean.STATUS_UP.toUpperCase(), ETLMonitorMBean.STATUS_UP);
    	VALID_STATUS_MAP.put(ETLMonitorMBean.STATUS_DOWN.toUpperCase(), ETLMonitorMBean.STATUS_DOWN);
    	VALID_STATUS_MAP.put(ETLMonitorMBean.STATUS_NOT_DEPLOYED.toUpperCase(), ETLMonitorMBean.STATUS_NOT_DEPLOYED);
    	VALID_STATUS_MAP.put(ETLMonitorMBean.STATUS_UNKNOWN.toUpperCase(), ETLMonitorMBean.STATUS_UNKNOWN);
    }
    
    protected MBeanAttributeInfo[] mBeanAttributeInfos = null;
    protected MBeanConstructorInfo[] mBeanConstructorInfos = null;
    protected MBeanInfo mBeanInfo = null;
    protected MBeanNotificationInfo[] mBeanNotificationInfos = null;
    protected Properties properties = new Properties();
    
    private AxionDBQuery mAxionQuery;
    private ETLMBeanConfig mETLMbeanConfig;
    private String mLoggingContextName = null;

    private MBeanServer mMBeanServer = null;
    private List mConnectorMBeanObjectNames = null;
    
    /**
     * Monitor bean constructor using MBeanConfig.
     * @param mbeanConfig
     * @throws Exception
     */
    public ETLMonitor(ETLMBeanConfig mbeanConfig) throws Exception {
        this.initialize();
        this.mETLMbeanConfig = mbeanConfig;
        this.mAxionQuery = new AxionDBQueryImpl();
        this.mLoggingContextName = mETLMbeanConfig.getProjectName() + "/" + mETLMbeanConfig.getDeploymentName();
        if (mbeanConfig.isInboundConnectorStartStoppable()){
        	this.mConnectorMBeanObjectNames = new ArrayList();
        	List connectorMBeanNameStrs = mbeanConfig.getInboundConnectorsNameStrs();
        	Iterator itr = connectorMBeanNameStrs.iterator();
        	String connectorMBeanName = null;
        	while(itr.hasNext()){
        		connectorMBeanName = (String) itr.next();
        		this.mConnectorMBeanObjectNames.add( new ObjectName(connectorMBeanName));
        	}
        }
    }


    // *****
    // ***** DynamicMBean methods - Start *****
    // *****
    /**
     * Returns value object corresponding to attribute name.
     */
    public Object getAttribute(String aName) throws AttributeNotFoundException, MBeanException, ReflectionException {
        return this.properties.getProperty(aName);
    }

    /**
     * Retrieves the value of specified attributes of the Dynamic MBean
     *
     * @param aNames aNames of the attributes
     * @return AttributeList list of attribute aNames and values
     */
    public AttributeList getAttributes(String[] aNames) {
        AttributeList attributes = new AttributeList(aNames.length);
        Attribute att = null;
        if (aNames != null){
        	for (int i=0; i < aNames.length; i++){
        		att = new Attribute(aNames[i], properties.getProperty(aNames[i]));
        		attributes.add(att);
        	}
        }
        return attributes;
    }

    public void setAttribute(Attribute aAttribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException,
            ReflectionException {
    	Object val = aAttribute.getValue();
    	// Support only String values 
    	if ((val != null) && (val instanceof String)){
    		properties.setProperty(aAttribute.getName(), (String)val);
    	}
    }

    /**
     * Sets the value of specified aAttributes of the Dynamic MBean.
     *
     * @param aAttributes list of attribute names and values.
     * @return AttributeList list of attribute names and values
     */
    public AttributeList setAttributes(AttributeList aAttributes) {
        for (Iterator it = aAttributes.iterator(); it.hasNext();) {
            Attribute attribute = (Attribute) it.next();
            try {
                setAttribute(attribute);
            } catch (Exception e) {
                continue;
            }
        }
        return aAttributes;
    }
    
    private Class[] getSignature(String[] strSignature) throws ClassNotFoundException{
    	Class[] ret = null;
    	String argClassName = null;
    	Class argClass = null;
    	if (strSignature != null){
    		ret = new Class[strSignature.length];
    		for (int i=0; i < strSignature.length; i++){
    			argClassName = strSignature[i];
    			argClass = Class.forName(argClassName);
    			ret[i] = argClass;
    		}
    	}
    	
    	return ret;
    }
    
    public Object invoke(String operationName, Object[] params, String[] signature) throws MBeanException, ReflectionException {

        if (operationName == null || operationName.equals("")) {
            throw new RuntimeOperationsException(new IllegalArgumentException("MBean.invoke operation name is " + " null "));
        }
        AttributeList resultList = new AttributeList();
        if (operationName.equals(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY)) {

            if (params.length == 3) {
                String whereCondition = (String) params[0];
                Integer limit = (Integer) params[1];
                Integer offset = (Integer) params[2];
                return this.executeSummaryQuery(whereCondition, limit, offset);
            } else if (params.length == 5) {
                String whereCondition = (String) params[0];
                String groupBy = (String) params[1];
                String orderBy = (String) params[2];
                Integer limit = (Integer) params[3];
                Integer offset = (Integer) params[4];
                return this.executeSummaryQuery(whereCondition, groupBy, orderBy, limit, offset);
            }

        } else if (operationName.equals(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY)) {

            if (params.length == 4) {
                String tableQualifiedName = (String) params[0];
                String whereCondition = (String) params[1];
                Integer limit = (Integer) params[2];
                Integer offset = (Integer) params[3];
                return this.executeDetailQuery(tableQualifiedName, whereCondition, limit, offset);
            } else if (params.length == 6) {
                String tableQualifiedName = (String) params[0];
                String whereCondition = (String) params[1];
                String groupBy = (String) params[2];
                String orderBy = (String) params[3];
                Integer limit = (Integer) params[4];
                Integer offset = (Integer) params[5];
                return this.executeDetailQuery(tableQualifiedName, whereCondition, groupBy, orderBy, limit, offset);
            }

        } else if (operationName.equals(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY)) {

            String whereCondition = (String) params[0];
            return this.executeSummaryCountQuery(whereCondition);

        } else if (operationName.equals(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY)) {

            String tableQualifiedName = (String) params[0];
            String whereCondition = (String) params[1];
            return this.executeDetailCountQuery(tableQualifiedName, whereCondition);

        } else if (operationName.equals(ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY)) {

            String whereCondition = (String) params[0];
            return this.executeSummaryTotalQuery(whereCondition);

        } else if (operationName.equals(ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT)) {

            String tableQualifiedName = (String) params[0];
            String whereCondition = (String) params[1];
            String aFlag = (String) params[2];
            return this.getDetailsTableContent(tableQualifiedName, whereCondition, aFlag);

        } else if (operationName.equals(ETLMonitorServerConstants.GET_TABLE_CONTENT)) {

            String tableQualifiedName = (String) params[0];
            String whereCondition = (String) params[1];
            String aFlag = (String) params[2];
            return this.getTableContent(tableQualifiedName, whereCondition, aFlag);

        } else if (operationName.equals(ETLMonitorServerConstants.GET_PURGE_INFO)) {

            String olderThanDate = (String) params[0];
            return this.getPurgeInfo(olderThanDate);

        } else if (operationName.equals(ETLMonitorServerConstants.DELETE_RECORDS)) {

            String olderThanDate = (String) params[0];
            return this.deleteRecords(olderThanDate);

        } else if (operationName.equals(ETLMonitorServerConstants.TRUNCATE_RECORDS)) {        	
        	
            return this.truncateRecords();
            
        } else if (operationName.equals(ETLMonitorServerConstants.GET_STATUS)) {

            return this.getStatus();

        } else if (operationName.equals(ETLMonitorServerConstants.GET_PROPERTIES)) {

            return this.getProperties();

        } else {
            String errMsg = null;
            boolean retException = false;
            Object retObj = null;
        	try {
	        	Class[] methodSignature = getSignature(signature);
	        	Method method = this.getClass().getMethod(operationName, methodSignature);
	        	retObj = method.invoke(this, params);
        	} catch (ClassNotFoundException ex){
        		retException = true;
        		errMsg = "Can not invoke method '" + operationName+ "', one of the arguments class not found:" + ex.getMessage();
        	} catch (NoSuchMethodException ex){
        		retException = true;
        		errMsg = "Can not invoke method '" + operationName+ "', method not found:" + ex.getMessage();        		
        	} catch (IllegalAccessException ex){
        		retException = true;
        		errMsg = "Can not invoke method '" + operationName+ "', access violation:" + ex.getMessage();        		        		
        	} catch (InvocationTargetException ex){
        		retException = true;
        		errMsg = "Can not invoke method '" + operationName+ "', access violation:" + ex.getMessage();        		        		
        	}

        	if (retException){
            	Map ret = new HashMap();
                ret.put(ETLMonitorServerConstants.EXECEPTION, errMsg);
                return ret;            	
        	}else{
        		return retObj;
        	}

        }
        return resultList;
    }

    public MBeanInfo getMBeanInfo() {
        return this.mBeanInfo;
    }
    
    // *****
    // ***** DynamicMBean methods - End *****
    // *****
    
    
    
    /**
     * Indicates whether the given String represents a writable directory.
     *
     * @param testFiler File object representing path to test
     * @return true if pathToDir is a writable directory, false otherwise.
     */
    public static boolean isValidAndWritableDirectory(File testFile) {
        return (testFile.isDirectory() && testFile.canWrite());
    }

    /**
     * Indicates whether the given String represents a writable directory.
     *
     * @param pathToDir String representation of path to test
     * @return true if pathToDir is a writable directory, false otherwise.
     */
    public static boolean isValidAndWritableDirectory(String pathToDir) {
		return false;
       // return (StringUtil.isNullString(pathToDir)) ? false : isValidAndWritableDirectory(new File(pathToDir));
    }

    /**
     * Resolves location of monitor database metadata in local filesystem
     *
     * @return String representing full path to monitor database metadata directory.
     */
    public static String resolveDbLocation(ETLMBeanConfig mbeanConfig) {
        return mbeanConfig.getLogFolder();
    }    

    
    private boolean tableExists(String tableName) throws Exception{
        boolean ret = false;
        StringBuffer sb = new StringBuffer();
        sb.append(SQL_TABLE_EXITS_1);
        sb.append(tableName.trim().toUpperCase());
        sb.append(SQL_TABLE_EXITS_2);
        ResultSet rs = executeResultSetQuery(sb.toString());
        if (rs.next()) {
            int count = rs.getInt(1);
            if (count > 0) {
                ret = true;
            }
        }
        return ret;
    }

    public Map deleteRecords(String anOlderThanDate) {
        Map ret = new HashMap();
        String exceptionStr = null;
        Integer summaryTotalRows = new Integer(0);
        List targetTableNameList = null;
        Map targetTableMap = new HashMap();
        int grandTotal = 0;
        String targetTableName = null;

        try {
            // Set logging context for this request.
            if (mLoggingContextName != null) {
                mLogger.log(Level.FINE, "entering deleteRecords");
            }
            //Get the summary counts
            String summaryWhereCondition = ETLMonitorServerConstants.getPurgeInfoSummaryWhereCondition(anOlderThanDate);
            Map summaryCountMap = this.executeSummaryCountQuery(summaryWhereCondition);
            exceptionStr = (String) summaryCountMap.get(ETLMonitorServerConstants.EXECEPTION);

            if (exceptionStr == null) {
                summaryTotalRows = (Integer) summaryCountMap.get(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY_RESULT);
                if (summaryTotalRows.intValue() > 0) {
                    String detailWhereCondition = ETLMonitorServerConstants.getPurgeInfoDetailWhereCondition(summaryWhereCondition);

                    targetTableNameList = this.getTargetTableNames(summaryWhereCondition);
                    if (targetTableNameList != null && targetTableNameList.size() > 0) {
                        for (int ii = 0; ii < targetTableNameList.size(); ii++) {
                            targetTableName = (String) targetTableNameList.get(ii);
                            String detailsQTableName = MonitorUtil.getDetailsTableName(targetTableName);
                            if (tableExists(detailsQTableName)) {
                                String detailStatement = "DELETE " + detailsQTableName + " WHERE " + detailWhereCondition;
                                int tempDetailRows = this.executeUpdateStatement(detailStatement);
                                if (tempDetailRows > 0) {
                                    targetTableMap.put(targetTableName, new Integer(tempDetailRows));
                                    grandTotal = grandTotal + tempDetailRows;
                                }
                            }
                        }
                    }
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT, new Integer(grandTotal));
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_TARGET_TABLE_MAP, targetTableMap);
                }
                String summaryStatement = "DELETE SUMMARY WHERE " + summaryWhereCondition;
                int tempSummaryRows = this.executeUpdateStatement(summaryStatement);
                if (tempSummaryRows > 0) {
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT, new Integer(tempSummaryRows));
                }
            } else {
                ret.put(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
            }
        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
            //Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Error while executing deleteRecords", ex);
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            if (mLoggingContextName != null) {
                mLogger.log(Level.FINE, "exiting deleteRecords");
            }
        }
        return ret;
    }

    public Map truncateRecords() {
        Map ret = new HashMap();
        List targetTableNameList = null;
        String targetTableName = null;
        String detailsQTableName = null;
        int totalDetailRows = 0;
        int summaryRows = 0;
        try {
            // Set logging context for this request.
            if (mLoggingContextName != null) {
                mLogger.log(Level.FINE, "entering truncateRecords");
            }
            
            targetTableNameList = this.getTargetTableNames(null);
            if (targetTableNameList != null && targetTableNameList.size() > 0) {
                for (int ii = 0; ii < targetTableNameList.size(); ii++) {
                    targetTableName = (String) targetTableNameList.get(ii);
                    detailsQTableName = MonitorUtil.getDetailsTableName(targetTableName);
                    if (tableExists(detailsQTableName)) {
                        String detailStatement = "TRUNCATE TABLE " + detailsQTableName ;
                        totalDetailRows += this.executeUpdateStatement(detailStatement);
                    }
                }
            }
            
            String summaryStatement = "TRUNCATE TABLE " + this.mETLMbeanConfig.getSummaryTableName();
            summaryRows += this.executeUpdateStatement(summaryStatement);
            
            ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT, new Integer(totalDetailRows));
            ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT, new Integer(summaryRows));
        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
            //Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Error while executing deleteRecords", ex);
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            if (mLoggingContextName != null) {
                mLogger.log(Level.FINE, "exiting truncateRecords");
            }
        }
        return ret;
    }

    public Map executeDetailCountQuery(String aTargetTableName, String aWhereCondition) {
        Map ret = new HashMap();
        int count = 0;

        try {
            // Set logging context for this request.
            if (mLoggingContextName != null) {
                mLogger.log(Level.FINE, "entering executeDetailCountQuery");
            }

            StringBuffer detailQuery = new StringBuffer("SELECT COUNT(*) FROM ");
            String detailsTableName = MonitorUtil.getDetailsTableName(aTargetTableName);
            detailQuery.append(detailsTableName);

            if (aWhereCondition != null) {
                detailQuery.append(" WHERE ");
                detailQuery.append(aWhereCondition);
            }

            ResultSet rset = executeResultSetQuery(detailQuery.toString());
            if (rset.next()) {
                count = rset.getInt(1);
            }
            ret.put(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY_RESULT, new Integer(count));

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
                mLogger.log(Level.FINE, "exiting executeDetailCountQuery");
        }

        return ret;
    }

    public Map executeDetailQuery(String aTargetTableName, String aWhereCondition, Integer aLimit, Integer anOffset) {
        return executeDetailQuery(aTargetTableName, aWhereCondition, null, null, aLimit, anOffset);
    }

    public Map executeDetailQuery(String aTargetTableName, String aWhereCondition, String aGroupBy, String anOrderBy, Integer aLimit, Integer anOffset) {
        Map ret = new HashMap();
        String result = null;

        try {
            // Set logging context for this request.
            if (mLoggingContextName != null) {
               mLogger.log(Level.FINE, "entering executeDetailQuery");
            }

            String detailsTableName = MonitorUtil.getDetailsTableName(aTargetTableName);
            StringBuffer detailQuery = new StringBuffer("SELECT * FROM ");
            detailQuery.append(detailsTableName);
            if (aWhereCondition != null) {
                detailQuery.append(" WHERE ");
                detailQuery.append(aWhereCondition);
            }

            if (aGroupBy != null) {
                detailQuery.append(" GROUP BY ");
                detailQuery.append(aGroupBy);
            }

            if (anOrderBy != null) {
                detailQuery.append(" ORDER BY ");
                detailQuery.append(anOrderBy);
            }

            if (aLimit != null) {
                detailQuery.append(" LIMIT ");
                detailQuery.append(aLimit.intValue());
            }

            if (anOffset != null) {
                detailQuery.append(" OFFSET ");
                detailQuery.append(anOffset.intValue());
            }

            result = executeQuery(detailQuery.toString());
            ret.put(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY_RESULT, result);

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            if (mLoggingContextName != null) {
                sContextExit.info(mLoggingContextName);
            }
        }

        return ret;
    }

    public Map executeSummaryCountQuery(String aWhereCondition) {
        Map ret = new HashMap();
        int count = 0;

        try {
            // 	Set logging context for this request.
            mLogger.log(Level.FINE, "entering executeSummaryCountQuery");

            StringBuffer summaryQuery = new StringBuffer("SELECT COUNT(*) FROM ");
            String summaryTableName = this.mETLMbeanConfig.getSummaryTableName();
            summaryQuery.append(summaryTableName);

            if (aWhereCondition != null) {
                summaryQuery.append(" WHERE ");
                summaryQuery.append(aWhereCondition);
            }

            ResultSet rset = executeResultSetQuery(summaryQuery.toString());

            if (rset != null && rset.next()) {
                count = rset.getInt(1);
            }
            ret.put(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY_RESULT, new Integer(count));

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            mLogger.log(Level.FINE, "exiting executeSummaryCountQuery");
        }

        return ret;
    }

    public Map executeSummaryQuery(String aWhereCondition, Integer aLimit, Integer anOffset) {
        return executeSummaryQuery(aWhereCondition, null, null, aLimit, anOffset);
    }

    public Map executeSummaryQuery(String aWhereCondition, String aGroupBy, String anOrderBy, Integer aLimit, Integer anOffset) {
        Map ret = new HashMap();

        try {
            // Set logging context for this request.
            mLogger.log(Level.FINE, "entering executeSummaryQuery");

            String summaryTableName = this.mETLMbeanConfig.getSummaryTableName();
            StringBuffer summaryQuery = new StringBuffer("SELECT ");
            for (int i = 0; i < ETLMonitorServerConstants.SUMMARY_COLUMN_NAMES.length; i++) {
                if (i != 0) {
                    summaryQuery.append(", ");
                }
                summaryQuery.append(ETLMonitorServerConstants.SUMMARY_COLUMN_NAMES[i]);
            }
            summaryQuery.append(" FROM ");

            summaryQuery.append(summaryTableName);

            if (aWhereCondition != null) {
                summaryQuery.append(" WHERE ");
                summaryQuery.append(aWhereCondition);
            }

            if (aGroupBy != null) {
                summaryQuery.append(" GROUP BY ");
                summaryQuery.append(aGroupBy);
            }

            if (anOrderBy != null) {
                summaryQuery.append(" ORDER BY ");
                summaryQuery.append(anOrderBy);
            } else {
                summaryQuery.append(" ORDER BY ");
                summaryQuery.append(ETLMonitorServerConstants.SUMMARY_EXECUTIONID_CNAME);
            }

            if (aLimit != null) {
                summaryQuery.append(" LIMIT ");
                summaryQuery.append(aLimit.intValue());
            }

            if (anOffset != null) {
                summaryQuery.append(" OFFSET ");
                summaryQuery.append(anOffset.intValue());
            }
            String result = executeQuery(summaryQuery.toString());
            ret.put(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY_RESULT, result);

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            mLogger.log(Level.FINE, "exiting executeSummaryQuery");
        }

        return ret;
    }

    public Map executeSummaryTotalQuery(String aWhereCondition) {
        Map ret = new HashMap();
        ResultSet resultSet = null;

        try {
            // Set logging context for this request.
            mLogger.log(Level.FINE, "entering executeSummaryTotalQuery");

            String summaryTableName = this.mETLMbeanConfig.getSummaryTableName();
            StringBuffer summaryQuery = new StringBuffer("SELECT ");

            summaryQuery.append("SUM(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_EXTRACTED_CNAME);
            summaryQuery.append(") AS SUM1, SUM(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_LOADED_CNAME);
            summaryQuery.append(") AS SUM2, SUM(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_REJECTED_CNAME);
            summaryQuery.append(") AS SUM3, AVG(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_EXTRACTED_CNAME);
            summaryQuery.append(") AS AVG1, AVG(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_LOADED_CNAME);
            summaryQuery.append(") AS AVG2, AVG(");
            summaryQuery.append(ETLMonitorServerConstants.SUMMARY_REJECTED_CNAME);
            summaryQuery.append(") AS AVG3");
            summaryQuery.append(" FROM ");
            summaryQuery.append(summaryTableName);

            if (aWhereCondition != null) {
                summaryQuery.append(" WHERE ");
                summaryQuery.append(aWhereCondition);
            }

            resultSet = executeResultSetQuery(summaryQuery.toString());

            ResultSetMetaData meta = resultSet.getMetaData();
            StringBuffer content = new StringBuffer(1000);

            int count = meta.getColumnCount();
            // find column values and widths
            while (resultSet.next()) {
                for (int i = 0; i < count; i++) {
                    String val = resultSet.getString(i + 1);
                    content.append(val);
                    if (i < count) {
                        content.append(",");
                    }
                }
            }
            ret.put(ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY_RESULT, content.toString());

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (Exception ex1) {

            }
            // Notify logging system to pop our context off the stack and publish it.
            mLogger.log(Level.FINE, "entering executeSummaryTotalQuery");
        }

        return ret;
    }


    public Map getDetailsTableContent(String aTargetTableName, String aWhereCondition, String aFlag) {
        String detailsTableName = MonitorUtil.getDetailsTableName(aTargetTableName);
        return this.getTableContent(detailsTableName, aWhereCondition, aFlag);
    }


    public Map getPurgeInfo(String anOlderThanDate) {
        return getPurgeInfo(anOlderThanDate, null);
    }

    public Map getPurgeInfo(String anOlderThanDate, String filterByTgtTable) {
        Map ret = new HashMap();
        String exceptionStr = null;
        Integer summaryTotalRows = new Integer(0);
        Integer detailTotalRows = new Integer(0);
        ArrayList targetTableNameList = null;
        Map targetTableMap = new TreeMap();
        int grandTotal = 0;
        String targetTableName = null;
        String summaryWhereCondition = null;
        String detailWhereCondition = null;
        try {
            // Set logging context for this request.
            mLogger.log(Level.FINE, "entering getPurgeInfo");
            // Get the summary counts
            if ((anOlderThanDate == null) && (filterByTgtTable == null)){
            	summaryWhereCondition = null;
            }else{
            	summaryWhereCondition = ETLMonitorServerConstants.getPurgeInfoSummaryWhereCondition(anOlderThanDate, filterByTgtTable);
            }
            
            Map summaryCountMap = this.executeSummaryCountQuery(summaryWhereCondition);
            exceptionStr = (String) summaryCountMap.get(ETLMonitorServerConstants.EXECEPTION);

            if (exceptionStr == null) {
                summaryTotalRows = (Integer) summaryCountMap.get(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY_RESULT);
                ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_SUMMARY_COUNT, summaryTotalRows);

                if (summaryTotalRows.intValue() > 0) {
                	if (summaryWhereCondition == null){
                		detailWhereCondition = null;                		
                	}else{
                		detailWhereCondition = ETLMonitorServerConstants.getPurgeInfoDetailWhereCondition(summaryWhereCondition);
                	}
                    
                    targetTableNameList = this.getTargetTableNames(summaryWhereCondition);

                    if (targetTableNameList != null && targetTableNameList.size() > 0) {
                        for (int ii = 0; ii < targetTableNameList.size(); ii++) {
                            targetTableName = (String) targetTableNameList.get(ii);
                            Map detailCountMap = this.executeDetailCountQuery(targetTableName, detailWhereCondition);
                            exceptionStr = (String) detailCountMap.get(ETLMonitorServerConstants.EXECEPTION);
                            if (exceptionStr == null) {
                                detailTotalRows = (Integer) detailCountMap.get(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY_RESULT);
                                if ((detailTotalRows != null) && (detailTotalRows.intValue() > 0)) {
                                    targetTableMap.put(targetTableName, detailTotalRows);
                                    grandTotal = grandTotal + detailTotalRows.intValue();
                                }
                            }
                        }
                    }
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT, new Integer(grandTotal));
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_TARGET_TABLE_MAP, targetTableMap);
                } else {
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_DETAIL_COUNT, new Integer(0));
                    ret.put(ETLMonitorServerConstants.GET_PURGE_INFO_TARGET_TABLE_MAP, null);
                }
            } else {
                ret.put(ETLMonitorServerConstants.EXECEPTION, exceptionStr);
            }
        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
            //Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Error while executing getPurgeInfo", ex);
        } finally {
            // Notify logging system to pop our context off the stack and publish it.
            mLogger.log(Level.FINE, "exiting getPurgeInfo");
        }
        return ret;
    }

    public Map getTableContent(String aTargetTableName, String aWhereCondition, String aFlag) {
        Map ret = new HashMap();
        ResultSet rset = null;
        try {
            // Set logging context for this request.
            mLogger.log(Level.FINE, "entering getTableContent");

            String detailsTableName = aTargetTableName;
            StringBuffer detailQuery = new StringBuffer("SELECT * FROM ");
            detailQuery.append(detailsTableName);
            if (aWhereCondition != null) {
                detailQuery.append(" WHERE ");
                detailQuery.append(aWhereCondition);
            }
            rset = executeResultSetQuery(detailQuery.toString());
            ret.put(ETLMonitorServerConstants.GET_TABLE_CONTENT_RESULT, createCSVContent(rset, aFlag));

        } catch (Exception ex) {
            ret.put(ETLMonitorServerConstants.EXECEPTION, ETLMonitorServerConstants.getAllAsString(ex));
        } finally {
            try {
                if (rset != null) {
                    rset.close();
                }
            } catch (Exception ex1) {

            }
            // Notify logging system to pop our context off the stack and publish it.
            mLogger.log(Level.FINE, "exiting getTableContent");
        }

        return ret;
    }

    public ArrayList getTargetTableNames(String aSummaryWhereCondition) throws Exception {
        ArrayList ret = new ArrayList();
        String tempTargetTable = null;

        StringBuffer summaryQuery = new StringBuffer("SELECT DISTINCT ");
        summaryQuery.append(ETLMonitorServerConstants.SUMMARY_TARGETTABLE_CNAME);
        summaryQuery.append(" FROM ");
        String summaryTableName = this.mETLMbeanConfig.getSummaryTableName();
        summaryQuery.append(summaryTableName);

        if (aSummaryWhereCondition != null) {
            summaryQuery.append(" WHERE ");
            summaryQuery.append(aSummaryWhereCondition);
        }
        ResultSet rset = executeResultSetQuery(summaryQuery.toString());
        while (rset.next()) {
            tempTargetTable = rset.getString(1);
            ret.add(tempTargetTable);
        }
        return ret;
    }


    private String createCSVContent(ResultSet rset, String aFlag) throws Exception {
        ResultSetMetaData meta = rset.getMetaData();
        StringBuffer contentBuffer = new StringBuffer(1000);
        boolean firstTime = true;

        int count = meta.getColumnCount();

        if (aFlag.equals(ETLMonitorServerConstants.FIRST_TIME)) {
            firstTime = true;
            for (int i = 0; i < count; i++) {
                if (!firstTime) {
                    contentBuffer.append(",");
                }
                String label = meta.getColumnLabel(i + 1);
                contentBuffer.append(label);
                firstTime = false;
            }
        }

        // find column values and widths
        while (rset.next()) {
            contentBuffer.append("\r\n");
            firstTime = true;
            for (int i = 0; i < count; i++) {
                if (!firstTime) {
                    contentBuffer.append(",");
                }
                String val = rset.getString(i + 1);
                if (rset.wasNull()) {
                    val = "NULL";
                }
                contentBuffer.append(val);
                firstTime = false;
            }
        }
        return contentBuffer.toString();
    }

    private String executeQuery(String query) throws Exception {
        String result = null;
        String dbName = this.mETLMbeanConfig.getCollabName();
        String dbLocation = ETLMonitor.resolveDbLocation(mETLMbeanConfig);

        result = mAxionQuery.executeQuery(dbName, dbLocation, query);

        return result;
    }

    private ResultSet executeResultSetQuery(String query) throws Exception {
        ResultSet result = null;
        String dbName = this.mETLMbeanConfig.getCollabName();
        String dbLocation = ETLMonitor.resolveDbLocation(mETLMbeanConfig);
        
        sContextEnter.info("dbName is " + dbName + "dbLocation is " + dbLocation);

        result = mAxionQuery.executeResultSetQuery(dbName, dbLocation, query);

        return result;
    }

    private int executeUpdateStatement(String aStatement) throws Exception {
        int result = -1;
        String dbName = this.mETLMbeanConfig.getCollabName();
        String dbLocation = ETLMonitor.resolveDbLocation(mETLMbeanConfig);

        result = mAxionQuery.executeUpdate(dbName, dbLocation, aStatement);

        return result;
    }

    //************************ DYNABMIC MBEAN METHODS START **************************
    /**
     * Build the protected MBeanInfo field, which represents the management interface
     * exposed by the MBean; i.e., the set of attributes, constructors, operations and
     * notifications which are available for management. A reference to the MBeanInfo
     * object is returned by the getMBeanInfo() method of the DynamicMBean interface. Note
     * that, once constructed, an MBeanInfo object is immutable.
     */
    private void initialize() throws Exception {

        try {
            MBeanParameterInfo[] executeSummaryQueryParams = { new MBeanParameterInfo("aWhereCondition", "String", "Where condition"),
                    new MBeanParameterInfo("aLimit", "Integer", "Limit"), new MBeanParameterInfo("anOffset", "Integer", "Offset")};
            mBeanOperationInfos[0] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY,
                ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY_DESCRIPTION, executeSummaryQueryParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeSummaryQueryParams1 = { new MBeanParameterInfo("aWhereCondition", "String", "Where condition"),
                    new MBeanParameterInfo("aGroupBy", "String", "Group By"), new MBeanParameterInfo("anOrderBy", "String", "Order By"),
                    new MBeanParameterInfo("aLimit", "Integer", "Limit"), new MBeanParameterInfo("anOffset", "Integer", "Offset")};
            mBeanOperationInfos[1] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY,
                ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY_DESCRIPTION, executeSummaryQueryParams1, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeSummaryTotalQueryParams = { new MBeanParameterInfo("aWhereCondition", "String", "Where condition")};
            mBeanOperationInfos[2] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY,
                ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY_DESCRIPTION, executeSummaryTotalQueryParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeSummaryCountQueryParams = { new MBeanParameterInfo("aWhereCondition", "String", "Where condition")};
            mBeanOperationInfos[3] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY,
                ETLMonitorServerConstants.EXECUTE_SUMMARY_COUNT_QUERY_DESCRIPTION, executeSummaryCountQueryParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeDetailQueryParams = { new MBeanParameterInfo("aTargetTableName", "String", "Target Table Name"),
                    new MBeanParameterInfo("aWhereCondition", "String", "Where condition"), new MBeanParameterInfo("aLimit", "Integer", "Limit"),
                    new MBeanParameterInfo("anOffset", "Integer", "Offset")};
            mBeanOperationInfos[4] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY,
                ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY_DESCRIPTION, executeDetailQueryParams, "java.util.Map", MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeDetailQueryParams1 = { new MBeanParameterInfo("aTargetTableName", "String", "Target Table Name"),
                    new MBeanParameterInfo("aWhereCondition", "String", "Where condition"), new MBeanParameterInfo("aGroupBy", "String", "Group By"),
                    new MBeanParameterInfo("anOrderBy", "String", "Order By"), new MBeanParameterInfo("aLimit", "Integer", "Limit"),
                    new MBeanParameterInfo("anOffset", "Integer", "Offset")};
            mBeanOperationInfos[5] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY,
                ETLMonitorServerConstants.EXECUTE_DETAIL_QUERY_DESCRIPTION, executeDetailQueryParams1, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] executeDetailCountQueryParams = { new MBeanParameterInfo("aTargetTableName", "String", "Table Qualifed Name"),
                    new MBeanParameterInfo("aWhereCondition", "String", "Where condition"),};
            mBeanOperationInfos[6] = new MBeanOperationInfo(ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY,
                ETLMonitorServerConstants.EXECUTE_DETAIL_COUNT_QUERY_DESCRIPTION, executeDetailCountQueryParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] getDetailsTableContentParams = { new MBeanParameterInfo("aTargetTableName", "String", "Table Qualifed Name"),
                    new MBeanParameterInfo("aWhereCondition", "String", "Where condition"),
                    new MBeanParameterInfo("aFlag", "String", "A Flag indicating the position")};
            mBeanOperationInfos[7] = new MBeanOperationInfo(ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT,
                ETLMonitorServerConstants.GET_DETAILS_TABLE_CONTENT_DESCRIPTION, getDetailsTableContentParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[8] = new MBeanOperationInfo(ETLMonitorServerConstants.GET_STATUS, ETLMonitorServerConstants.GET_STATUS_DESCRIPTION,
                null, "String", MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[9] = new MBeanOperationInfo(ETLMonitorServerConstants.GET_PROPERTIES,
                ETLMonitorServerConstants.GET_PROPERTIES_DESCRIPTION, null, "java.util.Map", MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[10] = new MBeanOperationInfo(ETLMonitorServerConstants.GET_TABLE_CONTENT,
                ETLMonitorServerConstants.GET_TABLE_CONTENT_DESCRIPTION, getDetailsTableContentParams, "java.util.Map",
                MBeanOperationInfo.ACTION_INFO);

            MBeanParameterInfo[] getPurgeInfoParams = { new MBeanParameterInfo("anOlderThanDate", "String", "Older than date")};
            mBeanOperationInfos[11] = new MBeanOperationInfo(ETLMonitorServerConstants.GET_PURGE_INFO,
                ETLMonitorServerConstants.GET_PURGE_INFO_DESCRIPTION, getPurgeInfoParams, "java.util.Map", MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[12] = new MBeanOperationInfo(ETLMonitorServerConstants.DELETE_RECORDS,
                ETLMonitorServerConstants.DELETE_RECORDS_DESCRIPTION, getPurgeInfoParams, "java.util.Map", MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[13] = new MBeanOperationInfo(ETLMonitorServerConstants.TRUNCATE_RECORDS,
                    ETLMonitorServerConstants.TRUNCATE_RECORDS_DESC, null, "java.util.Map",
                    MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[14] = new MBeanOperationInfo(ETLMonitorServerConstants.IS_STARTABLE,
            		ETLMonitorServerConstants.IS_STARTABLE_DESC, null, "java.lang.Boolean",
                    MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[15] = new MBeanOperationInfo(ETLMonitorServerConstants.IS_STOPPABLE,
            		ETLMonitorServerConstants.IS_STOPPABLE_DESC, null, "java.lang.Boolean",
                    MBeanOperationInfo.ACTION_INFO);
            
            mBeanOperationInfos[16] = new MBeanOperationInfo(ETLMonitorServerConstants.START,
            		ETLMonitorServerConstants.START_DESC, null, "java.lang.Boolean",
                    MBeanOperationInfo.ACTION_INFO);

            mBeanOperationInfos[17] = new MBeanOperationInfo(ETLMonitorServerConstants.STOP,
            		ETLMonitorServerConstants.STOP_DESC, null, "java.lang.Boolean",
                    MBeanOperationInfo.ACTION_INFO);

            this.mBeanInfo = new MBeanInfo("ETLMonitor", ETLMonitorServerConstants.ETL_MONITOR_DESCRIPTION, mBeanAttributeInfos,
                mBeanConstructorInfos, mBeanOperationInfos, mBeanNotificationInfos);

            this.properties.put(ATTR_STATUS, STATUS_UP);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // *****
    // ***** EmManagementInterface methods - Start *****
    // *****

    // Return first connector status, eTL will have max one connector.
    private String getConnectorStatus(){
    	String ret = null;
    	if (mConnectorMBeanObjectNames != null){
    		Iterator itr = mConnectorMBeanObjectNames.iterator();
    		try {
	    		if (itr.hasNext()){
	    			ObjectName mbeanObjName = (ObjectName) itr.next();
	    			String connStatus = (String) mMBeanServer.invoke(mbeanObjName, "getStatus", null, null);
	    			if (connStatus != null){
	    				ret = (String) VALID_STATUS_MAP.get(connStatus.toUpperCase());
	    			}
	    		}
    		}catch (Exception ex){
    			//Logger.printThrowable(Logger.WARN, LOG_CATEGORY, this, "Exception while starting/stopping connectors:", ex);    			
    		}
    	}
    	
    	return ret;
    }
        
    private boolean startStopConnectors(boolean start){
    	boolean exceptionWhileStartStop = false;    	
    	Iterator itr = this.mConnectorMBeanObjectNames.iterator();
    	ObjectName mbeanObjName = null;
    	Boolean isStartOrStoppable = null; 
    	while (itr.hasNext()){
    		mbeanObjName = (ObjectName) itr.next();
    		try {
	    		if (start){
	    			isStartOrStoppable = (Boolean) mMBeanServer.invoke(mbeanObjName, "isStartable", null, null);
	    			
	    			if (FORCE_START_STOP || ((isStartOrStoppable != null) && (isStartOrStoppable.booleanValue()))){
	    				mMBeanServer.invoke(mbeanObjName, "start", null, null);
	    			}else{
	    				exceptionWhileStartStop = true;
	    			}
	    		}else{
	    			isStartOrStoppable = (Boolean) mMBeanServer.invoke(mbeanObjName, "isStoppable", null, null);
	    			
	    			if (FORCE_START_STOP || ((isStartOrStoppable != null) && (isStartOrStoppable.booleanValue()))){
	    				mMBeanServer.invoke(mbeanObjName, "stop", null, null);
	    			}else{
	    				exceptionWhileStartStop = true;
	    			}
	    		}
    		}catch (Exception ex){
    			exceptionWhileStartStop = true;
    			//Logger.printThrowable(Logger.WARN, LOG_CATEGORY, this, "Exception while starting/stopping connectors:", ex);
    		}
    	}
    	return exceptionWhileStartStop;
    }
    
	/**
	 * Start method: Start the component- the semantics of this operation is left to implementation
	 */
	public synchronized void start() {
		if (isStartable().booleanValue()){		
			if (this.mMBeanServer != null) {
				boolean exception = startStopConnectors(true);
				if (exception){
					throw new UnsupportedOperationException("There were exception in starting one or more connectors.");
				}else{
					this.properties.setProperty(ATTR_STATUS, STATUS_UP);					
				}
			}
		}else{
			throw new IllegalStateException("Can not Start: 'Start' not allowed or already started or being started.");
		}		
	}

	/**
	 * Restart method: Restart the component- the semantics of this operation is left to implementation
	 */
	public void restart(){
		throw new UnsupportedOperationException("Restart operation is not supported.");
	}
	
	/**
	 * Stop method: Stop the component - the semantics of this operation is left to implementation 
	 */
	public synchronized void stop(){
		if (isStoppable().booleanValue()){		
			if (this.mMBeanServer != null){
				boolean exception = startStopConnectors(false);				
				if (exception){
					throw new UnsupportedOperationException("There were exception in starting one or more connectors.");
				}else{
					this.properties.setProperty(ATTR_STATUS, STATUS_DOWN);					
				}
			}			
		}else{
			throw new IllegalStateException("Can not Stop: 'Stop' not allowed or already stopped or being stopped.");
		}
	}
	
	/**
	 * Return the status of the component e.g. Up/Down/Unknown
	 */
    public String getStatus() {
    	if (this.mETLMbeanConfig.isInboundConnectorStartStoppable()){
        	String connStatus = getConnectorStatus();
        	if (connStatus != null){
        		this.properties.setProperty(ATTR_STATUS, connStatus);
        	}
    	}
    	
    	return this.properties.getProperty(ATTR_STATUS);
    }

	/**
	 * Returns a list of properties: name-value pairs
	 */
    public Properties getProperties() {
    	Properties prop = new Properties();
    	prop.putAll(this.properties);
    	return prop;
    }
    
	/**
	 * This method will be used to determine whether a "start" button would be 
	 * presented to the user return true if the component can be started (remotely)
	 */
	public Boolean isStartable(){
		String status = this.getStatus();
		if ( this.mETLMbeanConfig.isInboundConnectorStartStoppable() 
				&& (status != null) 
				&& STATUS_DOWN.equals(status)){
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}
	
	/**
	 * This method will be used to determine whether a "restart" button 
	 * would be presented to the user return true if the componennt can be restarted. 
	 */

	public Boolean isRestartable(){
		return Boolean.FALSE;
	}
	
	/**
	 * This method will be used to determine whether a "stop" button would 
	 * be presented to the user return true if the component can be stopped.
	 */
	public Boolean isStoppable(){
		String status = this.getStatus();		
		if ( this.mETLMbeanConfig.isInboundConnectorStartStoppable()
				&& (status != null) 
				&& STATUS_UP.equals(status)){
			return Boolean.TRUE;
		}
		
		return Boolean.FALSE;
	}
    // *****
    // ***** EmManagementInterface methods - End *****
    // *****
	

    // *****
    // ***** MBeanRegistration methods - Start *****
    // *****
		
	public void postDeregister() {
		// Do nothing
		
	}


	public void postRegister(Boolean arg0) {
		// Do nothing
		
	}

	public void preDeregister() throws Exception {
		this.mMBeanServer = null;
	}

	public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
		this.mMBeanServer = server;
		return name;
	}

    // *****
    // ***** MBeanRegistration methods - End *****
    // *****	
}
