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
 * @(#)RuntimeUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.runtime;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.ScEncrypt;

public class RuntimeUtil {
	// Custom message types
	public static final String EVT_COLLAB_STARTED = "ETL-00001";
	public static final String EVT_COLLAB_STOPPED = "ETL-00002";	
	public static final String EVT_COLLAB_EXCEPTION = "ETL-00003";
	
	public static final String EVT_ACTIVITY_STARTED = "ETL-00004";
	public static final String EVT_ACTIVITY_STOPPED = "ETL-00005";	
	public static final String EVT_ACTIVITY_EXCEPTION = "ETL-00006";
	
	private static final String LOG_CATEGORY = RuntimeUtil.class.getName();
	//private static final String EVENT_FORWARDER_MBEAN_NAME = "EventManagement:name=EventForwarderMBean";
	//private static final String[] MBEAN_INVOCATION_PARAM_SIG = new String[] { "com.sun.eventmanagement.NotificationEvent" };
	
	// Constants used for Properties key
	public static final String COLLAB_NAME = "collabName";	
	public static final String COLLAB_ID = "collabId";	
	public static final String PROJECT_NAME = "projectName";
	public static final String DEPLOYMENT_NAME = "deploymentName";
	public static final String ENV_NAME = "envName";	
	public static final String LOGICAL_HOST_NAME = "logicalHostName";
	public static final String PHYSICAL_HOST_NAME = "physicalHostName" ;
	public static final String IS_NAME = "integrationServerName";	
	public static final String TARGET_TABLE_NAME = "targetTableName";	
	public static final String EXECUTION_ID = "executionId";	
	public static final String EXECEPTION = "exception";	
	
	//private static Properties msgId2Msg = null;
	
//	public static MBeanServer getMBeanServer() {
//		MBeanServer mBeanServer = MbeanLoaderRegistry.getInstance().findMBeanServer();
//		ObjectName eventForwarderMBeanName = null;
//		try {
//			eventForwarderMBeanName = eventForwarderMBeanName = new ObjectName(EVENT_FORWARDER_MBEAN_NAME);
//			if (mBeanServer.isRegistered(eventForwarderMBeanName) == false) {
//				Logger.print(Logger.WARN, LOG_CATEGORY, "EventForwarderMBean MBean is not registered. Event discarded.");
//			}
//		} catch (MalformedObjectNameException ex) {
//			mBeanServer = null;
//			Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, RuntimeUtil.class, "EventForwarderMBean MBean is not registered. Event discarded.", ex);
//		}
//		return mBeanServer;
//	}


//	private static Properties getMessageProperties() throws Exception {
//		if (msgId2Msg == null){
//			synchronized(RuntimeUtil.class){
//				if (msgId2Msg == null){
//					msgId2Msg = new Properties();
//					InputStream is = null;
//					try{
//						is = RuntimeUtil.class.getResourceAsStream("/eTLAlert.properties");
//						msgId2Msg.load(is);
//					}finally{
//						if (is != null){
//							try{
//								is.close();
//							}catch(Exception ex){
//								// ignore
//							}
//						}
//					}
//				}
//			}
//		}
//		
//		return msgId2Msg;
//	}
//	public static String getMessage(String msgCode){
//		String msgDet = "" ;
//		try {
//			Properties prop = getMessageProperties();
//			msgDet = prop.getProperty(msgCode);
//		}catch (Exception ex){
//			Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, RuntimeUtil.class, "While loading alert messages.", ex);
//		}
//		
//		return msgDet;
//	}
//	
	/**
	 * @param notificationProps
	 * @param type
	 * @param msg
	 * @param mbServer
	 */
//	public static void sendNotificationEvent(Properties notificationProps, String type, MBeanServer mbServer, 
//			                                 int severity, int opStatus) {
//		ObjectName eventForwarderMBeanName = null;
//		String msg = getMessage(type);
//		try {
//			eventForwarderMBeanName = eventForwarderMBeanName = new ObjectName(EVENT_FORWARDER_MBEAN_NAME);
//			
//			msg = StringUtil.replace(msg, notificationProps);
//			NotificationEvent event = new EventFactory().getNotificationEvent(
//											 notificationProps.getProperty(PHYSICAL_HOST_NAME), 
//											 notificationProps.getProperty(DEPLOYMENT_NAME), 
//											 notificationProps.getProperty(ENV_NAME), 
//											 notificationProps.getProperty(LOGICAL_HOST_NAME),
//											 Event.SERVER_TYPE_INTEGRATION, 
//											 notificationProps.getProperty(IS_NAME),
//											 Event.COMPONENT_TYPE_COLLABORATION, 
//											 notificationProps.getProperty(PROJECT_NAME), 
//											 notificationProps.getProperty(COLLAB_NAME),
//											 "Alert", 
//											 severity, 
//											 opStatus, 
//											 type, 
//											 null, 
//											 msg);
//
//			Object[] params = new Object[1];
//			params[0] = event;			
//			mbServer.invoke(eventForwarderMBeanName,EventProperties.EVENT_FORWARD_OPERATION, params, MBEAN_INVOCATION_PARAM_SIG);
//		} catch (Exception ex) {
//			Logger.printThrowable(Logger.WARN, LOG_CATEGORY, RuntimeUtil.class, "Exception while sending notification event. Event discarded.", ex);
//		}
//	}
//	
	
	public static Map getLDAPResolvedParameters(String jdbcUid, String jdbcPswd) throws Exception{
		Map ret = new HashMap();
		ret.put(DBConnectionParameters.USER_NAME_ATTR, jdbcUid);
		ret.put(DBConnectionParameters.PASSWORD_ATTR, jdbcPswd);
		
		boolean uidChanged = false;
		String resolvedUid = null;
		String resolvedPswd = null;
		
		if (jdbcPswd != null) {
			if (jdbcUid == null){
				jdbcUid = "";
			}
								
			// Decrypt the field, using UID lookup string
			jdbcPswd = ScEncrypt.decrypt(jdbcUid, jdbcPswd);
			Logger.print(Logger.DEBUG, LOG_CATEGORY, "Decrypted PSWD.");					
		}
		
		if (DBConnectionFactory.isLDAPQuery(jdbcUid)){
//			resolvedUid = lookupLDAP(jdbcUid);
			if (resolvedUid == null){
				resolvedUid = jdbcUid;
			}					
			ret.put(DBConnectionParameters.USER_NAME_ATTR, resolvedUid);
			uidChanged = true;
			Logger.print(Logger.DEBUG, LOG_CATEGORY, "Looked-up UID:" + resolvedUid);									
		}else{
			resolvedUid = jdbcUid;
		}

		if (DBConnectionFactory.isLDAPQuery(jdbcPswd)){						
//			resolvedPswd = lookupLDAP(jdbcPswd);
			Logger.print(Logger.DEBUG, LOG_CATEGORY, "Looked-up PSWD.");					
			// Encrypt the field back using new UID
			resolvedPswd = ScEncrypt.encrypt(resolvedUid.trim(), resolvedPswd.trim());
			ret.put(DBConnectionParameters.PASSWORD_ATTR, resolvedPswd);
			Logger.print(Logger.DEBUG, LOG_CATEGORY, "New encrypted PSWD:" + resolvedPswd);					
		}else{
			if (uidChanged){
				resolvedPswd = ScEncrypt.encrypt(resolvedUid.trim(), jdbcPswd);
				ret.put(DBConnectionParameters.PASSWORD_ATTR, resolvedPswd);
				Logger.print(Logger.DEBUG, LOG_CATEGORY, "New encrypted PSWD:" + resolvedPswd);						
			}
		}

		return ret;
	}
	
//	  private static String lookupLDAP(String ldapQuery) throws Exception {
//		AttributeTransformer tr = null;
//		String transformedStr = ldapQuery;
//		int colonpos = ldapQuery.indexOf(':');
//		if (colonpos != -1) {
//			String prefixStr = ldapQuery.substring(0, colonpos);
//			if (prefixStr != null && prefixStr.length() > 0) {
//				tr = AttributeTransformerFactory.getInstance().getTransformer(
//						prefixStr);
//			}
//			if (tr != null) {
//				try {
//					transformedStr = tr.getValue(ldapQuery);
//					Logger.print(Logger.DEBUG, LOG_CATEGORY, "LDAP Query:"
//							+ ldapQuery + " Result:" + transformedStr);
//				} catch (MalformedURLException ex) {
//					Logger.printThrowable(Logger.ERROR, LOG_CATEGORY,
//							DBConnectionFactory.class,
//							"MalformedURLException while querying LDAP:", ex);
//					transformedStr = null;
//					throw ex;
//				} catch (AttributeTransfomException ex) {
//					Logger.printThrowable(Logger.ERROR, LOG_CATEGORY,
//							DBConnectionFactory.class,
//							"AttributeTransfomException while querying LDAP:",
//							ex);
//					transformedStr = null;
//					throw ex;
//				}
//
//			}
//		}
//		return transformedStr;
//	}
	  
	  
	public static String getExceptionStackTrace(Throwable ex){
		StringBuffer sb = new StringBuffer("Exception is:");
		sb.append("(");
		sb.append(ex.getClass().getName());
		sb.append(")");
		
		sb.append(ex.getMessage());

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		try {
			ex.printStackTrace(pw);
			sb.append("\n");
			sb.append(sw.toString());
			pw.close();
			sw.close();
		}catch(Exception ex1){
			Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, RuntimeUtil.class, "Exception while send exception alert message.", ex1);
			sb = new StringBuffer("\nException stack trace not available.");
		}
		
		return sb.toString();
	}
}
