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
 * @(#)MBeanUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infohazard.maverick.flow.ControllerContext;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;

/**
 * A utility class which provides access to eTL MBean, which exposes methods
 * that this web application calls to provide the functionality.
 * 
 * @author Ritesh Adval
 */
public class MBeanUtil {
	public static final String MANAGEMENT_AGENT_SERVER_NAME = "ICAN Enterprise Monitor Management Agent";

	public static final String PROXY_MBEAN_NAME = "EM:type=service,name=ProxyService";

	public static final String TREEBUILDER_MBEAN_NAME = "EM:type=service,name=TreeBuilder";

	public static final String ETL_OBJECT_TYPE = "ETL.ETLDefinition";

	public static final String ATTR_COMPONENT = "component";

	public static final String ETL_MBEAN_LIST = "etl_mbean_list";

	private static final String[] STRING_TYPE_ARRAY = new String[] { "java.lang.String",
			"java.lang.String" };

	private static final Log log = LogFactory.getLog(MBeanUtil.class);

	private static MBeanServerConnection mbeanServer = null; // MBeans
	// container

	static {
		initMBeanServer();
	}

	public static ObjectName getETLMBeanObjectName(ControllerContext context) throws Exception {
		HttpServletRequest request = context.getRequest();
		HttpSession session = context.getRequest().getSession();

		String component = request.getParameter(ATTR_COMPONENT);
		if (component != null) {
			// set the current component back into session
			session.setAttribute(ATTR_COMPONENT, component);
		} else {
			component = (String) session.getAttribute(ATTR_COMPONENT);
		}

		ObjectName etlMbeanObjectName = null;

		if (component != null) {
			ETLServiceInfo existingServiceInfo = (ETLServiceInfo) session.getAttribute(component);
			if (existingServiceInfo != null) {
				ObjectName currObj = existingServiceInfo.getETLMBeanObjectName();
				if (mbeanServer.isRegistered(currObj)) {
					return currObj;
				}

				log.info("Object " + currObj
						+ " has been unregistered on appserver - flushing cache.");
			}
		}

		Object[] params = new Object[] { component, ETL_OBJECT_TYPE };

		// Get eTL MBean object name from MBean server.
		// String etlMbeanObjectNameStr = (String) mbeanServer.invoke(new
		// ObjectName(
		// TREEBUILDER_MBEAN_NAME), "getObjectName", params, STRING_TYPE_ARRAY);
		// if (null == etlMbeanObjectNameStr ||
		// "".equals(etlMbeanObjectNameStr.trim())) {
		// return null;
		// }

		String etlMbeanObjectNameStr = "ETL Monitoring:type=service,collab=" + component;
		ObjectName etlRuntimeMBean = new ObjectName(etlMbeanObjectNameStr + ",*");

		Set etlMBeans = mbeanServer.queryMBeans(etlRuntimeMBean, null);
		if (etlMBeans.size() == 1) {
			ObjectInstance etlObjIn = (ObjectInstance) etlMBeans.iterator().next();
			etlMbeanObjectName = etlObjIn.getObjectName();
		}

		// create a service info for a given component and keep in session
		String pageSize = context.getServletContext().getInitParameter("pageSize");
		String exportPageSize = context.getServletContext().getInitParameter("exportPageSize");
		ETLServiceInfo serviceInfo = new ETLServiceInfo(component, etlMbeanObjectName, Integer
				.parseInt(pageSize), Integer.parseInt(exportPageSize));
		session.setAttribute(component, serviceInfo);

		return etlMbeanObjectName;
	}

	private static void initMBeanServer() {

		try {
			HashMap env = new HashMap();

			String[] credentials = new String[] { "admin", "adminadmin" };
			env.put(JMXConnector.CREDENTIALS, credentials);

			JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:8686/jmxrmi");

			JMXConnector jmxc = JMXConnectorFactory.connect(url, env);
			mbeanServer = jmxc.getMBeanServerConnection();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static MBeanServerConnection getMBeanServer() {
		return mbeanServer;
	}

	public static Set getETLEngineMbeanList() throws Exception {
		String etlMbeanObjectNameStr = "ETL Monitoring:type=service";
		ObjectName etlRuntimeMBean = new ObjectName(etlMbeanObjectNameStr + ",*");

		Set etlMBeans = mbeanServer.queryMBeans(etlRuntimeMBean, null);
		return etlMBeans;

	}

	public static ObjectName getETLMBeanObjectName(String collabName) {
		// TODO Auto-generated method stub
		return null;
	}
}
