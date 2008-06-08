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
 * @(#)Agent.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.io.IOException;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.engine.etl.monitor.mbeans.Localizer;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

/**
 * jmx agent
 * 
 * @author Sujit Biswas
 * 
 */
public class Agent {
	/**
	 * the jdk logger used by the etl monitor agent application
	 */
	private static transient final Logger mLogger = Logger.getLogger(Agent.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
	/**
	 * rmi registry port
	 */
	private int rmiRegistryPort = 9999;
	/**
	 * html adapter port
	 */
	private int htmlAdapterPort = 9099;
	/**
	 * security enabled for etl monitor admin(jmx) service
	 */
	private boolean secuirityEnabled = true;
	/**
	 * hostname of the monitor server
	 */
	private String hostName;
	/**
	 * mbean server
	 */
	private MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	/**
	 * rmi registry
	 */
	private Registry registry;

	/**
	 * Use this constructor when the monitor is run outside the sun java
	 * application server
	 * 
	 * @param startJmxConnector
	 * @throws Exception
	 */
	public Agent(boolean startJmxConnector) throws Exception {
		try {
			init();
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO114: JMX Agent could not be started {0}",e));
			throw e;
		}
	}

	/**
	 * default constructor, to use when the monitor is run within the sun java
	 * application server
	 */
	public Agent() {
	}

	public void init() throws Exception {
		startRmiRegistry();
		startJmxConnectorServer();
		startHtmlAdapter();
	}

	private void startRmiRegistry() throws RemoteException {
		// registry = LocateRegistry.getRegistry(port);
		try {
			registry = LocateRegistry.createRegistry(rmiRegistryPort);
			registry.list();
			mLogger.log(Level.INFO,mLoc.loc("INFO115: RMI Registry started on port: {0}",rmiRegistryPort));
		} catch (RemoteException e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO116: RMI Registry running on port: {0}",rmiRegistryPort));
			;
		}
	}

	/**
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void startJmxConnectorServer() throws MalformedURLException, IOException {
		Map env = new HashMap();
		// env.put(JMXConnectorServer.AUTHENTICATOR,new
		// BridgeAuthenticator(secuirityEnabled));
		env.put("jmx.remote.jndi.rebind", "true");

		JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/server");

		JMXConnectorServer cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
		cs.start();
		mLogger.log(Level.INFO,mLoc.loc("INFO117: JMXConnectorServer started with URL: {0}",url));
	}

	/**
	 * this method requires an external jar lib/jmxtools.jar from the JMX
	 * reference implementation , If the jmxtools.jar is not present in the
	 * classpath the htmladapter won't start
	 * 
	 */
	
	
	private void startHtmlAdapter() {
		try {
			Class cl = Class.forName("com.sun.jdmk.comm.HtmlAdaptorServer");
			Constructor contr = cl.getConstructor(new Class[] { Integer.TYPE });
			Object htmlAdaptor = contr.newInstance(new Object[] { new Integer(htmlAdapterPort) });
			Method method = cl.getMethod("start",(Class[]) null);

			ObjectName adapterObjectName = new ObjectName("HTML_ADAPTER_MBEAN_NAME" + ",port="
					+ htmlAdapterPort);

			mbs.registerMBean(htmlAdaptor, adapterObjectName);
			method.invoke(htmlAdaptor, (Object[])null);
			mLogger.log(Level.INFO,mLoc.loc("INFO118: HTML adapter server  started on port {0}",htmlAdapterPort));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			mLogger.log(Level.INFO,mLoc.loc("INFO119: cannot start HTML adapter server on port {0}",htmlAdapterPort));
			// e.printStackTrace();
		}
	}

	public MBeanServer getMBeanServer() {
		return mbs;
	}

	public static void main(String[] args) {
		try {
			new Agent();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// test
		JMXServiceURL url;

		try {
			url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/server");

			JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
			MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
			String[] domains = mbsc.getDomains();

			for (int i = 0; i < domains.length; i++) {
				System.out.println("Domain[" + i + "] = " + domains[i]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.management.MBeanServer#registerMBean(java.lang.Object,
	 *      javax.management.ObjectName)
	 */
	public ObjectInstance registerMBean(Object object, ObjectName name)
			throws InstanceAlreadyExistsException, MBeanRegistrationException,
			NotCompliantMBeanException {
		return mbs.registerMBean(object, name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.management.MBeanServer#unregisterMBean(javax.management.ObjectName)
	 */
	public void unregisterMBean(ObjectName name) throws InstanceNotFoundException,
			MBeanRegistrationException {
		mbs.unregisterMBean(name);
	}

	/**
	 * @return Returns the hostName.
	 */
	public String getHostName() {
		if (hostName == null) {
			try {
				InetAddress ia = InetAddress.getLocalHost();
				hostName = ia.getHostName();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		return hostName;
	}

	/**
	 * @return Returns the rmiRegistryPort.
	 */
	public int getRmiRegistryPort() {
		return rmiRegistryPort;
	}

	/**
	 * @return Returns the secuirityEnabled.
	 */
	public boolean isSecuirityEnabled() {
		return secuirityEnabled;
	}

	/**
	 * @param secuirityEnabled
	 *            The secuirityEnabled to set.
	 */
	public void setSecuirityEnabled(boolean secuirityEnabled) {
		this.secuirityEnabled = secuirityEnabled;
	}
}
