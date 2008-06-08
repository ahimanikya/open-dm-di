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
 * @(#)ETLSEBootstrap.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import javax.management.ObjectName;
import javax.management.MBeanServer;
import javax.jbi.component.Bootstrap;
import javax.jbi.component.InstallationContext;
import javax.jbi.component.ComponentContext;
import javax.jbi.management.MBeanNames;
import javax.management.StandardMBean;
import javax.jbi.JBIException;
import com.sun.jbi.engine.etl.Localizer;
import com.sun.jbi.configuration.ConfigPersistence;
import com.sun.jbi.engine.etl.mbean.ETLSEInstallerConfigurationMBean;
import com.sun.jbi.engine.etl.mbean.ETLSEInstallerConfiguration;

public class ETLSEBootstrap implements Bootstrap {

	private static transient final Logger mLogger = Logger.getLogger(ETLSEBootstrap.class.getName());

    private static transient final Localizer mLoc = Localizer.get();

    private InstallationContext mContext;

    private ObjectName mInstallerExtName;

    public ETLSEBootstrap() {
    }

    /**
     * Initializes the installation environment for a SE. This method is
     * expected to save any information from the installation context that
     * may be needed by other methods.
     * @param installContext is the context containing information from the
     * install command and from the SE jar file.
     * @throws JBIException when there is an error requiring that the
     * installation be terminated.
     */
    public void init(InstallationContext installContext)
        throws javax.jbi.JBIException {           
		mLogger.log(Level.INFO,mLoc.loc("INFO007: Calling init method"));
        mContext = installContext;
        MBeanServer mbeanServer = installContext.getContext().getMBeanServer();
        MBeanNames mbeanNames = installContext.getContext().getMBeanNames();
        String compName = installContext.getContext().getComponentName();
        try {
            ETLSEInstallerConfigurationMBean installerExt = new ETLSEInstallerConfiguration();
            mInstallerExtName = mbeanNames.createCustomComponentMBeanName(MBeanNames.BOOTSTRAP_EXTENSION);
            if (!mbeanServer.isRegistered(mInstallerExtName)) {
                StandardMBean stdBean = new StandardMBean(installerExt, ETLSEInstallerConfigurationMBean.class);
                mbeanServer.registerMBean(stdBean,  mInstallerExtName);
            }
        } catch (Exception ex) {
            throw new JBIException("Caught exception while creating Installatoin Configuration MBean, failed to init etlse component", ex);
        }
		mLogger.log(Level.INFO,mLoc.loc("INFO008: init method has been called"));
    }
 
    /**
     * Obtains the optional installer configuration MBean ObjectName. If none
     * is provided by this SE, returns null.
     * @return ObjectName which represents the MBean registered by the init()
     * method. If none was registered, returns null.
     */
    public ObjectName getExtensionMBeanName() {
        return mInstallerExtName;
    }

    /**
     * Called at the beginning of installation of a SE to perform any special
     * installation tasks required by the SE.
     * @throws JBIException when there is an error requiring that the
     * installation be terminated.
     */
    public void onInstall() throws JBIException {
		mLogger.log(Level.INFO,mLoc.loc("INFO009: Calling onInstall method"));
        ComponentContext ctx = mContext.getContext();
        MBeanServer mbServer = ctx.getMBeanServer();
        org.w3c.dom.DocumentFragment descriptorExtension = mContext.getInstallationDescriptorExtension();
        Properties defaultProperties = ConfigPersistence.parseDefaultConfig(descriptorExtension);
        ConfigPersistence.persistInitialConfig(mbServer, mInstallerExtName, ctx.getWorkspaceRoot(), defaultProperties);
		mLogger.log(Level.INFO,mLoc.loc("INFO010: onInstall method has been called"));
    }

    /**
     * Called at the beginning of uninstallation of a SE to perform any
     * special uninstallation tasks required by the SE.
     * @throws JBIException when there is an error requiring that
     * the uninstallation be terminated.
     */
    public void onUninstall()
        throws javax.jbi.JBIException {
		mLogger.log(Level.INFO,mLoc.loc("INFO011: Calling onUninstall method"));
		mLogger.log(Level.INFO,mLoc.loc("INFO412: onUninstall method has been called"));
    }
    
    /**
     * Cleans up any resources allocated by the bootstrap implementation, 
     * including performing deregistration of the extension MBean, if applicable. 
     * This method must be called after the onInstall() or onUninstall() method 
     * is called, whether it succeeds or fails. It must be called after init() 
     * is called, if init() fails by throwing an exception. 
     *
     * @throws JBIException - if the bootstrap cannot clean up allocated resources}
     */
    public void cleanUp() throws JBIException {
        try {
            if (mInstallerExtName != null) {
                ComponentContext ctx = mContext.getContext();
                if (ctx != null) {
                    if (ctx.getMBeanServer().isRegistered(mInstallerExtName)) {
                        ctx.getMBeanServer().unregisterMBean(mInstallerExtName);
						mLogger.log(Level.INFO,mLoc.loc("INFO012: Unregistered MBean{0}",mInstallerExtName));
                    }
                }
            }
        } catch (Exception ex) {
            throw new JBIException("Failed to unregister MBean " + mInstallerExtName + ": " + ex.getMessage(), ex);
        }
    }

}
