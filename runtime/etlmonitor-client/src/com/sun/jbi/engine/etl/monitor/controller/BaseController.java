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
 * @(#)BaseController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import org.infohazard.maverick.ctl.ThrowawayBean2;

import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;
/**
 * Base Controller.
 * 
 * @author Girish Patil
 * @version 
 */
public abstract class BaseController extends ThrowawayBean2{
	private static final Logger LOGGER = Logger.getLogger(BaseController.class.getName());
	protected static final String DEFAULT = "default";

	protected Object invokeMonitorMethod(String method, Object[] param, String[] sig) throws Exception {
        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
        MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
        return mBeanServer.invoke(objName, method, param, sig);
        
	}

	protected String getStatus() {
		String ret = "";
		try {
	        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
	        MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
	        String tStatus = (String) mBeanServer.invoke(objName, ETLMonitorServerConstants.GET_STATUS, null, null);
	        if (tStatus != null){
	        	ret = tStatus;
	        }
		}catch(Exception ex){
			LOGGER.log(Level.WARNING, "BaseController.getStatus()", ex);
		}
		
		return ret;
	}

	protected String getComponentName() {
		String ret = "";
		
		String temp = (String) this.getCtx().getRequest().getAttribute(MBeanUtil.ATTR_COMPONENT);
		if ((temp == null) || ("".equals(temp))){
			temp = (String) this.getCtx().getRequest().getSession().getAttribute(MBeanUtil.ATTR_COMPONENT);
		}
		
		if ((temp != null) && (!"".equals(temp))){
			ret = temp;
		}
		
		return ret;
	}

	protected boolean isStartable() {
		boolean ret = false;
		try {
	        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
	        MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
	        Boolean boolObj = (Boolean) mBeanServer.invoke(objName, ETLMonitorServerConstants.IS_STARTABLE, null, null);
	        ret = boolObj.booleanValue();
		}catch(Exception ex){
			LOGGER.log(Level.WARNING, "BaseController.isStartable()", ex);
		}
		
		return ret;
	}

	protected boolean isStoppable() {
		boolean ret = false;
		try {
	        ObjectName objName = MBeanUtil.getETLMBeanObjectName(this.getCtx());
	        MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
	        Boolean boolObj = (Boolean) mBeanServer.invoke(objName, ETLMonitorServerConstants.IS_STOPPABLE, null, null);
	        ret = boolObj.booleanValue();
		}catch(Exception ex){
			LOGGER.log(Level.WARNING, "BaseController.isStoppable()", ex);
		}
		
		return ret;
	}

}
