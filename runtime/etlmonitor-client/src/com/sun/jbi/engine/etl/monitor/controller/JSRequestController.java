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
 * @(#)JSRequestController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jbi.engine.etl.monitor.mbeans.ETLMonitorServerConstants;

/**
 * Controller for all JavaScript Interactive Actions AKA AJAX.
 * @author Girish Patil
 * @version 
 */
public class JSRequestController extends BaseController {
    private static final Logger LOGGER = Logger.getLogger(JSRequestController.class.getName());

    protected String perform() throws Exception {
        String action = this.getCtx().getRequest().getParameter("a");
        this.getCtx().getResponse().setHeader("Cache-Control", "no-cache");
        this.getCtx().getResponse().setHeader("Pragma", "nocache");

        if ((action != null) && (!"".equals(action))){
            try {
                if ("start".equals(action) || "stop".equals(action)){
                    Object obj = invokeMonitorMethod(action, null, null);
                    if (obj instanceof Map){
                        String excp = (String) ((Map)obj).get(ETLMonitorServerConstants.EXECEPTION);
                        if ((excp != null) && (!"".equals(excp))){
                            this.getCtx().getRequest().setAttribute("interactiveBdy", getStatus() + "|exception");
                            LOGGER.log(Level.WARNING, "while executing monitor start/stop method."+ excp);
                        }else{
                            this.getCtx().getRequest().setAttribute("interactiveBdy", getStatus() + "|success");
                        }
                    }else{
                        this.getCtx().getRequest().setAttribute("interactiveBdy", getStatus() + "|success");
                    }
                }
            }catch (Exception ex){
                this.getCtx().getRequest().setAttribute("interactiveBdy", getStatus() + "|exception");
                LOGGER.log(Level.WARNING, "while executing monitor start/stop method.", ex);
            }
        }

        return DEFAULT;
    }
}
