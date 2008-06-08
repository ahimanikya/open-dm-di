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
 * @(#)AlertsUtil.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import com.sun.jbi.alerter.Alerter;
import com.sun.jbi.alerter.AlerterImpl;

/**
 *
 * @author Sun
 */
public class AlertsUtil {
    public static final String SERVER_TYPE_GLASSFISH = "glassfish";
    public static final String COMPONENT_TYPE = "ServiceEngine";
    
    private static Alerter alertDel;
    
    private static AlertsUtil singleton;
    
    private AlertsUtil () {alertDel = new AlerterImpl();}
    
    public static Alerter getAlerter() {
        if (singleton == null) {
            singleton = new AlertsUtil();
        }
        return alertDel;
    }    
    
    public static String getServerType() {
        return SERVER_TYPE_GLASSFISH;
    }
}
