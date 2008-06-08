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
 * @(#)DetailsPageController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import com.sun.jbi.engine.etl.monitor.data.ETLServiceInfo;
import com.sun.jbi.engine.etl.monitor.data.PageInfo;

/**
 * @author Ritesh Adval
 * Details Page Controller provides paging navigation for the details
 * page of the etl monitor web application.
 *  It just implements getPageIngo to return PageInfo object
 *  for a given execution id of a details page.
 *
 */
public class DetailsPageController extends PageController {

    public PageInfo getPageInfo() {
        String component = this.getCtx().getRequest().getParameter("component");
        String executionId = this.getCtx().getRequest().getParameter("executionId");
        ETLServiceInfo serviceInfo = (ETLServiceInfo) this.getCtx().getRequest()
                                                            .getSession().getAttribute(component);
        return serviceInfo.getDetailsPageInfo(executionId);
    }
}
