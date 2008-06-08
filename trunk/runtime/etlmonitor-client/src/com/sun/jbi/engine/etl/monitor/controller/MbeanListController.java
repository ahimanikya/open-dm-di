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
 * @(#)MbeanListController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import java.util.Set;

import javax.servlet.http.HttpSession;

import org.infohazard.maverick.ctl.ThrowawayBean2;



public class MbeanListController extends ThrowawayBean2 {

	
	protected String perform() throws Exception {
		HttpSession s = this.getCtx().getRequest().getSession(true);
		Set mbeans = MBeanUtil.getETLEngineMbeanList();
		s.setAttribute(MBeanUtil.ETL_MBEAN_LIST, mbeans);
		
		return SUCCESS;
	}

}
