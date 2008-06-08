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
 * @(#)PageController.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.controller;

import org.infohazard.maverick.ctl.ThrowawayBean2;

import com.sun.jbi.engine.etl.monitor.data.PageInfo;

/**
 * A Page Controller which provides the paging mechanism for summary and details page.  
 * Subclass overrides getPageInfo() to provide a specific PageInfo object which provides 
 * paging information.
 * 
 * @author Ritesh Adval
 */
public abstract class PageController extends ThrowawayBean2 {
	
	protected String perform() throws Exception {
		performPaging();
		return SUCCESS;
	}
	
	private void performPaging() {
		
		String destinationPage = this.getCtx().getRequest().getParameter("destinationPage");
		PageInfo pageInfo = this.getPageInfo();
		
		if(destinationPage.equals("pageFirst")) {
			pageInfo.goToFirstPage();
		} else if(destinationPage.equals("pagePrevious")) {
			pageInfo.goToPreviousPage();
		} else if(destinationPage.equals("pageNext")) {
			pageInfo.goToNextPage();
		} else if(destinationPage.equals("pageLast")) {
			pageInfo.goToLastPage();
		} else {
			pageInfo.setCurrentPage(Integer.parseInt(destinationPage));
		}
	}
	
	public abstract PageInfo getPageInfo();	
	
}
