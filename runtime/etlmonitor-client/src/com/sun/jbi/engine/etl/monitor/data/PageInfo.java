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
 * @(#)PageInfo.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.data;

/**
 * @author Ritesh Adval
 *
 * A PageInfo class provides information which is used for 
 * page navigation in summary and details page.
 */
public class PageInfo {
	private int currentPage = 1;
	private int pageSize;
	private int totalPages = 0;
	private int totalRowCount = 0;
	
	private String defaultStartDate = "";
	private String defaultEndDate = "";
	
	private String startDate = defaultStartDate;
	private String endDate = defaultEndDate;
	
	public PageInfo(int pSize) {
		this.pageSize = pSize;
	}
	
	public int getCurrentPage() {
		return this.currentPage;
	}
	
	public void setCurrentPage(int cPage) {
		this.currentPage = cPage;
	}
	
	public void setPageSize(int pSize) {
		this.pageSize = pSize;
	}
	
	public int getPageSize() {
		return this.pageSize;
	}
	
	public void setTotalPages(int tPages) {
		this.totalPages = tPages;
	}
	
	public int getTotalPages() {
		return this.totalPages;
	}
	
	public int getCurrentOffset() {
		return (currentPage-1) * pageSize;
	}
	
	public void setTotalRowCount(int totalRows) {
		if(this.totalRowCount != totalRows) {
			this.totalRowCount = totalRows;
			int absPages = totalRows / getPageSize();
			int remainder = totalRows % getPageSize();
			int additionalPage = (remainder > 0)  ? 1 : 0;
			
			this.setTotalPages(absPages + additionalPage);
		}
	}
	
	public int getTotalRowCount() {
		return totalRowCount;
	}
	
	public void goToFirstPage() {
		currentPage = 1;
	}
	
	public void goToNextPage() {
		if(currentPage +1 <=totalPages) {
			currentPage++;
		}
	}
	
	public void goToPreviousPage() {
		if(currentPage - 1 > 0) {
			currentPage--;
		}
	}
	
	public void goToLastPage() {
		currentPage = totalPages;
	}
	
	public String getStartDate() {
	    return this.startDate;
	}
	
	public void setStartDate(String aValue) {
	    if (aValue != null) {
	        this.startDate = aValue;
	    } else {
	        this.startDate = this.getDefaultStartDate();
	    }
	}
	
	public String getEndDate() {
	    return this.endDate;
	}
	
	public void setEndDate(String aValue) {
	    if (aValue != null) {
	        this.endDate = aValue;
	    } else {
	        endDate = this.getDefaultEndDate();
	    }
	}
	
	public void reset() {
	    this.currentPage = 1;
	}
	
	public String getDefaultStartDate() {
	    return this.defaultStartDate;
	}
	
	public String getDefaultEndDate() {
	    return this.defaultEndDate;
	}
	
}
