/*
 * FilterDate.java
 *
 * Created on November 12, 2006, 6:52 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import com.sun.jbi.cam.plugin.etlse.jmx.MonitorClient;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.cam.plugin.etlse.model.Localizer;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;

/**
 * backing bean which represents the filter date. This is used as a controller for query/select and purge the summary table of a given collab
 * 
 * @author Sujit Biswas
 */
public class FilterDate {

	private static transient final Logger mLogger = Logger.getLogger(FilterDate.class.getName());

    private static transient final Localizer mLoc = Localizer.get();

	private Date startDate;
	private Date endDate;

	private Date minDate;
	private Date maxDate;

	private Date olderThanDate;

	private boolean purgeAll;
        

	/**
	 * Creates a new instance of FilterDate
	 */
	public FilterDate() {

		Calendar cal = new GregorianCalendar(1999, 0, 1);

		minDate = cal.getTime();

		cal = new GregorianCalendar(2100, 0, 1);
		maxDate = cal.getTime();
	}

	/**
	 * @return the endDate
	 */
	public Date getEndDate() {
		return endDate;
	}

	/**
	 * @param endDate
	 *            the endDate to set
	 */
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
		mLogger.log(Level.INFO,mLoc.loc("INFO211: set end date: {0}",endDate));
	}

	/**
	 * @return the maxDate
	 */
	public Date getMaxDate() {
		return maxDate;
	}

	/**
	 * @param maxDate
	 *            the maxDate to set
	 */
	public void setMaxDate(Date maxDate) {
		this.maxDate = maxDate;
	}

	/**
	 * @return the minDate
	 */
	public Date getMinDate() {
		return minDate;
	}

	/**
	 * @param minDate
	 *            the minDate to set
	 */
	public void setMinDate(Date minDate) {
		this.minDate = minDate;
	}

	/**
	 * @return the startDate
	 */
	public Date getStartDate() {
		return startDate;
	}

	/**
	 * @param startDate
	 *            the startDate to set
	 */
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
		mLogger.log(Level.INFO,mLoc.loc("INFO212: set start date: {0}",startDate));
	}

	/**
	 * @return the olderThanDate
	 */
	public Date getOlderThanDate() {
		return olderThanDate;
	}

	/**
	 * @param olderThanDate
	 *            the olderThanDate to set
	 */
	public void setOlderThanDate(Date olderThanDate) {
		this.olderThanDate = olderThanDate;
	}

	public void select() {
		mLogger.log(Level.INFO,mLoc.loc("INFO213: select(): "));
		FacesContext ctx = FacesContext.getCurrentInstance();
		FacesMessage msg = new FacesMessage();
		Date sdate = getStartDate();
		Date edate = getEndDate();
		if(sdate != null && edate !=null ){
			
		
			if(edate.before(sdate)){
				msg.setSeverity(FacesMessage.SEVERITY_ERROR);
				msg.setSummary("Start Date should be before End date");
				msg.setDetail("Start Date should be before End date");
				ctx.addMessage("selectform", msg);
				return;
			}
		}
		Map session = ctx.getExternalContext().getSessionMap();
		CollabList cl = (CollabList)session.get("mbeanlist");
		CollabSummaryList csl = (CollabSummaryList)session.get("MbeanSummaryList");
		
		csl.fetchData(cl.getCurrentCollab(),this);
	}

        public void purge() {
		mLogger.log(Level.INFO,mLoc.loc("INFO214: purge(): "));
            FacesContext ctx = FacesContext.getCurrentInstance();
            Map session = ctx.getExternalContext().getSessionMap();
            CollabList cl = (CollabList) session.get("mbeanlist");
            CollabSummaryList csl = (CollabSummaryList) session.get("MbeanSummaryList");
            mLogger.log(Level.INFO, cl.getCurrentCollab());
            csl.purge(cl.getCurrentCollab(), this.getOlderThanDate(), this.isPurgeAll());
	}

	/**
	 * @return the purgeAll
	 */
	public boolean isPurgeAll() {
		return purgeAll;
	}

	/**
	 * @param purgeAll
	 *            the purgeAll to set
	 */
	public void setPurgeAll(boolean purgeAll) {
		this.purgeAll = purgeAll;
	}

}
