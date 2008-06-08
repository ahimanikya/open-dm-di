/*
 * CollabSummaryList.java
 *
 * Created on November 11, 2006, 1:21 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.cam.plugin.etlse.model.Localizer;
import javax.faces.context.FacesContext;
import com.sun.data.provider.TableDataProvider;
import com.sun.data.provider.impl.ObjectListDataProvider;
import com.sun.jbi.cam.plugin.etlse.jmx.MonitorClient;
import com.sun.jbi.cam.plugin.etlse.jmx.data.SummaryTotalData;

/**
 * Backing bean which return the list of rows from a Summary table of a given
 * collab, depending on the filter date condition, if filter date is null then
 * it contains all the data from the summary table of a given collab
 * 
 * @author Sujit Biswas
 */
public class CollabSummaryList {

	private static transient final Logger mLogger = Logger.getLogger(CollabSummaryList.class.getName());

    private static transient final Localizer mLoc = Localizer.get();

	private List list;

	private TableDataProvider provider;

	private SummaryTotalData summaryTotalData;

	/**
	 * Creates a new instance of CollabSummaryList
	 */

	public CollabSummaryList() {
		CollabList collabList = getCollabList();
		MonitorClient client = new MonitorClient();
		list = client.getSummary(collabList.getCurrentCollab(), null).getList();

		summaryTotalData = client.getSummaryTotal(collabList.getCurrentCollab(), null);
	}

	/**
	 * @return
	 */
	private CollabList getCollabList() {
		FacesContext ctx = FacesContext.getCurrentInstance();
		Map session = ctx.getExternalContext().getSessionMap();
		CollabList collabList = (CollabList) session.get("mbeanlist");
		return collabList;
	}

	public TableDataProvider getList() {
		FacesContext fc = FacesContext.getCurrentInstance();
		Map sessionMap = fc.getExternalContext().getSessionMap();
		mLogger.log(Level.INFO,mLoc.loc("INFO210: session map: {0}",sessionMap));
		provider = new ObjectListDataProvider(list);
		return provider;
	}

	/**
	 * fetch data from the summary table of a given collaboration based on the
	 * filter date
	 * 
	 * @param collab
	 *            name of the collab
	 * @param filter
	 *            filter date on whose basis the data is selected
	 */
	public void fetchData(String collab, FilterDate date) {
		MonitorClient client = new MonitorClient();
		list = client.getSummary(collab, date).getList();
		summaryTotalData = client.getSummaryTotal(collab, date);
	}

	/**
	 * refresh the list of rows by fetching the data from the summary table of the given
	 * collab
	 * 
	 * @param collab
	 *            name of the collab
	 */
	public void refresh(String collab) {
		fetchData(collab, null);
	}

	
	public void purge(String collab, Date olderThanDate,boolean purgeAll){
		MonitorClient client = new MonitorClient();
		client.purge(collab,olderThanDate,purgeAll);
		refresh(collab);
	}
	/**
	 * 
	 * @return SummaryTotalData
	 */
	public SummaryTotalData getSummaryTotalData() {
		return summaryTotalData;
	}

	public void setSummaryTotalData(SummaryTotalData summaryTotalData) {
		this.summaryTotalData = summaryTotalData;
	}

	public String details() {
		return "details";
	}

}
