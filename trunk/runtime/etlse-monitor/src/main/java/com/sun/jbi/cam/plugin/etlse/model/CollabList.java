/*
 * CollabList.java
 *
 * Created on November 10, 2006, 3:47 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;

import com.sun.data.provider.TableDataProvider;
import com.sun.data.provider.impl.ObjectListDataProvider;
import com.sun.jbi.cam.plugin.etlse.jmx.MonitorClient;

/**
 * 
 * Backing bean reponsible to returning the list of mbeans or collabs active on
 * the ETLSE
 * 
 * @author Sujit Biswas
 */
public class CollabList {

	private static Logger logger = Logger.getLogger(CollabList.class.getName());

	private List mbeans;
	private String collab;

	private TableDataProvider provider;

	/**
	 * Creates a new instance of CollabList
	 */
	public CollabList() {
	}

	/**
	 * 
	 * @return TableDataProvider for the mbean list
	 */
	public TableDataProvider getCollabNames() {

		provider = new ObjectListDataProvider(getMbeans());
		return provider;
	}

	@SuppressWarnings("unchecked")
	public List getMbeans() {
		MonitorClient c = new MonitorClient();
		mbeans = c.getCollabList();
		return mbeans;
	}

	public void setMbeans(List l) {
		this.mbeans = l;
	}

	public String summary() {
		FacesContext c = FacesContext.getCurrentInstance();
		ExternalContext ec = c.getExternalContext();

		Map params = ec.getRequestParameterMap();
		String collab = (String) params.get("collab");
		setCurrentCollab(collab);

		Map session = ec.getSessionMap();

		logger.fine("session map: " + session.toString());

		CollabSummaryList collabSummaryList = (CollabSummaryList) session.get("MbeanSummaryList");

		// this happens when a user goes back and forth from the summary page(
		// of a given collab) to the collab-list page and once again comes back
		// to the summary page to view a selected collab
		if (collabSummaryList != null)
			collabSummaryList.refresh(collab);

		return "summary";
	}

	/**
	 * @return the current collab whose summary data is being viewed by the user
	 */
	public String getCurrentCollab() {
		return collab;
	}

	/**
	 * @param collab
	 *            the collab to set
	 */
	public void setCurrentCollab(String collab) {
		this.collab = collab;
	}

}
