/*
 * CollabSummary.java
 *
 * Created on November 10, 2006, 6:03 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 
 * Backing bean which represents a row in the summary table for a given collab 
 * 
 * @author Sujit Biswas
 */
public class CollabSummary {

	private Integer executionID = 10;
	private String targetTable = "";
	private String startDate;
	private String endDate;
	private Integer extracted = 15;
	private Integer loaded = 12;
	private Integer rejected = 3;
	private String exceptionMsg;

	/**
	 * Creates a new instance of CollabSummary
	 */
	public CollabSummary() {
	}

	public Integer getExecutionID() {
		return executionID;
	}

	public void setExecutionID(Integer executionID) {
		this.executionID = executionID;
	}

	public String getTargetTable() {
		return targetTable;
	}

	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public Integer getExtracted() {
		return extracted;
	}

	public void setExtracted(Integer extracted) {
		this.extracted = extracted;
	}

	public Integer getLoaded() {
		return loaded;
	}

	public void setLoaded(Integer loaded) {
		this.loaded = loaded;
	}

	public Integer getRejected() {
		return rejected;
	}

	public void setRejected(Integer rejected) {
		this.rejected = rejected;
	}

	public String getExceptionMsg() {
		return exceptionMsg;
	}

	public void setExceptionMsg(String exceptionMsg) {
		this.exceptionMsg = exceptionMsg;
	}

	enum column {
		EXECUTIONID, TARGETTABLE, STARTDATE, ENDDATE, EXTRACTED, LOADED, REJECTED, EXCEPTION_MSG
	}

	/**
	 * unmarshall the xml element in to this java object
	 * @param row
	 */
	public void unmarshall(Node row) {
		row.normalize();
		NodeList columns = row.getChildNodes();
		for (int i = 0; i < columns.getLength(); i++) {
			Node col = columns.item(i);
			if (col.getNodeType() == Node.TEXT_NODE) {
				continue;
			}
			NamedNodeMap attr = col.getAttributes();
			updateAttribute(attr);

		}
	}

	/**
	 * <row position = "1" > <data-column position = "0" value = "1" />
	 * <data-column position = "1" value = "T1_CSV_INPUT_TARGET" /> <data-column
	 * position = "2" value = "2006-11-24 12:39:46.890" /> <data-column position =
	 * "3" value = "2006-11-24 12:39:49.156" /> <data-column position = "4"
	 * value = "10" /> <data-column position = "5" value = "10" /> <data-column
	 * position = "6" value = "0" /> <data-column position = "7" value = "NULL" />
	 * </row> @param attr
	 */
	private void updateAttribute(NamedNodeMap attr) {
		Node p1 = attr.getNamedItem("position");
		Node p2 = attr.getNamedItem("value");

		int position = Integer.valueOf(p1.getNodeValue());
		String value = p2.getNodeValue();

		value.toString();

		switch (position) {
		case 0:
			this.executionID = Integer.valueOf(value);
			break;

		case 1:
			this.targetTable = value;
			break;

		case 2:
			this.startDate = value;
			break;

		case 3:
			this.endDate = value;
			break;

		case 4:
			try {
				this.extracted = Integer.valueOf(value);
			} catch (NumberFormatException e) {
			}
			break;

		case 5:
			try {
				this.loaded = Integer.valueOf(value);
			} catch (NumberFormatException e) {
			}
			break;

		case 6:
			try {
				this.rejected = Integer.valueOf(value);
			} catch (NumberFormatException e) {
			}
			break;

		case 7:
			this.exceptionMsg = value;
			break;

		}

	}

}
