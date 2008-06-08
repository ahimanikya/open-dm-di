package com.sun.jbi.cam.plugin.etlse.jmx.data;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * This represent the aggregate data given a summary table, and the query made
 * on the table based on the filter date
 * 
 * @author Sujit Biswas
 * 
 */
public class SummaryTotalData {

	private int totalExtracted = 0;
	private int totalLoaded = 0;
	private int totalRejected = 0;
	private int averageExtracted = 0;
	private int averageLoaded = 0;
	private int averageRejected = 0;

	private String exception;

	public SummaryTotalData() {
		super();
	}

	public int getTotalExtracted() {
		return this.totalExtracted;
	}

	public void setTotalExtracted(int value) {
		this.totalExtracted = value;
	}

	public int getTotalLoaded() {
		return this.totalLoaded;
	}

	public void setTotalLoaded(int value) {
		this.totalLoaded = value;
	}

	public int getTotalRejected() {
		return this.totalRejected;
	}

	public void setTotalRejected(int value) {
		this.totalRejected = value;
	}

	public int getAverageExtracted() {
		return this.averageExtracted;
	}

	public void setAverageExtracted(int value) {
		this.averageExtracted = value;
	}

	public int getAverageLoaded() {
		return this.averageLoaded;
	}

	public void setAverageLoaded(int value) {
		this.averageLoaded = value;
	}

	public int getAverageRejected() {
		return this.averageRejected;
	}

	public void setAverageRejected(int value) {
		this.averageRejected = value;
	}

	private void init() {
		this.totalExtracted = 0;
		this.totalLoaded = 0;
		this.totalRejected = 0;
		this.averageExtracted = 0;
		this.averageLoaded = 0;
		this.averageRejected = 0;
	}

	private int parseIntFromStringList(int index, List strList) {
		int ret = 0;

		if ((index >= 0) && (strList.size() > index)) {
			try {
				ret = Integer.parseInt((String) strList.get(index));
			} catch (Exception ex) {
				// Log the exception
			}
		}

		return ret;
	}

	public void unmarshal(String data) {
		char delimiter = ',';
		this.init();
		if (data != null && data.trim().length() > 0) {

			ArrayList<String> strings = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(data, String.valueOf(delimiter));
			if (tok.hasMoreTokens()) {
				do {
					strings.add(tok.nextToken().trim());
				} while (tok.hasMoreTokens());
			}
			this.totalExtracted = parseIntFromStringList(0, strings);
			this.totalLoaded = parseIntFromStringList(1, strings);
			this.totalRejected = parseIntFromStringList(2, strings);
			this.averageExtracted = parseIntFromStringList(3, strings);
			this.averageLoaded = parseIntFromStringList(4, strings);
			this.averageRejected = parseIntFromStringList(5, strings);
		}
	}

	public String toString() {
		StringBuffer buff = new StringBuffer();
		buff.append(this.getTotalExtracted());
		buff.append(",");
		buff.append(this.getTotalLoaded());
		buff.append(",");
		buff.append(this.getTotalRejected());
		buff.append(",");
		buff.append(this.getAverageExtracted());
		buff.append(",");
		buff.append(this.getAverageLoaded());
		buff.append(",");
		buff.append(this.getAverageRejected());

		return buff.toString();
	}

	/**
	 * @return the exception
	 */
	public String getException() {
		return exception;
	}

	/**
	 * @param exception
	 *            the exception to set
	 */
	public void setException(String exception) {
		this.exception = exception;
	}

}
