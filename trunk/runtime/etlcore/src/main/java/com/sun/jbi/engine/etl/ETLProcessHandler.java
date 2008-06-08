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
 * @(#)ETLProcessHandler.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineListener;
import com.sun.etl.engine.ETLEngineContext.CollabStatistics;
import com.sun.jbi.engine.etl.monitor.ETLMonitorHelper;
import com.sun.jbi.engine.etl.Localizer;


/**
 * 
 * This class is responsible for starting the etl engine process, and retrieving
 * the results once the engine completes processing or get an exception
 * 
 * @author Sujit Biswas
 * 
 */
public class ETLProcessHandler {

	private static transient final Logger mLogger = Logger.getLogger(ETLProcessHandler.class.getName());

    private static transient final Localizer mLoc = Localizer.get();

	private Properties envProps;

	private EtlEngineOutputParams outputParams;
        
	public ETLProcessHandler(Properties envProps) {
		this.envProps = envProps;
	}

	public String startProcess(ETLEngine etlEngine) {

		//ETLEngine etlEngine = etlMapEntry.getETLEngine();
		ETLEngineListener lsnr = new ETLSEListener(etlEngine);
		mLogger.log(Level.INFO,mLoc.loc("INFO002: starting ETL engine process"));
		etlEngine.setRunningOnAppServer(true);
		//registerEtlMonitorMbean(etlEngine);
		etlEngine.exec(lsnr);

		synchronized (lsnr) {
			try {
				lsnr.wait();
			} catch (InterruptedException e) {
				mLogger.log(Level.SEVERE,mLoc.loc("ERRO686: ErrorMsg-{0}",e.getMessage()), e);
				e.printStackTrace();
			}
		}
                
		setOutputParams(etlEngine);
		StringBuffer sb = createOutputMessageString(etlEngine);
                mLogger.log(Level.INFO,mLoc.loc("INFO0040: response: " + sb.toString()));
		return sb.toString();
	}

	private void setOutputParams(ETLEngine etlEngine) {

		ETLEngineContext context = etlEngine.getContext();
		String status = SUCCESS;
		outputParams = new EtlEngineOutputParams();
		List exList = context.getThrowableList();

		// TODO the status impl needs to changes, should be fetching the status
		// value from the ETL engine listener
		if (!exList.isEmpty()) {
			status = FAILURE;
		} 

		CollabStatistics stats = context.getStatistics();

		long starttime = stats.getCollabStartTime().getTime();
		long endtime = stats.getCollabFinishTime().getTime();

		Map outputArgs = etlEngine.getRuntimeOutputArguments();

		Iterator it = outputArgs.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			if (key.equalsIgnoreCase(STATUS)) {
				mLogger.log(Level.INFO,mLoc.loc("INFO303: Setting output value: {0} : {1} ",key,status));
				outputParams.setStatus(status);
			} else if (key.equalsIgnoreCase(START_TIME)) {
				mLogger.log(Level.INFO,mLoc.loc("INFO303: Setting output value: {0} : {1} ",key,getTimeStampString(starttime)));
				outputParams.setStartTime(getTimeStampString(starttime));
			} else if (key.equalsIgnoreCase(END_TIME)) {
				mLogger.log(Level.INFO,mLoc.loc("INFO303: Setting output value: {0} : {1} ",key,getTimeStampString(endtime)));
				outputParams.setEndTime(getTimeStampString(endtime));
			} else if (key.startsWith(COUNT_PREFIX)) {
				String tableName = key.substring(COUNT_PREFIX.length());
				if (context != null) {
					Long ct = new Long(context.getStatistics()
							.getRowsInsertedCount(tableName));
					mLogger.log(Level.INFO,mLoc.loc("INFO306: Setting output value: {0} : {1}",key,ct));
					outputParams.setRowsInserted(ct);
				}
			}
		}

	}

	private String getTimeStampString(long time) {
		Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
		calendar.setTimeInMillis(time);
		StringBuffer buffer = new StringBuffer();
		buffer.append(calendar.get(Calendar.YEAR));
		buffer.append("-").append(twoDigit(calendar.get(Calendar.MONTH) + 1));
		buffer.append("-").append(twoDigit(calendar.get(Calendar.DAY_OF_MONTH)));
		buffer.append("T").append(twoDigit(calendar.get(Calendar.HOUR_OF_DAY)));
		buffer.append(":").append(twoDigit(calendar.get(Calendar.MINUTE)));
		buffer.append(":").append(twoDigit(calendar.get(Calendar.SECOND)));
		buffer.append(".").append(twoDigit(calendar.get(Calendar.MILLISECOND) / 10));
		buffer.append("Z");
		return buffer.toString();
	}

	private static String twoDigit(int i) {
		if ((i >= 0) && (i < 10)) {
			return "0" + String.valueOf(i);
		}
		return String.valueOf(i);
	}

	/**
	 * @param etlEngine
	 * @return
	 */
	public StringBuffer createOutputMessageString(ETLEngine engine) {
		StringBuffer sb = new StringBuffer();
		// TODO change the prefix and suffix according to the format of the
		// output message in the etl.wsdl file
                
		String prefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <execute><" +
                        engine.getDisplayName() + "_engine_outputItem xmlns=\"http://com.sun.jbi/etl/etlengine\">";
		String suffix = "</" + engine.getDisplayName() + "_engine_outputItem></execute>";

		sb.append(prefix);

		sb.append("<status>" + outputParams.getStatus() + "</status>");
		sb.append("<startTime>" + outputParams.getStartTime() + "</startTime>");
		sb.append("<endTime>" + outputParams.getEndTime() + "</endTime>");
		sb.append("<rowsInserted>" + outputParams.getRowsInserted() + "</rowsInserted>");

		sb.append(suffix);

		return sb;
	}

//	private void registerEtlMonitorMbean(ETLEngine etlEngine) {
//		ETLMonitorHelper helper = new ETLMonitorHelper();
//		helper.registerMbean(etlEngine);
//
//	}

	private static final String STATUS = "status";
	private static final String END_TIME = "endTime";
	private static final String START_TIME = "startTime";
	private static final String INSERTED_ROW_COUNT = "rowsInserted";
	private static final String COUNT_PREFIX = "Count_";
	public static final String FAILURE = "Failure";
	public static final String SUCCESS = "Success";

	private class EtlEngineOutputParams {
		private String status;
		private String startTime;
		private String endTime;
		private Long rowsInserted;

		public String getEndTime() {
			return endTime;
		}

		public void setEndTime(String endTime) {
			this.endTime = endTime;
		}

		public Long getRowsInserted() {
			return rowsInserted;
		}

		public void setRowsInserted(Long rowsInserted) {
			this.rowsInserted = rowsInserted;
		}

		public String getStartTime() {
			return startTime;
		}

		public void setStartTime(String startTime) {
			this.startTime = startTime;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}

		public String toXmlString() {
			return null;
		}
	}

}
