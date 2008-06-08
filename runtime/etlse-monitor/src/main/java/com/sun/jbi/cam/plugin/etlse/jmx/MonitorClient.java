package com.sun.jbi.cam.plugin.etlse.jmx;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.cam.plugin.etlse.model.Localizer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import com.sun.jbi.cam.plugin.etlse.jmx.data.SummaryDataList;
import com.sun.jbi.cam.plugin.etlse.jmx.data.SummaryTotalData;
import com.sun.jbi.cam.plugin.etlse.model.Collab;
import com.sun.jbi.cam.plugin.etlse.model.FilterDate;

/**
 * @author Sujit Biswas
 * 
 */
public class MonitorClient {

	private static final String JAVA_LANG_INTEGER = "java.lang.Integer";
	private static final String JAVA_LANG_STRING = "java.lang.String";
	private static transient final Logger mLogger = Logger.getLogger(MonitorClient.class.getName());
    private static transient final Localizer mLoc = Localizer.get();
	protected static final String DEFAULT = "default";
	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");

	protected Object invokeMonitorMethod(String collabName, String method, Object[] param,
			String[] sig) throws Exception {
		ObjectName objName = MBeanUtil.getETLMBeanObjectName(collabName);
		MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
		return mBeanServer.invoke(objName, method, param, sig);

	}

	protected String getStatus(String collabName) {
		String ret = "";
		try {
			ObjectName objName = MBeanUtil.getETLMBeanObjectName(collabName);
			MBeanServerConnection mBeanServer = MBeanUtil.getMBeanServer();
			String tStatus = (String) mBeanServer.invoke(objName,
					ETLMonitorServerConstants.GET_STATUS, null, null);
			if (tStatus != null) {
				ret = tStatus;
			}
		} catch (Exception ex) {
			mLogger.log(Level.INFO,mLoc.loc("INFO203: MonitorClient.getStatus()",ex));
		}

		return ret;
	}

	public SummaryTotalData getSummaryTotal(String collabName, FilterDate filter) {

		Map returnMap = null;
		String exceptionStr = null;

		SummaryTotalData summaryTotalData = new SummaryTotalData();

		try {
			String whereCondition = whereCondition(filter);

			// Get the Summary Totals
			Object[] totalParam = new Object[] { whereCondition };
			String[] totalParamTypes = new String[] { JAVA_LANG_STRING };

			returnMap = (Map) invokeMonitorMethod(collabName,
					ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY, totalParam,
					totalParamTypes);

			exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

			summaryTotalData.setException(exceptionStr);
			if (exceptionStr == null) {
				String temp = (String) returnMap
						.get(ETLMonitorServerConstants.EXECUTE_SUMMARY_TOTAL_QUERY_RESULT);
				summaryTotalData.unmarshal(temp);

			}
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO204: getSummaryTotal()",e));
		}

		return summaryTotalData;
	}

	/**
	 * @param filter
	 * @return whereCondition based on start and end date
	 */
	private String whereCondition(FilterDate filter) {

		String whereCondition = null;
		String startDate = null;
		String endDate = null;
		if (filter != null) {

			if (filter.getStartDate() != null)
				startDate = simpleDateFormat.format(filter.getStartDate());
			if (filter.getEndDate() != null)
				endDate = simpleDateFormat.format(filter.getEndDate());

		}
		whereCondition = ETLMonitorServerConstants.getWhereConditionWithStartAndEndDate(startDate,
				endDate);
		return whereCondition;
	}

	public SummaryDataList getSummary(String collabName, FilterDate filter) {

		Integer offset = 0;
		Integer limit = null;
		String whereCondition = null;
		Map returnMap = null;
		String exceptionStr = null;
		String summaryData = null;
		SummaryDataList sdl = new SummaryDataList();

		whereCondition = whereCondition(filter);

		Object[] params = new Object[] { whereCondition, limit, offset };
		String[] paramTypes = new String[] { JAVA_LANG_STRING, JAVA_LANG_INTEGER,
				JAVA_LANG_INTEGER };

		try {
			returnMap = (Map) invokeMonitorMethod(collabName,
					ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY, params, paramTypes);
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO205: getSummary()",e));
		}

		exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);

		if (exceptionStr == null) {
			summaryData = (String) returnMap
					.get(ETLMonitorServerConstants.EXECUTE_SUMMARY_QUERY_RESULT);
		}

		sdl.unmarshall(summaryData);
		return sdl;

	}

	@SuppressWarnings("unchecked")
	public List getCollabList() {
		Set s = null;
		try {
			s = MBeanUtil.getETLEngineMbeanList();
		} catch (Exception e) {
			mLogger.log(Level.INFO,mLoc.loc("INFO206: getCollabList()",e));
		}

		List l = new ArrayList();
		Iterator iter = s.iterator();
		while (iter.hasNext()) {
			ObjectName element = ((ObjectInstance) iter.next()).getObjectName();

			String name = element.getKeyProperty("collab");

			l.add(new Collab(name, element.getCanonicalName()));
		}
		return l;
	}

	public void purge(String collab, Date olderThanDate, boolean purgeAll) {
		String older_than_date = null;

		if (olderThanDate != null) {
			older_than_date = simpleDateFormat.format(olderThanDate);
		}

		if (purgeAll) {
			try {
				Map returnMap = (Map) invokeMonitorMethod(collab,
						ETLMonitorServerConstants.TRUNCATE_RECORDS, null, null);

				String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
			} catch (Exception e) {
				mLogger.log(Level.INFO,mLoc.loc("INFO503: MonitorClient.purge()",e));
			}

		} else {

			try {
				Object[] deleteParams = new Object[] { older_than_date };
				String[] deleteParamTypes = new String[] { JAVA_LANG_STRING };
				Map returnMap = (Map) invokeMonitorMethod(collab,
						ETLMonitorServerConstants.DELETE_RECORDS, deleteParams, deleteParamTypes);

				String exceptionStr = (String) returnMap.get(ETLMonitorServerConstants.EXECEPTION);
			} catch (Exception e) {
				mLogger.log(Level.INFO,mLoc.loc("INFO503: MonitorClient.purge()",e));
			}

		}

	}

	public static void main(String[] args) {
		try {
			String collab = "xxx";
			FilterDate fd = new FilterDate();

			Calendar cal = Calendar.getInstance();

			cal.set(2001, 03, 03);
			fd.setStartDate(cal.getTime());
			// fd.setEndDate(new Date());
			MonitorClient c = new MonitorClient();
			SummaryTotalData std = c.getSummaryTotal(collab, fd);

			SummaryDataList sdl = c.getSummary(collab, fd);
			System.out.println(std);
			System.out.println(sdl);
			System.out.println(c.getCollabList());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
