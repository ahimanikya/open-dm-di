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
 * @(#)EtlMapEntry.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.io.File;
import java.util.logging.Logger;
import javax.jbi.servicedesc.ServiceEndpoint;
import javax.management.ObjectName;
import javax.wsdl.Definition;
import javax.wsdl.Operation;
import javax.wsdl.Output;
import javax.wsdl.PortType;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;

import org.w3c.dom.Document;
import com.sun.jbi.engine.etl.Localizer;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.impl.ETLEngineImpl;
import com.sun.jbi.eManager.provider.EndpointStatus;

public class EtlMapEntry {
	public static final String REQUEST_REPLY_SERVICE = "requestReplyService";

	public static final String FILTER_ONE_WAY = "filterOneWay";

	public static final String FILTER_REQUEST_REPLY = "filterRequestReply";

	private String mServiceUnitName;

	private String mType;

	private QName mPartnerLink;

	private QName mPortType;

	private QName mOperation;

	private QName mService;

	private String mFile;

	private ETLEngine mETLEngine;

	private String mEndpointStatusId;

	private EndpointStatus mEndpointStatus;

	private QName mOutPartnerLink;

	private QName mOutPortType;

	private QName mOutOperation;

	private QName mOutService;

	private String mReplyFile;

	private Transformer mReplyTransformer;

	private String mOutEndpointStatusId;

	private EndpointStatus mOutEndpointStatus;

	private boolean mStarted;

	private ServiceEndpoint mServiceEndpoint;

	private Definition mWsdl;

	private Output mOutput;

	private ObjectName mbeanObjectName;

	private static transient final Logger mLogger = Logger.getLogger(EtlMapEntry.class.getName());

    private static transient final Localizer mLoc = Localizer.get();

	/**
	 * Creates a new instance of PortMapEntry
	 */
	private EtlMapEntry(String serviceUnitName, 
						String type, 
						QName partnerLink,
						QName portType, 
						QName operation, 
						String file, 
						QName outPartnerLink,
						QName outPortType, 
						QName outOperation, 
						String replyFile,
						Definition wsdl) 
	{
		mServiceUnitName = serviceUnitName;
		mType = type;
		mPartnerLink = partnerLink;
		mPortType = portType;
		mOperation = operation;
		mFile = file;
		//TODO createETLengine(); should create the etlengine instance only once

		mOutPartnerLink = outPartnerLink;
		mOutPortType = outPortType;
		mOutOperation = outOperation;
		mReplyFile = replyFile;
		mWsdl = wsdl;
		if (wsdl != null) {
			try {
				PortType pType = wsdl.getPortType(portType);
				Operation outop = pType.getOperation(operation.toString(),
						null, null);
				mOutput = outop.getOutput();
			} catch (Exception ex) {
				// failed to get out messages...
				mOutput = null;
			}
		}
	}

	

	public static EtlMapEntry newRequestReplyService(
			String serviceUnitName,
			QName partnerLink, 
			QName portType, 
			QName operation, 
			String file,
			Definition wsdl) {
		return new EtlMapEntry(serviceUnitName, REQUEST_REPLY_SERVICE,
				partnerLink, portType, operation, file, null, null, null, null,
				wsdl);
	}

	public static EtlMapEntry newFilterOneWay(String serviceUnitName,
			QName partnerLink, QName portType, QName operation, String file,
			QName outPartnerLink, QName outPortType, QName outOperation,
			Definition wsdl) {
		return new EtlMapEntry(serviceUnitName, FILTER_ONE_WAY, partnerLink,
				portType, operation, file, outPartnerLink, outPortType,
				outOperation, null, wsdl);
	}

	public static EtlMapEntry newFilterRequestReply(String serviceUnitName,
			QName partnerLink, QName portType, QName operation, String file,
			QName outPartnerLink, QName outPortType, QName outOperation,
			String replyFile, Definition wsdl) {
		return new EtlMapEntry(serviceUnitName, FILTER_REQUEST_REPLY,
				partnerLink, portType, operation, file, outPartnerLink,
				outPortType, outOperation, replyFile, wsdl);
	}
	
	/**
	 * This always returns new instance of ETL engine 
	 */
	private void createETLengine() {
		mETLEngine = new ETLEngineImpl();
		mETLEngine.setContext(new ETLEngineContext());
		try {
			DocumentBuilder builder = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder();
			Document doc = builder.parse(new File(mFile));
			mETLEngine.parseXML(doc.getDocumentElement());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}



	

	public String getServiceUnitName() {
		return mServiceUnitName;
	}

	public String getType() {
		assert mType != null;
		return mType;
	}

	public QName getPartnerLink() {
		assert mPartnerLink != null;
		return mPartnerLink;
	}

	public QName getPortType() {
		return mPortType;
	}

	public QName getOperation() {
		return mOperation;
	}

	public void setService(QName service) {
		assert service != null;
		assert mService == null; // can only set once
		mService = service;
	}

	public QName getService() {
		return mService;
	}

	public String getFile() {
		assert mFile != null;
		return mFile;
	}

	public synchronized ETLEngine getETLEngine() {
		createETLengine();
		return mETLEngine;
	}

	public QName getOutPartnerLink() {
		return mOutPartnerLink;
	}

	public QName getOutPortType() {
		return mOutPortType;
	}

	public QName getOutOperation() {
		return mOutOperation;
	}

	public void setOutService(QName outService) {
		assert outService != null;
		assert mOutService == null; // can only set once
		mOutService = outService;
	}

	public QName getOutService() {
		return mOutService;
	}

	public boolean hasReplyFile() {
		return mReplyFile != null;
	}

	public String getReplyFile() {
		assert mReplyFile != null;
		return mFile;
	}

	public boolean hasReplyTransformer() {
		return mReplyTransformer != null;
	}

	public Transformer getReplyTransformer() {
		return mReplyTransformer;
	}

	public void setStarted(boolean started) {
		mStarted = started;
	}

	public boolean isStarted() {
		return mStarted;
	}

	public void setServiceEndpoint(ServiceEndpoint endpointRef) {
		mServiceEndpoint = endpointRef;
	}

	public ServiceEndpoint getServiceEndpoint() {
		return mServiceEndpoint;
	}

	public void setEndpointStatus(String endpointStatusId,
			EndpointStatus endpointStatus) {
		mEndpointStatusId = endpointStatusId;
		mEndpointStatus = endpointStatus;
	}

	public String getEndpointStatusId() {
		return mEndpointStatusId;
	}

	public EndpointStatus getEndpointStatus() {
		return mEndpointStatus;
	}

	public void setOutEndpointStatus(String outEndpointStatusId,
			EndpointStatus outEndpointStatus) {
		mOutEndpointStatusId = outEndpointStatusId;
		mOutEndpointStatus = outEndpointStatus;
	}

	public String getOutEndpointStatusId() {
		return mOutEndpointStatusId;
	}

	public EndpointStatus getOutEndpointStatus() {
		return mOutEndpointStatus;
	}

	public Definition getWsdl() {
		return mWsdl;
	}

	public void setWsdl(Definition wsdl) {
		this.mWsdl = wsdl;
	}

	public Output getOutput() {
		return mOutput;
	}

	public void setOutput(Output output) {
		this.mOutput = output;
	}



	public void setMbeanObjectName(ObjectName objName) {
		this.mbeanObjectName= objName;
		
	}



	public ObjectName getMbeanObjectName() {
		return mbeanObjectName;
	}
}
