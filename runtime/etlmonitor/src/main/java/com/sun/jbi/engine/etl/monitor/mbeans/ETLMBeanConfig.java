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
 * @(#)ETLMBeanConfig.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.mbeans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.management.MBeanException;
import javax.management.ObjectName;

import org.w3c.dom.Document;
import org.w3c.dom.Element;


/**
 * @author Ritesh Adval
 * @version 
 */
public class ETLMBeanConfig  implements Serializable {

    private static final String ATTR_APPLICATION_NAME = "applicationName";

    private static final String ATTR_COLLAB_NAME = "collabName";

    private static final String ATTR_COLLABORATION_TYPE = "collaborationType";

    private static final String ATTR_DEPLOYMENT_NAME = "deploymentName";

    private static final String ATTR_DETAILS_TABLE = "detailsTable";

    private static final String ATTR_LOG_FOLDER = "logFolder";

    private static final String ATTR_OID = "oid";

    private static final String ATTR_PROJECT_NAME = "projectName";

    private static final String ATTR_SUMMARY_TABLE = "summaryTable";

    private static final String ATTR_UNCONVERTED_COPLLAB_NAME = "unConvertedCollabName";

    private static final String ATTR_UNCONVERTED_PROJECT_NAME = "unConvertedProjectName";
    
    private static final String ELEM_INBOUND_CONNECTOR = "inboundConnector";
    
    private static final String ATTR_CONNECTOR_NAME = "connectorName" ;

    private String mApplicationName = null;

    private String mCollabName = null;

    private String mCollaborationType = null;

    private String mDeployable = null;

    private String mDeploymentName = null;

    private String mDetailsTable = null;

    private String mLogFolder = null;

    private String mOid = null;

    private String mProjectName = null;

    private String mSummaryTable = null;

    private String mUnConvertedCollabName = null;

    private String mUnConvertedProjName = null;
    
    private List mInboundConnectorNameStrs = null;
    
    private ObjectName mObjectName = null;

	private String name;

    public String getApplicationName() {
        return mApplicationName;
    }

    public String getCollabName() {
        return mCollabName;
    }

    public String getCollaborationType() {
        return mCollaborationType;
    }

    public String getDeployable() {
        return mDeployable;
    }

    public String getDeploymentName() {
        return mDeploymentName;
    }

    public String getDetailsTableName() {
        return this.mDetailsTable;
    }

    public String getLogFolder() {
        return this.mLogFolder;
    }

    public String getObjectNameStr() throws MBeanException {
        if (mObjectName == null) {
            try {
                mObjectName = null;
            } catch (Exception e) {
                throw new MBeanException(e, "Fail to obtain ObjectName:");
            }
        }
        return mObjectName.toString();
    }

    public String getOid() {
        return this.mOid;
    }

    public String getProjectName() {
        return mProjectName;
    }

    public String getSummaryTableName() {
        return this.mSummaryTable;
    }

    public String getUnConvertedCollabName() {
        return mUnConvertedCollabName;
    }

    public String getUnConvertedProjName() {
        return mUnConvertedProjName;
    }

    public List getInboundConnectorsNameStrs(){
    	return mInboundConnectorNameStrs;
    }
    
    public void setApplicationName(String applicationName) {
        mApplicationName = applicationName;
    }

    public void setCollabName(String collabName) {
        mCollabName = collabName;
    }

    public void setCollaborationType(String collaborationType) {
        mCollaborationType = collaborationType;
    }

    public void setDeployable(String deployable) {
        mDeployable = deployable;
    }

    public void setDeploymentName(String deploymentName) {
        mDeploymentName = deploymentName;
    }

    public void setDetailsTableName(String detailsTable) {
        this.mDetailsTable = detailsTable;
    }

    public void setLogFolder(String lFolder) {
        this.mLogFolder = lFolder;
    }

    public void setOid(String nOid) {
        this.mOid = nOid;
    }

    public void setProjectName(String projectName) {
        mProjectName = projectName;
    }

    public void setSummaryTableName(String summaryTable) {
        this.mSummaryTable = summaryTable;
    }

    public void setUnConvertedCollabName(String unConvertedCollabName) {
        mUnConvertedCollabName = unConvertedCollabName;
    }

    public void setUnConvertedProjName(String unConvertedProjName) {
        mUnConvertedProjName = unConvertedProjName;
    }

    public void setInboundConnectorsNameStrs(List newNameStrs){
    	mInboundConnectorNameStrs = newNameStrs;
    }
    

    public boolean isInboundConnectorStartStoppable(){
    	boolean ret = false;
    	if ((mInboundConnectorNameStrs != null) && (mInboundConnectorNameStrs.size() > 0)){
    		ret = true;
    	}
    	
    	return ret;
    }
    
    // Base class owned by codegen-framework expects Vector
    protected Vector additionalElements(Document doc) throws Exception{
        Vector childElements = new Vector(); 
        

        Element inboundConnectorNameElement = null;
        String inboundConnectorName = null;
        Iterator itr = null; 
        if (mInboundConnectorNameStrs != null && mInboundConnectorNameStrs.size() > 0)
        {
        	itr = mInboundConnectorNameStrs.iterator();
        	while ( itr.hasNext()) {
                inboundConnectorName = (String) itr.next();
                inboundConnectorNameElement = doc.createElement(ELEM_INBOUND_CONNECTOR);
                inboundConnectorNameElement.setAttribute(ATTR_CONNECTOR_NAME, inboundConnectorName);
                childElements.add(inboundConnectorNameElement);
            }
        }
       return childElements;
    }
    
    protected HashMap additionalAttributes() throws Exception {
        HashMap map = new HashMap();
      
        map.put(ATTR_COLLAB_NAME, mCollabName);
        map.put(ATTR_PROJECT_NAME, mProjectName);
        map.put(ATTR_DEPLOYMENT_NAME, mDeploymentName);
        map.put(ATTR_APPLICATION_NAME, mApplicationName);
        map.put(ATTR_UNCONVERTED_PROJECT_NAME, mUnConvertedProjName);
        map.put(ATTR_UNCONVERTED_COPLLAB_NAME, mUnConvertedCollabName);
        map.put(ATTR_COLLABORATION_TYPE, mCollaborationType);
        map.put(ATTR_OID, mOid);
        map.put(ATTR_LOG_FOLDER, mLogFolder);
        map.put(ATTR_SUMMARY_TABLE, mSummaryTable);
        map.put(ATTR_DETAILS_TABLE, mDetailsTable);
        return map;
    }

    protected void additionalParse(Element xmlElement) throws XMLDocumentException {
        
        //((ETLMBeanLoader) mService).setName(mName);
        
        mCollabName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_COLLAB_NAME, true);
        mProjectName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_PROJECT_NAME, true);
        mDeploymentName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_DEPLOYMENT_NAME, true);
        mApplicationName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_APPLICATION_NAME, true);
        mUnConvertedProjName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_UNCONVERTED_PROJECT_NAME, true);
        mUnConvertedCollabName = XMLDocumentUtils.getAttribute(xmlElement, ATTR_UNCONVERTED_COPLLAB_NAME, true);
        mCollaborationType = XMLDocumentUtils.getAttribute(xmlElement, ATTR_COLLABORATION_TYPE, true);
        mOid = XMLDocumentUtils.getAttribute(xmlElement, ATTR_OID, true);
        mLogFolder = XMLDocumentUtils.getAttribute(xmlElement, ATTR_LOG_FOLDER, true);
        mSummaryTable = XMLDocumentUtils.getAttribute(xmlElement, ATTR_SUMMARY_TABLE, true);
        mDetailsTable = XMLDocumentUtils.getAttribute(xmlElement, ATTR_DETAILS_TABLE, true);
        
        //mInboundObjectNames
        Vector children = XMLDocumentUtils.getChildren(xmlElement, ELEM_INBOUND_CONNECTOR);
        String inboundConnetorName = null;
        Element inboundConnectorElem = null; 

        if (children != null && children.size() > 0){
        	
            if (mInboundConnectorNameStrs == null){
            	mInboundConnectorNameStrs = new Vector ();
            }
        	
            Iterator it = children.iterator(); 
            while(it.hasNext()){
               inboundConnectorElem = (Element) it.next();
               inboundConnetorName = XMLDocumentUtils.getAttribute(inboundConnectorElem, ATTR_CONNECTOR_NAME, true);
               mInboundConnectorNameStrs.add(inboundConnetorName);
           }
        }
        
    }

	public String getName() {
		// TODO Auto-generated method stub
		return name;
	}
}
