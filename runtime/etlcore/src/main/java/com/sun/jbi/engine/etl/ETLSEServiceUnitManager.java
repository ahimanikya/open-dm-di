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
 * @(#)ETLSEServiceUnitManager.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */
package com.sun.jbi.engine.etl;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import com.sun.jbi.engine.etl.Localizer;
import com.sun.jbi.engine.etl.AlertsUtil;
import com.sun.jbi.alerter.NotificationEvent;
import java.util.logging.Level;
import javax.jbi.component.ComponentContext;
import javax.jbi.component.ServiceUnitManager;
import javax.jbi.management.DeploymentException;
import javax.jbi.messaging.DeliveryChannel;
import javax.jbi.servicedesc.ServiceEndpoint;
import javax.wsdl.Definition;
import javax.wsdl.PortType;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;
import javax.xml.namespace.QName;

import org.xml.sax.EntityResolver;

import com.ibm.wsdl.factory.WSDLFactoryImpl;
import com.sun.jbi.eManager.provider.EndpointStatus;
import com.sun.jbi.eManager.provider.StatusProviderHelper;
import com.sun.jbi.eManager.provider.StatusReporting;
import com.sun.jbi.engine.etl.mbean.ETLSERuntimeConfiguration;
import com.sun.jbi.engine.etl.mbean.ETLSERuntimeConfigurationMBean;
import com.sun.jbi.engine.etl.monitor.ETLMonitorHelper;
import com.sun.jbi.management.descriptor.SUDescriptorSupport;
import com.sun.jbi.management.message.DefaultJBITaskMessageBuilder;
import com.sun.jbi.management.message.JBITaskMessageBuilder;
import com.sun.jbi.management.descriptor.Provides;

public class ETLSEServiceUnitManager implements ServiceUnitManager {

    private static transient final Logger mLogger = Logger.getLogger(ETLSEServiceUnitManager.class.getName());
    private static transient final Localizer mLoc = Localizer.get();
    private static final String PARTNER_MYROLE = "myRole";
    private static final String PARTNER_PARTNERROLE = "partnerRole";
    private ETLSEComponent mComponent;
    private HashSet mDeployedId;
    private ComponentContext mContext;
    private DeliveryChannel mChannel;
    private StatusProviderHelper mStatusProviderHelper;
    private EtlMapEntryTable mEtlMapEntryTable;
    private Hashtable mWsdlMap = null;
    private ETLSERuntimeConfiguration mRuntimeConfig;
    private ETLInOutQueue queue;

    /** Creates a new instance of ETLSEServiceUnitManager */
    public ETLSEServiceUnitManager(ETLSEComponent component) {
        mComponent = component;
    }

    private String createSuccessMessage(String taskName, String componentName) {
        JBITaskMessageBuilder msgBuilder = new DefaultJBITaskMessageBuilder();
        msgBuilder.setComponentName(componentName);
        String retMsg = msgBuilder.createSuccessMessage(taskName);

        return retMsg;
    }

    private String createExceptionMessage(String componentName, String taskName, String status,
            String locToken, String locParam, String locMessage, Throwable exObj) {
        JBITaskMessageBuilder msgBuilder = new DefaultJBITaskMessageBuilder();
        msgBuilder.setComponentName(componentName);
        String retMsg = msgBuilder.createExceptionMessage(taskName, locToken, locMessage, locParam,
                exObj);

        return retMsg;
    }

    void initialize(EtlMapEntryTable etlMapEntryTable, ComponentContext context,
            DeliveryChannel channel, StatusProviderHelper statusProviderHelper, Hashtable wsdlMap, ETLSERuntimeConfigurationMBean runtimeConfiguration) {
        mEtlMapEntryTable = etlMapEntryTable;
        mDeployedId = new HashSet();
        mContext = context;
        mChannel = channel;
        mStatusProviderHelper = statusProviderHelper;
        mWsdlMap = wsdlMap;
        mRuntimeConfig = (ETLSERuntimeConfiguration) runtimeConfiguration;
    }

    /**
     * Deploy a Service Unit to the component. This is called by the JBI
     * implementation in order to deploy the given artifact to the implementing
     * component.
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     * @param serviceUnitRootPath -
     *            the full path to the Service Unit artifact root directory
     */
    public String deploy(String serviceUnitName, String serviceUnitRootPath)
            throws DeploymentException {
        mLogger.log(Level.INFO, mLoc.loc("INFO555: Deploying service unit: {0} from {1} ", serviceUnitName, serviceUnitRootPath));
        String retMsg = createSuccessMessage("deploy", mContext.getComponentName());
        mLogger.log(Level.INFO, mLoc.loc("INFO556: Deploying service unit: {0} from {1} successfully", serviceUnitName, serviceUnitRootPath));
        return retMsg;
    }

    /**
     * Undeploy a Service Unit from the component. This is called by the JBI
     * implementation in order to undeploy the given Service Unit from the
     * implementing component. The deployment must be shut down before it can be
     * undeployed
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     * @param serviceUnitRootPath -
     *            the full path to the Service Unit artifact root directory
     */
    public String undeploy(String serviceUnitName, String serviceUnitRootPath)
            throws DeploymentException {
        mLogger.log(Level.INFO, mLoc.loc("INFO557: Undeploying service unit: {0} from {1}", serviceUnitName, serviceUnitRootPath));
        String retMsg = createSuccessMessage("undeploy", mContext.getComponentName());
        mLogger.log(Level.INFO, mLoc.loc("INFO558: Undeploying service unit: {0} from {1} successfully", serviceUnitName, serviceUnitRootPath));
        return retMsg;
    }

    /**
     * Initialize the deployment. This is the first phase of a two-phase start,
     * where the component must prepare to receive service requests related to
     * the deployment (if any).
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     * @param serviceUnitRootPath -
     *            the full path to the Service Unit artifact root directory
     */
    public void init(String serviceUnitName, String serviceUnitRootPath) throws DeploymentException {
        String taskName = "init";
        mLogger.log(Level.INFO, mLoc.loc("INFO059: Initializing service unit {0} serviceUnitRootPath: {1}", serviceUnitName, serviceUnitRootPath));
        if (mDeployedId.contains(serviceUnitName)) {
            String extMsg = createExceptionMessage(mContext.getComponentName(), taskName, "FAILED",
                    "EtlSum_Init_1", serviceUnitName, "Service Unit has already been deployed",
                    null);
            throw new DeploymentException(extMsg);
        }
        try {
            // load portmap.xml and etlmap.xml
            File deploydir = new File(serviceUnitRootPath);
            String[] files = deploydir.list();
            // String portmapfile = null;
            String etlmapfile = null;
            for (int i = 0; i < files.length; i++) {
                String filename = files[i];
                if (filename.endsWith("etlmap.xml")) {
                    etlmapfile = deploydir.getAbsolutePath() + File.separator + filename;
                } else if (filename.endsWith("portmap.xml")) {
                    // portmapfile = deploydir.getAbsolutePath() +
                    // File.separator + filename;
                }
            }

            // process wsdls and place them into wsdl map
            readAllDefinitions(deploydir, serviceUnitName, taskName, "ETLSE_Init_2");

            // load SU jbi.xml
            SUDescriptorSupport sud = new SUDescriptorSupport(serviceUnitRootPath);
            /*
             * Provides[] pds = sud.getProvides(); for (int i=0; i<pds.length;
             * i++) { mLogger.info("ETL Provide[" + i + " ]: " +
             * pds[i].getServiceName()+", "+pds[i].getEndpointName()); }
             */
            
            // Iterator portmaps = PortMapReader.parse(new File(portmapfile));
            Iterator portmaps = PortMapReader.parse(sud);
            EtlMapReader.parse(new File(etlmapfile), mEtlMapEntryTable, serviceUnitName, deploydir,
                    mWsdlMap);

            // bind portmap entry and etl entry using partnerLink
            while (portmaps.hasNext()) {
                PortMapEntry entry = (PortMapEntry) portmaps.next();
                QName serviceName = entry.getServiceName();
                QName partnerLink = entry.getPartnerLink();
                QName endpoint = entry.getEndPoint();
                if (entry.getRole().equalsIgnoreCase(PARTNER_MYROLE)) {
                    List list = mEtlMapEntryTable.getEntryList();
                    for (int i = 0, I = list.size(); i < I; i++) {
                        EtlMapEntry etlMapEntry = (EtlMapEntry) list.get(i);
                        QName etlPartnerLink = etlMapEntry.getPartnerLink();
                        if (partnerLink.equals(etlPartnerLink)) {
                            ServiceEndpoint serviceEndpoint = mContext.activateEndpoint(
                                    serviceName, endpoint.getLocalPart());

                            etlMapEntry.setService(serviceName);
                            etlMapEntry.setServiceEndpoint(serviceEndpoint);

                            StatusReporting reporting = mStatusProviderHelper.getStatusReporter();
                            String statusId = mStatusProviderHelper.createProvisioningEndpointIdentifier(serviceName, endpoint.getLocalPart());
                            reporting.addProvisioningEndpoint(statusId);
                            EndpointStatus endpointStatus = reporting.getEndpointStatus(statusId);
                            etlMapEntry.setEndpointStatus(statusId, endpointStatus);
                            mLogger.log(Level.INFO, mLoc.loc("INFO060: Activated etl ({0},{1},{2}) inbound service: {3}", etlMapEntry.getFile(), etlMapEntry.getPortType(), etlMapEntry.getOperation(), serviceName));
                        }
                    }
                    continue;
                }
                if (entry.getRole().equalsIgnoreCase(PARTNER_PARTNERROLE)) {
                    List list = mEtlMapEntryTable.getEntryList();
                    for (int i = 0, I = list.size(); i < I; i++) {
                        EtlMapEntry etlMapEntry = (EtlMapEntry) list.get(i);
                        QName etlOutPartnerLink = etlMapEntry.getOutPartnerLink(); // etlOutPartnerLink
                        // maybe
                        // null

                        if (partnerLink.equals(etlOutPartnerLink)) {
                            etlMapEntry.setOutService(serviceName);

                            StatusReporting reporting = mStatusProviderHelper.getStatusReporter();
                            String statusId = mStatusProviderHelper.createConsumingEndpointIdentifier(serviceName, endpoint.getLocalPart());
                            reporting.addConsumingEndpoint(statusId);
                            EndpointStatus outEndpointStatus = reporting.getEndpointStatus(statusId);
                            etlMapEntry.setOutEndpointStatus(statusId, outEndpointStatus);
                            mLogger.log(Level.INFO, mLoc.loc("INFO061: Set outbound service ({0},{1},{2},{3})", serviceName, etlMapEntry.getFile(), etlMapEntry.getOutPortType(), etlMapEntry.getOutOperation()));
                        }
                    }
                    continue;
                }
            }

            // Add serviceUnitName to lookup table
            mDeployedId.add(serviceUnitName);
        } catch (Exception e) {
            String extMsg = createExceptionMessage(mContext.getComponentName(), taskName, "FAILED",
                    "EtlSeSum_Init_2", serviceUnitName, "Service Unit init error", e);
            AlertsUtil.getAlerter().critical(extMsg,
                    ETLSELifeCycle.SHORT_DISPLAY_NAME,
                    serviceUnitName,
                    AlertsUtil.getServerType(),
                    AlertsUtil.COMPONENT_TYPE,
                    NotificationEvent.OPERATIONAL_STATE_RUNNING,
                    NotificationEvent.EVENT_TYPE_ALERT,
                    "ETLSE-E00101");
            throw new DeploymentException(extMsg, e);
        }
        this.queue.restore();
        mLogger.log(Level.INFO, mLoc.loc("INFO062: Initializing service unit {0} serviceUnitRootPath: {1} successfully.", serviceUnitName, serviceUnitRootPath));
    }
    
    public void setQueue(ETLInOutQueue queue) {
         this.queue = queue;
    }

    private void readAllDefinitions(File asaDir, String id, String taskName, String locToken)
            throws DeploymentException {
        /*
         * CatalogManager catalogManager = new CatalogManager();
         * catalogManager.setCatalogFiles(asaDir.getAbsolutePath() +
         * File.separator + "xml-catalog.xml");
         * catalogManager.setRelativeCatalogs(true); EntityResolver resolver =
         * new CatalogResolver(catalogManager);
         */
        EntityResolver resolver = null;
        List wsdls = listWSDLFiles(asaDir);
        File[] wsdlFiles = (File[]) wsdls.toArray(new File[0]);

        // read all wsdl files to see if
        if (wsdlFiles != null) {
            for (int i = 0; i < wsdlFiles.length; i++) {
                try {
                    Definition def = readWsdl(wsdlFiles[i], resolver);
                    QName key = getWsdlMapKey(def);
                    mWsdlMap.put(key, def);
                    mLogger.log(Level.INFO, mLoc.loc("INFO063: Added WSDL {0} , file: {1} ", def.getTargetNamespace(), wsdlFiles[i].getName()));
                } catch (Exception e) {
                    String msg = "Unable to read WSDL file " + wsdlFiles[i] + " : " + e.getMessage();
                    String exMsg = createExceptionMessage(mContext.getComponentName(), taskName,
                            "FAILED", locToken, id, msg, e);
                    AlertsUtil.getAlerter().critical(exMsg,
                            ETLSELifeCycle.SHORT_DISPLAY_NAME,
                            id,
                            AlertsUtil.getServerType(),
                            AlertsUtil.COMPONENT_TYPE,
                            NotificationEvent.OPERATIONAL_STATE_RUNNING,
                            NotificationEvent.EVENT_TYPE_ALERT,
                            "ETLSE-E00102");
                    throw new DeploymentException(exMsg, e);
                }
            }
        }
    }

    private QName getWsdlMapKey(Definition def) {
        QName key = null;
        Iterator iterator = def.getPortTypes().keySet().iterator();
        while (iterator.hasNext()) {
            QName element = (QName) iterator.next();
            PortType pt = def.getPortType(element);
            key = pt.getQName();
            break;
        }
        return key;
    }

    private List listWSDLFiles(File currentDir) {
        List cumulativeResults = new ArrayList();
        File[] filesInCurrentDir = currentDir.listFiles();
        for (int fileCount = 0; fileCount < filesInCurrentDir.length; fileCount++) {
            if (filesInCurrentDir[fileCount].isFile()) {
                if (filesInCurrentDir[fileCount].getName().toLowerCase().endsWith(".wsdl")) {
                    cumulativeResults.add(filesInCurrentDir[fileCount]);
                }
            } else if (filesInCurrentDir[fileCount].isDirectory()) {
                List wsdlsInSubDirectories = listWSDLFiles(filesInCurrentDir[fileCount]);
                cumulativeResults.addAll(wsdlsInSubDirectories);
            }
        }
        return cumulativeResults;
    }

    private Definition readWsdl(File f, EntityResolver resolver) throws javax.wsdl.WSDLException {
        WSDLFactory wsdlFactory = WSDLFactory.newInstance();
        WSDLReader reader = ((WSDLFactoryImpl) wsdlFactory).newWSDLReader(resolver);
        Definition def = reader.readWSDL(f.getAbsolutePath());

        return def;
    }

    /**
     * Shut down the deployment. This causes the deployment to return to the
     * state it was in after deploy() and before init().
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     */
    public void shutDown(String serviceUnitName) throws DeploymentException {
        String taskName = "init";
        mLogger.log(Level.INFO, mLoc.loc("INFO064: Shutting down service unit: {0}", serviceUnitName));
        if (!mDeployedId.contains(serviceUnitName)) {
            String extMsg = createExceptionMessage(mContext.getComponentName(), taskName, "FAILED",
                    "EtlSeSum_ShutDown_1", serviceUnitName, "Service Unit has not been deployed",
                    null);
            throw new DeploymentException(extMsg);
        }
        try {
            // Remove serviceUnitName from lookup table
            mDeployedId.remove(serviceUnitName);
            StatusReporting reporting = mStatusProviderHelper.getStatusReporter();
            List list = mEtlMapEntryTable.getEntryListByServiceUnitName(serviceUnitName);
            for (int i = 0,  I = list.size(); i < I; i++) {
                EtlMapEntry etlMapEntry = (EtlMapEntry) list.get(i);
                ServiceEndpoint serviceEndpoint = etlMapEntry.getServiceEndpoint();
                if (serviceEndpoint != null) {
                    mContext.deactivateEndpoint(serviceEndpoint);
                }
                String endpointStatusId = etlMapEntry.getEndpointStatusId();
                if (endpointStatusId != null) {
                    reporting.removeProvisioningEndpoints(new String[]{endpointStatusId                            });
                }
                String outEndpointStatusId = etlMapEntry.getOutEndpointStatusId();
                if (outEndpointStatusId != null) {
                    reporting.removeConsumingEndpoints(new String[]{outEndpointStatusId                            });
                }
                mEtlMapEntryTable.removeEntry(etlMapEntry);
            }
        } catch (Exception e) {
            String extMsg = createExceptionMessage(mContext.getComponentName(), taskName, "FAILED",
                    "EtlSeSum_ShutDown_2", serviceUnitName, "Service Unit shutDown error", e);
            AlertsUtil.getAlerter().critical(extMsg,
                    ETLSELifeCycle.SHORT_DISPLAY_NAME,
                    serviceUnitName,
                    AlertsUtil.getServerType(),
                    AlertsUtil.COMPONENT_TYPE,
                    NotificationEvent.OPERATIONAL_STATE_RUNNING,
                    NotificationEvent.EVENT_TYPE_ALERT,
                    "ETLSE-E00103");
            throw new DeploymentException(extMsg, e);
        }
        mLogger.log(Level.INFO, mLoc.loc("INFO065: Shutting down service unit: {0} successfully", serviceUnitName));
    }

    /**
     * Start the deployment. This is the second phase of a two-phase start,
     * where the component can now initiate service requests related to the
     * deployment.
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     */
    public void start(String serviceUnitName) throws DeploymentException {
        mLogger.log(Level.INFO, mLoc.loc("INFO066: Starting service unit: {0}", serviceUnitName));
        List list = mEtlMapEntryTable.getEntryListByServiceUnitName(serviceUnitName);
        for (int i = 0,  I = list.size(); i < I; i++) {
            EtlMapEntry etlMapEntry = (EtlMapEntry) list.get(i);
            etlMapEntry.setStarted(true);
            registerEtlMonitorMbean(etlMapEntry, serviceUnitName);
        }
        mLogger.log(Level.INFO, mLoc.loc("INFO067: Service unit: {0} started successfully.", serviceUnitName));
    }

    /**
     * Stop the deployment. This causes the component to cease generating
     * service requests related to the deployment. This returns the deployment
     * to a state equivalent to after init() was called
     * 
     * @param serviceUnitName -
     *            the name of the Service Unit being deployed.
     */
    public void stop(java.lang.String serviceUnitName) throws DeploymentException {
        mLogger.log(Level.INFO, mLoc.loc("INFO068: Stopping down service unit {0} successfully", serviceUnitName));
        List list = mEtlMapEntryTable.getEntryListByServiceUnitName(serviceUnitName);
        for (int i = 0,  I = list.size(); i < I; i++) {
            EtlMapEntry etlMapEntry = (EtlMapEntry) list.get(i);
            etlMapEntry.setStarted(false);
            unregisterEtlMonitorMbean(etlMapEntry);
        }
        mLogger.log(Level.INFO, mLoc.loc("INFO069: Stopped service unit {0} successfully", serviceUnitName));
    }

    private void unregisterEtlMonitorMbean(EtlMapEntry etlMapEntry) {

        ETLMonitorHelper helper = new ETLMonitorHelper();
        helper.unregisterMbean(etlMapEntry);



    }

    private void registerEtlMonitorMbean(EtlMapEntry etlMapEntry, String serviceUnitName) {
        ETLMonitorHelper helper = new ETLMonitorHelper();
        helper.registerMbean(etlMapEntry, serviceUnitName);

    }
}
