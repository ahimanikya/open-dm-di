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
 * @(#)ETLSEInOutThread.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */
package com.sun.jbi.engine.etl;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jbi.messaging.DeliveryChannel;
import javax.jbi.messaging.ExchangeStatus;
import javax.jbi.messaging.InOut;
import javax.jbi.messaging.MessageExchange;
import javax.jbi.messaging.MessageExchangeFactory;
import javax.jbi.messaging.MessagingException;
import javax.jbi.messaging.NormalizedMessage;
import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.wsdl.Output;
import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import com.sun.jbi.engine.etl.Localizer;
import com.sun.jbi.engine.etl.AlertsUtil;
import com.sun.jbi.alerter.NotificationEvent;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.spi.DBConnectionProvider;
import com.sun.jbi.engine.etl.mbean.ETLSERuntimeConfiguration;
import com.sun.jbi.engine.etl.persistence.BaseDAO;
import com.sun.jbi.engine.etl.persistence.DAOFactory;
import com.sun.jbi.engine.etl.persistence.ETLMessagePipeLineDAO;
import com.sun.jbi.engine.etl.persistence.EngineState;
import com.sun.jbi.engine.etl.persistence.impl.MessagePersistenceHandler;
import com.sun.jbi.nms.exchange.ExchangePattern;
import com.sun.jbi.nms.wsdl11wrapper.HelperFactory;
import com.sun.jbi.nms.wsdl11wrapper.WrapperBuilder;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.utils.RuntimeAttribute;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;



import com.sun.sql.framework.utils.StringUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ETLSEInOutThread extends Thread {

    private static transient final Logger mLogger = Logger.getLogger(ETLSEInOutThread.class.getName());
    private static transient final Localizer mLoc = Localizer.get();
    private static final String InputPartNode = "inputItem";
    private static final String OutputPartName = "part";
    private static final String OutputPartNode = "outputItem";
    private EtlMapEntryTable mEtlMapEntryTable;
    private MessageExchangeFactory mMessageExchangeFactory;
    private DeliveryChannel mChannel;
    private ETLSERuntimeConfiguration runtimeConfigMBean;
    private boolean mRunFlag;
    private Properties envProps = new Properties();
    private ExecutorService threadpool;
    private ETLInOutQueue messageQueue;
    /**
     * Map of invocation requests that are waiting for a response. Key is
     * message exchange id value is a InOut message
     */
    private Map mCorrelationMap;
    private final String COMPONENT_NAME = "sun-etl-engine";
    public static final String FAULTCODE_PROPERTY_NAME = "com.sun.jbi.crl.faultcode";
    public static final String FAULTSTRING_PROPERTY_NAME = "com.sun.jbi.crl.faultstring";
    public static final String FAULTACTOR_PROPERTY_NAME = "com.sun.jbi.crl.faultactor";
    public static final String FAULTDETAIL_PROPERTY_NAME = "com.sun.jbi.crl.faultdetail";
    private int retryMaxCount = 0;

    /**
     * Creates a new instance of ETLSEInOutThread
     * 
     * @param runtimeConfigMBean
     */
    public ETLSEInOutThread(ETLSERuntimeConfiguration runtimeConfigMBean,
            DeliveryChannel channel, EtlMapEntryTable etlMapEntryTable) {
        try {
            mRunFlag = true;
            mChannel = channel;
            mMessageExchangeFactory = mChannel.createExchangeFactory();
            mEtlMapEntryTable = etlMapEntryTable;
            mCorrelationMap = new HashMap();
            this.runtimeConfigMBean = runtimeConfigMBean;
            this.runtimeConfigMBean.addNotificationListener(listener, null,
                    null);
            this.retryMaxCount = this.runtimeConfigMBean.getRetryMaxCount();

            threadpool = Executors.newCachedThreadPool();
            //this.messageQueue = new ETLInOutQueue(this);
           
        } catch (Exception e) {
            e.printStackTrace();
            String text = e.getLocalizedMessage();
            AlertsUtil.getAlerter().critical(text,
                    ETLSELifeCycle.SHORT_DISPLAY_NAME,
                    "",
                    AlertsUtil.getServerType(),
                    AlertsUtil.COMPONENT_TYPE,
                    NotificationEvent.OPERATIONAL_STATE_RUNNING,
                    NotificationEvent.EVENT_TYPE_ALERT,
                    "ETLSE-E00701");
        }
    }

    private void process(InOut inOut) {
        threadpool.execute(new InOutMessageHandler(inOut, this.messageQueue));
    }
    
    public void setMessageQueue(ETLInOutQueue queue) {
        this.messageQueue = queue;
    }

    public void stopMessageQueue() {
        if (this.messageQueue != null) {
            this.messageQueue.stopPollingETLInOutQueue();
        }
    }
    
    public void restoreMessageQueue() {
        if (this.messageQueue != null) {
            this.messageQueue.restore();
        }
    }

    
    public void startMessageQueue() {
        if (this.messageQueue != null) {
            this.messageQueue.start();
        }
    }
    /**
     * pretty printing XML data
     * @param   doc   - XML document
     * @param   out   - output stream
     */
    public void serializeDocument(java.io.Writer writer, Document document) throws Exception {
        OutputFormat format = new OutputFormat(document);
        format.setLineWidth(65);
        format.setIndenting(true);
        format.setIndent(2);
        XMLSerializer serializer = new XMLSerializer(writer, format);
        serializer.serialize(document);
    }

    /**
     * 
     * This will be called from the executor's thread
     * 
     * @param inOut
     */
    public void handleInOutMessage(ETLInOutQueueElement element) {
        if (element == null) {
            return;
        }
        String result = "";
        QName operation = new QName(element.getOperationName());
        QName service = new QName(element.getServiceName());
        EtlMapEntry etlMapEntry = mEtlMapEntryTable.findETLEntry(operation, service);


        try {
            if (etlMapEntry == null) {
                mLogger.log(Level.SEVERE,"etlMapentry is null..........");
                String errMsg = "Cannot find etl entry for operation=" + operation + ", service=" + service;
                mLogger.log(Level.SEVERE,mLoc.loc("INFO085: {0}", errMsg));
                // FIX ME: inOut.setStatus(ExchangeStatus.ERROR)?
                return;
            }

            if(!etlMapEntry.isStarted()) {
                mLogger.log(Level.SEVERE,"etlMapentry is not started");
                // The containing service unit is not started
                //Wait for a minute for the SU to start and then try again. 
                //If still it does not start, return
                
                sleep(60000);
                if(!etlMapEntry.isStarted()) return;
            }


            Node rn = XmlUtil.createDocumentFromXML(false, element.getNormalizedMsg());
            if (rn instanceof Document) {
                mLogger.info(mLoc.loc("INFO020: InputXML: {0}", XmlUtil.toXml(((Document) rn).getDocumentElement(), "UTF-8", false)));
            } else if (rn instanceof Element) {
                mLogger.info(mLoc.loc("INFO020: InputXML: {0}", XmlUtil.toXml(rn, "UTF-8", false)));
            }
            mLogger.fine("calling startETLProcess");
            result = startETLProcess(etlMapEntry, rn);



            if (etlMapEntry != null) {
                etlMapEntry.getEndpointStatus().incrementReceivedRequests();
            }
            int noOfErrors = etlMapEntry.getETLEngine().getContext().getThrowableList().size();
            if (noOfErrors < 1) {
                MessagePersistenceHandler.deleteSuccessMessage(element);
                Writer writer = new StringWriter();
                DOMSource processedContent = new DOMSource(XmlUtil.createDocumentFromXML(
						true, result));

                Document outputDoc = XmlUtil.createDocumentFromXML(false, result);
                Element outRoot = ((Document) ((DOMSource) processedContent)
						.getNode()).getDocumentElement();
                NodeList nodeList = outRoot.getElementsByTagName(etlMapEntry.getETLEngine().getDisplayName() + "_engine_" + OutputPartNode);
                //etlMapEntry.getETLEngine().getDisplayName()+"_engine_"+
                Element partNode = (Element) nodeList.item(0);
                Output output = etlMapEntry.getOutput();
                WrapperBuilder builder = HelperFactory.createBuilder();
                builder.initialize(XmlUtil.newDocument(), output.getMessage(),
                        output.getName());
                builder.addPart(OutputPartName, partNode);

                Document normalizedDoc = builder.getResult();
                InOut inOutMsg = (InOut) mCorrelationMap.get(element);
                NormalizedMessage responseMsg = inOutMsg.createMessage();
                responseMsg.setContent(new DOMSource(normalizedDoc)); // processedContent);

                inOutMsg.setOutMessage(responseMsg);
                mChannel.send(inOutMsg);
            } else {
                MessagePersistenceHandler.UpdateMessage(element, EngineState.FAILED);

                Integer retryCount = (Integer)this.mCorrelationMap.get(element.getMsgExchId());
                if (retryCount == null) {
                    retryCount = new Integer(0);
                }

                int noOfRetries = retryCount.intValue();
                long retryInterval = this.runtimeConfigMBean.getRetryMaxInterval();
                if(retryInterval > 0) {
                    sleep(retryInterval);
                }
                if (noOfRetries <= this.retryMaxCount) {
                    this.messageQueue.pushIntoQueue(element);
                    mCorrelationMap.put(element.getMsgExchId(), new Integer(++noOfRetries));
                } else {
                    MessagePersistenceHandler.deleteSuccessMessage(element);
                }
            }
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * This will be called from the executor's thread
     * 
     * @param inOut
     */
    public void handleInOutMessage(InOut inOut) {
        if (inOut == null) {
            return;
        }

        QName operation = inOut.getOperation();
        QName service = inOut.getEndpoint().getServiceName();
        
        mLogger.log(Level.FINE,"incomingoperation is " + operation.toString());
        mLogger.log(Level.FINE,"incoming Service is " + service.toString());
        EtlMapEntry etlMapEntry = mEtlMapEntryTable.findETLEntry(operation,
                service);

        try {
            if (etlMapEntry == null) {
                String errMsg = "Cannot find etl entry for operation=" + operation + ", service=" + service;
                mLogger.log(Level.INFO, mLoc.loc("INFO085: {0}", errMsg));

                // FIX ME: inOut.setStatus(ExchangeStatus.ERROR)?
                return;
            }

            if (!etlMapEntry.isStarted()) {
                // The containing service unit is not started
                return;
            }

            NormalizedMessage request = inOut.getInMessage();

            if (request == null) {
                return;
            }

            // Calculate processed content
            Source content = request.getContent();
            Source processedContent = null;

            if (content instanceof DOMSource) {
            } else {
                String errMsg = "Unknown XML Source type: " + content.getClass().getName();
                mLogger.log(Level.INFO, mLoc.loc("INFO585: Error1{0}", errMsg));
                etlMapEntry.getEndpointStatus().incrementReceivedErrors();
                String text = mLoc.loc("INFO586: Error2{0}", errMsg);
                AlertsUtil.getAlerter().critical(text,
                        ETLSELifeCycle.SHORT_DISPLAY_NAME,
                        "",
                        AlertsUtil.getServerType(),
                        AlertsUtil.COMPONENT_TYPE,
                        NotificationEvent.OPERATIONAL_STATE_RUNNING,
                        NotificationEvent.EVENT_TYPE_ALERT,
                        "ETLSE-E00702");
                throw new Exception(errMsg);
            }


            mLogger.log(Level.INFO, mLoc.loc("INFO023: Service type: {0}doesn't support InOut", etlMapEntry.getType()));
        } catch (Throwable t) {
            String msg = "caught unexpected exception";
            mLogger.log(Level.SEVERE, mLoc.loc("ERRO587: Error3{0} ", msg), t);
            try {
                inOut.setError(new Exception(mLoc.loc("ERRO588: Error4{0} ", msg), t));
                inOut.setStatus(ExchangeStatus.ERROR);
                inOut.setProperty(FAULTCODE_PROPERTY_NAME, "Server");
                inOut.setProperty(FAULTSTRING_PROPERTY_NAME, mLoc.loc("ERRO589: Error5{0} ", msg));
                inOut.setProperty(FAULTACTOR_PROPERTY_NAME, COMPONENT_NAME);
                inOut.setProperty(FAULTDETAIL_PROPERTY_NAME, mLoc.loc("ERRO590: Error6{0} ", msg));
                mChannel.send(inOut);
                etlMapEntry.getEndpointStatus().incrementSentErrors();
            } catch (MessagingException ex) {
                mLogger.log(Level.SEVERE, mLoc.loc("ERRO025: unable to set error status on inOut"), ex);
            }
        }
    }

    private StringBuffer generateETLOutput(EtlMapEntry etlMapEntry, Node inputArgs) {
        ETLProcessHandler handler = new ETLProcessHandler(envProps);
        ETLEngine etlEngine = etlMapEntry.getETLEngine();
        setInputArgument(etlEngine, inputArgs);
        return handler.createOutputMessageString(etlEngine);
    }

    /**
     * 
     * @param etlMapEntry
     * @param inputArgs
     * @return status
     */
    private String startETLProcess(EtlMapEntry etlMapEntry, Node inputArgs) {
        ETLProcessHandler handler = new ETLProcessHandler(envProps);
        ETLEngine etlEngine = etlMapEntry.getETLEngine();

        // check if runtimeConfigMBean has any application configuration set
        Map appConfigObjectMap = runtimeConfigMBean.retrieveApplicationConfigurationsMap();
        java.util.Iterator it2 = appConfigObjectMap.keySet().iterator();
        String appDataRoot = null;
        while (it2.hasNext()) {
            String key = (String) it2.next();
            //mLogger.info("Key : "+ key);
            String value = (String) appConfigObjectMap.get(key);
            if (value != null) {
                //mLogger.info("Value : "+ value);
                appDataRoot = value;
            }
        }
        List connDefs = etlEngine.getConnectionDefList();
        List modConnections = new ArrayList();
        Iterator itr = connDefs.iterator();
        while (itr.hasNext()) {
            DBConnectionParameters conn = (DBConnectionParameters) itr.next();
            //mLogger.info("DB URL=====: "+ conn.getConnectionURL());
            String dbUrl = conn.getConnectionURL();
            if (appDataRoot != null) {
                dbUrl = dbUrl.replace("{APP_DATAROOT}", StringUtil.escapeJavaRegexpChars(appDataRoot));
                //mLogger.info("Modified DB URL=====: "+ dbUrl);
                conn.setConnectionURL(dbUrl);
            }
            modConnections.add(conn);
        }
        etlEngine.setConnectionDefList(modConnections);

        setInputArgument(etlEngine, inputArgs);
        return handler.startProcess(etlEngine);
    }

    public static void setInputArgument(ETLEngine etlEngine, Node inputArgs) {
        Map attrMap = etlEngine.getInputAttrMap();

        if (inputArgs instanceof Document) {
            Document d = (Document) inputArgs;
            Element e = d.getDocumentElement();

            NodeList nl = e.getElementsByTagNameNS("*", etlEngine.getDisplayName() + "_engine_" + InputPartNode).item(0).getChildNodes();

            for (int i = 0; i < nl.getLength(); i++) {
                Node arg = nl.item(i);

                if (arg.getNodeType() == Node.TEXT_NODE) {
                    continue;
                }
                String key = arg.getLocalName();
                if (arg.getFirstChild() == null) {
                    continue;
                }
                String value = arg.getFirstChild().getNodeValue();
                RuntimeAttribute ra = (RuntimeAttribute) attrMap.get(key);
                if (ra != null && value != null && !value.equals("")) {
                    Object o = XmlUtil.convertInputArgument(ra, value);
                    ra.setAttributeValue(o);
                }
            }
        }

    }

    public void run() {
        mLogger.log(Level.INFO, mLoc.loc("INFO026: Started ETL service engine in-out thread"));
        while (mRunFlag) {
            // when receiving an IN-OUT, IN-ONLY, ROBUST-IN message, it means
            // that BC is invoking SE.
            // when receiving an OUT-IN message, it means that the SE has
            // invoked out, and this message exchange
            // is the response.
            // There shouldn't be OUT-ONLY or ROBUST-OUT because only BC are
            // suppose to receive such messages
            if (mChannel == null) {
                continue;
            }

            try {
                MessageExchange msgEx = mChannel.accept();
                URI pattern = msgEx.getPattern();
                mLogger.log(Level.INFO, mLoc.loc("INFO027: Status: {0}, Pattern for exchange Id {1} is {2} ", msgEx.getStatus().toString(), msgEx.getExchangeId(), pattern));
                if (msgEx.getStatus().equals(ExchangeStatus.ACTIVE)) {

                    switch (ExchangePattern.valueOf(msgEx)) {
                        case IN_OUT:
                            mLogger.log(Level.INFO, mLoc.loc("INFO028: Received in-out message {0}", msgEx.getExchangeId()));
                            process((InOut) msgEx);
                            break;
                        case IN_ONLY:
                            mLogger.log(Level.INFO, mLoc.loc("INFO029: In-only is not supported {0}", msgEx.getExchangeId()));
                            sendError(msgEx);
                            break;
                        case IN_OPTIONAL_OUT:
                            mLogger.log(Level.INFO, mLoc.loc("INFO030: Robust in-optional-out is not supported {0}", msgEx.getExchangeId()));
                            sendError(msgEx);
                            break;
                        case ROBUST_IN_ONLY:
                            mLogger.log(Level.INFO, mLoc.loc("INFO031: Robust in-only is not supported {0}", msgEx.getExchangeId()));
                            sendError(msgEx);
                            break;
                        case CUSTOM:
                            mLogger.log(Level.INFO, mLoc.loc("INFO032: Received invalid pattern info {0}", msgEx.getExchangeId()));
                            sendError(msgEx);
                            break;
                        default:
                            mLogger.log(Level.INFO, mLoc.loc("INFO032: Received invalid pattern info {0}", msgEx.getExchangeId()));
                            sendError(msgEx);
                            break;
                    }

                }
            } catch (Exception e) {
                if (mRunFlag) {
                    e.printStackTrace();
                    mLogger.log(Level.SEVERE, mLoc.loc("ERRO034: caught exception acception message on channel "), e);
                    String text = mLoc.loc("INFO034: caught exception acception message on channel");
                    AlertsUtil.getAlerter().critical(text,
                            ETLSELifeCycle.SHORT_DISPLAY_NAME,
                            "",
                            AlertsUtil.getServerType(),
                            AlertsUtil.COMPONENT_TYPE,
                            NotificationEvent.OPERATIONAL_STATE_RUNNING,
                            NotificationEvent.EVENT_TYPE_ALERT,
                            "ETLSE-E00703");
                }
            }
        }
        mLogger.log(Level.INFO, mLoc.loc("INFO035: ETL service engine in-out thread finished"));
    }

    private void sendError(MessageExchange msgEx) throws MessagingException {
        msgEx.setStatus(ExchangeStatus.ERROR);
        mChannel.send(msgEx);
    }

    public void cease() {
        try {
            mLogger.log(Level.INFO, mLoc.loc("INFO036: Ceasing ETL service engine in-out thread"));
            mRunFlag = false;
            mLogger.log(Level.INFO, mLoc.loc("INFO037: ETL service engine in-out thread ceased"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private NotificationListener listener = new  

          NotificationListener(   ) {
             public void handleNotification(Notification notification, Object obj) {
			mLogger.log(Level.INFO,mLoc.loc("INFO038: ETLSEInOutThread.Handling_notification"));
            if (notification instanceof AttributeChangeNotification) {
                AttributeChangeNotification attrNotif = (AttributeChangeNotification) notification;
                String attrName = attrNotif.getAttributeName();
                mLogger.log(Level.INFO, mLoc.loc("INFO039: ETLSEInOutThread.Getting_notification_attribute {0}", attrName));
                envProps.setProperty(attrName, (String) attrNotif.getNewValue());
            // TODO use this envProps for the next instance of ETL engine
            }
        }
    };

    private class InOutMessageHandler implements Runnable {

        InOut inOutMsg;
        ETLInOutQueue inOutQueue;

        public InOutMessageHandler(InOut inout, ETLInOutQueue inOutQueue) {
            inOutMsg = inout;
            this.inOutQueue = inOutQueue;
        }

        public void run() {
            String msgExchangeId = inOutMsg.getExchangeId();
            String serviceName = inOutMsg.getEndpoint().getServiceName().toString();
            String operationName = inOutMsg.getOperation().getLocalPart().toString();// "execute";

            Source content = inOutMsg.getInMessage().getContent();
            EtlMapEntry etlMapEntry = null;
            try {
                MessagePersistenceHandler.persistMessage(inOutMsg);
                if (content instanceof DOMSource) {
                    Writer writer = new StringWriter();
                    String serializedMessage = XmlUtil.toXml(((DOMSource) content).getNode(), "UTF-8", false);
                    ETLInOutQueueElement queueElement = new ETLInOutQueueElement(msgExchangeId, serializedMessage, serviceName, operationName);
                    mCorrelationMap.put(queueElement, inOutMsg);
                    inOutQueue.pushIntoQueue(queueElement);
                } else {
                    handleInOutMessage(inOutMsg);
                }
            } catch (Throwable t) {
                String msg = "caught unexpected exception";
                mLogger.log(Level.SEVERE, mLoc.loc("ERRO587: Error3{0} ", msg), t);
                try {
                    inOutMsg.setError(new Exception(mLoc.loc("ERRO588: Error4{0} ", msg), t));
                    inOutMsg.setStatus(ExchangeStatus.ERROR);
                    inOutMsg.setProperty(FAULTCODE_PROPERTY_NAME, "Server");
                    inOutMsg.setProperty(FAULTSTRING_PROPERTY_NAME, mLoc.loc("ERRO589: Error5{0} ", msg));
                    inOutMsg.setProperty(FAULTACTOR_PROPERTY_NAME, COMPONENT_NAME);
                    inOutMsg.setProperty(FAULTDETAIL_PROPERTY_NAME, mLoc.loc("ERRO590: Error6{0} ", msg));
                    mChannel.send(inOutMsg);
                    etlMapEntry.getEndpointStatus().incrementSentErrors();
                } catch (MessagingException ex) {
                    mLogger.log(Level.SEVERE, mLoc.loc("ERRO025: unable to set error status on inOut"), ex);
                }
            }
        }
    }
}
