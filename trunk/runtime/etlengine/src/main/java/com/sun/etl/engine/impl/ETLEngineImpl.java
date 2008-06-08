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
 * @(#)ETLEngineImpl.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLEngineListener;
import com.sun.etl.engine.ETLPersistableMessage;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.RuntimeAttribute;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.sql.framework.utils.XmlUtil;

/**
 * This class is the container for running the ETL Process. This class contains the engine
 * context and metamodel using which ETLProcess is executed, also implements ETLEngine
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLEngineImpl implements ETLEngine {

    /** ETLEngine tag */
    public static final String TAG_ETLENGINE = "etlengine";

    /** Runtime input tag */
    public static final String TAG_INPUTATTR = "runtimeInputs";

    /** Runtime output tag */
    public static final String TAG_OUTPUTATTR = "runtimeOutputs";

    /** ETLEngineTasks */
    public static final String TAG_TASK_IMPL_CLASS = "implClass";

    /** ETLEngineTask Root Class */
    public static final String TAG_TASK_IMPL_CLASS_FQ_NAME = "class";

    /** ETLEngineTask Root Name */
    public static final String TAG_TASK_IMPL_CLASS_NAME = "name";

    /** ETLEngineTask Root name */
    public static final String TAG_TASK_IMPL_CLASSNAME_MAP = "TaskImplClassNameMap";

    /** Runtime context for this class. */
    private static final String LOG_CATEGORY = ETLEngineImpl.class.getName();

    private static volatile MessageManager messageManager = MessageManager.getManager("com.sun.etl.engine");

    private static final String TASK_DEF_FILE = "com/sun/etl/engine/impl/enginetaskdef.xml";

    /**
     * Should be used only in this package
     * 
     * @return MessageManager for the package
     */
    public static MessageManager getPackageMessageManager() {
        return MessageManager.getManager(ETLEngine.class);
    }

    private List conDefList = new ArrayList();

    /** ETL Engine context */
    private ETLEngineContext context;

    private String displayName = "";

    /** end task node. */
    private ETLTaskNode endETLTaskNode;

    private Map inputAttrMap = new HashMap();

    private boolean onAppServer = false;

    private Map outputAttrMap = new HashMap();

    private ETLTaskNode startETLTaskNode;

    private String taskDefFile;

    /** Task Impl Class Name Map. */
    private HashMap taskImplClassNameMap = new HashMap();

    /** ETL Task thread manager. */
    private ETLTaskThreadManager taskManager;

    /** Task node map. */
    private HashMap taskNodeMap = new HashMap();

    /**
     * Constructor: loads the the default task definition file.
     */
    public ETLEngineImpl() {
        start();
    }

    public void addInputAttribute(RuntimeAttribute runtimeAttribute) {
        this.inputAttrMap.put(runtimeAttribute.getAttributeName(), runtimeAttribute);
    }

    public void addOutputAttribute(RuntimeAttribute runtimeAttribute) {
        this.outputAttrMap.put(runtimeAttribute.getAttributeName(), runtimeAttribute);
    }

    /**
     * Create end task node.
     * 
     * @return true if successful. false if failed.
     */
    public boolean createEndETLTaskNode() {
        if (endETLTaskNode == null) {
            endETLTaskNode = new ETLTaskNodeImpl();
            endETLTaskNode.setId(END);
            endETLTaskNode.setTaskType(END);
            endETLTaskNode.setParent(this);

            taskNodeMap.put(END, endETLTaskNode);
            return true;
        }

        return false;
    }

    /**
     * Creates task node of the given type.
     * 
     * @param taskType String representing task node type.
     * @return new ETLTaskNode instance of given type
     */
    public ETLTaskNode createETLTaskNode(String taskType) {
        if (START.equals(taskType) || END.equals(taskType)) {
            throw new IllegalArgumentException("Cannot create a START or END task using this method!");
        }

        ETLTaskNodeImpl taskNode = new ETLTaskNodeImpl();
        taskNode.setTaskType(taskType);
        taskNode.setParent(this);

        String nodeId = generateId(taskType);
        taskNode.setId(nodeId);

        taskNodeMap.put(nodeId, taskNode);
        return taskNode;
    }

    /**
     * Create start task node.
     * 
     * @return true if successful. false if failed.
     */
    public boolean createStartETLTaskNode() {
        if (startETLTaskNode == null) {
            startETLTaskNode = new ETLTaskNodeImpl();
            startETLTaskNode.setId(START);
            startETLTaskNode.setTaskType(START);
            startETLTaskNode.setParent(this);

            taskNodeMap.put(START, startETLTaskNode);
            return true;
        }

        return false;
    }

    /**
     * execute the tasks by invoking process() method.
     * 
     * @param theExecListener Which implements ETLEngineExecListener
     * @return an integer
     */
    public final int exec(ETLEngineListener theExecListener) {
        String managerName = "ETLEngine";
        ETLTaskThread startThread = null;

        if (displayName != null) {
            managerName += displayName;
        }

        ETLEngineExecEvent anEvent = new ETLEngineExecEvent(managerName, ETLEngine.STATUS_COLLAB_STARTED, "Collab Started");
        theExecListener.executionPerformed(anEvent);
        
        try {
            if (this.getContext() == null) {
                String errContextMessage = messageManager.getString("ERR_NO_CONTEXT");
                throw new RuntimeException(errContextMessage);
            }

            taskManager = new ETLTaskThreadManager(managerName);

            synchronized (this) {
                taskManager.addEngineListener(theExecListener);
            }

            if (startETLTaskNode == null) {
                String errMsg = messageManager.getString("ERR_ROOT_NODE");
                throw new ETLException(LOG_CATEGORY, errMsg);
            }
            Logger.print(Logger.infoLevel(), LOG_CATEGORY, "Starting ETLEngine Thread");
            startThread = new ETLTaskThread(taskManager, startETLTaskNode);

            if (startThread == null) {
                throw new RuntimeException("Can't instantiate eTL engine...");
            }
            return 0;
        } catch (Exception t) {
            Logger.printThrowable(Logger.errorLevel(), LOG_CATEGORY, "ETLEngine", t.getMessage(), t);
            ETLEngineExecEvent errEvent = new ETLEngineExecEvent(managerName, ETLEngine.STATUS_COLLAB_EXCEPTION, t);
            theExecListener.executionPerformed(errEvent);
            taskManager.setEngineListener(null);
            return -1;
        }
    }

    /**
     * Generate a unique id for the tasknode
     * 
     * @param prefix prefix is used to make it more redable.
     */
    public synchronized String generateId(String prefix) {
        int cnt = 0;

        String id = prefix + "_" + cnt;
        while (taskNodeMap.containsKey(id)) {
            cnt++;
            id = prefix + "_" + cnt;
        }
        return id;
    }

    /**
     * get connection definition list associated with this engine
     */
    public List getConnectionDefList() {
        return conDefList;
    }

    /**
     * Gets the associated context of the engine instance
     * 
     * @return The context value
     */
    public ETLEngineContext getContext() {
        return context;
    }

    /**
     * get display name of the engine.
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Get the end ETLTaskNode
     * 
     * @return ETLTaskNode The end ETLTaskNode value
     */
    public final ETLTaskNode getEndETLTaskNode() {
        return endETLTaskNode;
    }

    public synchronized ETLEngineListener getETLEngineListener() {
        return taskManager.getEngineListener();
    }

    /**
     * Get the taskNode of a given taskId
     * 
     * @param taskId Task Id
     * @return ETLTaskNode The taskNode value
     */
    public final ETLTaskNode getETLTaskNode(String taskId) {
        return (ETLTaskNode) taskNodeMap.get(taskId);
    }

    /**
     * Get the Full qualified impl class name of a given task type
     * 
     * @param taskType Task Type
     * @return The classRef value
     */
    public String getFullQualifiedImplClassName(String taskType) {

        String classRef = (String) taskImplClassNameMap.get(taskType);
        if (classRef != null && classRef.trim().length() != 0) {

            return classRef;
        }
        return null;
    }

    /**
     * Get Runtime attributes as name and value
     * 
     * @return Map
     */
    public Map getInputAttributes() {
        Map runtimeAttributes = new HashMap();

        Iterator iter = this.inputAttrMap.values().iterator();
        while (iter.hasNext()) {
            RuntimeAttribute runAttr = (RuntimeAttribute) iter.next();
            runtimeAttributes.put(runAttr.getAttributeName(), runAttr.getAttributeValue());
        }
        return runtimeAttributes;
    }

    /**
     * @see com.sun.etl.engine.ETLEngine#getInputAttrMap
     */
    public Map getInputAttrMap() {
        return inputAttrMap;
    }

    /**
     * Getter for Runtime Output arguments
     * 
     * @return Collection of runtime output arguments
     */
    public Map getRuntimeOutputArguments() {
        return this.outputAttrMap;
    }

    /**
     * Get the Start ETLTaskNode
     * 
     * @return ETLTaskNode The Start ETLTaskNode value
     */
    public final ETLTaskNode getStartETLTaskNode() {
        return startETLTaskNode;
    }

    /**
     * Setter for JndiStatus to indicate if it is running in IS
     * 
     * @param flag
     */
    public boolean isRunningOnAppServer() {
        return onAppServer;
    }

    /**
     * Parse the XML element to build Engine
     * 
     * @param element XML form of engine tasks
     * @throws com.sun.sql.framework.exception.BaseException if error.
     */
    public void parseXML(Element element) throws BaseException {
        if (element == null) {
            throw new BaseException("Must supply non-null Element ref for parameter 'element'.");
        }
        setDisplayName(element.getAttribute("name"));
        NodeList list = element.getElementsByTagName(ETLTaskNode.TAG_TASK);
        for (int i = 0; i < list.getLength(); i++) {
            Element taskElement = (Element) list.item(i);
            ETLTaskNodeImpl taskNode = new ETLTaskNodeImpl();
            taskNode.parseXML(taskElement);
            taskNode.setParent(this);
            if (taskNode.getId() != null) {
                if (taskNode.getId().equals(START)) {
                    startETLTaskNode = taskNode;
                    startETLTaskNode.setId(START);
                    startETLTaskNode.setTaskType(START);
                } else if (taskNode.getId().equals(END)) {
                    endETLTaskNode = taskNode;
                    endETLTaskNode.setId(END);
                    endETLTaskNode.setTaskType(END);
                }
            }

            taskNodeMap.put(taskNode.getId(), taskNode);
        }

        list = element.getElementsByTagName(DBConnectionParameters.CONNECTION_DEFINITION_TAG);

        conDefList.clear();
        try {
            for (int i = 0; i < list.getLength(); i++) {
                Element conDefElement = (Element) list.item(i);
                DBConnectionParameters conDef = new DBConnectionParameters();
                conDef.parseXML(conDefElement);
                conDefList.add(conDef);
            }
        } catch (BaseException e) {
            Logger.print(Logger.ERROR, LOG_CATEGORY, "Failed to parse ConnectionDefinition in Engine", e);
            throw new BaseException("Failed to parse ConnectionDefinition in Engine", e);
        }

        // Read in runtime input attributes, if any.
        inputAttrMap.clear();
        list = element.getElementsByTagName(TAG_INPUTATTR);
        for (int i = 0; i < list.getLength(); i++) {
            Element runtimeElem = (Element) list.item(i);
            NodeList attrList = runtimeElem.getElementsByTagName(RuntimeAttribute.TAG_ATTR);
            for (int j = 0; j < attrList.getLength(); j++) {
                RuntimeAttribute attr = new RuntimeAttribute();
                Element attrElement = (Element) attrList.item(j);
                attr.parseXMLString(attrElement);
                inputAttrMap.put(attr.getAttributeName(), attr);
            }
        }

        // Read in runtime output attributes, if any.
        outputAttrMap.clear();
        list = element.getElementsByTagName(TAG_OUTPUTATTR);
        for (int i = 0; i < list.getLength(); i++) {
            Element runtimeElem = (Element) list.item(i);
            NodeList attrList = runtimeElem.getElementsByTagName(RuntimeAttribute.TAG_ATTR);
            for (int j = 0; j < attrList.getLength(); j++) {
                RuntimeAttribute attr = new RuntimeAttribute();
                Element attrElement = (Element) attrList.item(j);
                attr.parseXMLString(attrElement);
                outputAttrMap.put(attr.getAttributeName(), attr);
            }
        }
    }

    /**
     * set connection definition list associated with this engine
     */
    public void setConnectionDefList(List conDefs) {
        conDefList = conDefs;
    }

    /**
     * Sets the context of the engine
     * 
     * @param engineContext The context value
     */
    public synchronized void setContext(ETLEngineContext engineContext) {
        this.context = engineContext;
    }

    /**
     * set display name of the engine.
     */
    public void setDisplayName(String theDisplayName) {
        displayName = theDisplayName;
    }

    /**
     * @see com.sun.etl.engine.ETLEngine#setInputAttrMap
     */
    public void setInputAttrMap(Map newAttrs) {
        inputAttrMap.clear();
        if (newAttrs != null) {
            inputAttrMap.putAll(newAttrs);
        }
    }

    /**
     * @see com.sun.etl.engine.ETLEngine#setOutputAttrMap
     */
    public void setOutputAttrMap(Map newAttrs) {
        outputAttrMap.clear();
        if (newAttrs != null) {
            outputAttrMap.putAll(newAttrs);
        }
    }

    /**
     * Setter for Overriding runtime values
     * 
     * @param obj to be set
     */
    public void setOverrideMap(ETLPersistableMessage persistMsg) {
        Iterator it = this.inputAttrMap.keySet().iterator();
        Logger.print(Logger.DEBUG, LOG_CATEGORY, "InputAttrMap is " + this.inputAttrMap);
        try {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, "persistMsg: " + persistMsg);
            while (it.hasNext()) {
                String key = (String) it.next();
                if (persistMsg != null) {
                    Object objRef = persistMsg.getPart(key);
                    RuntimeAttribute attr = (RuntimeAttribute) this.inputAttrMap.get(key);
                    if (objRef != null) {
                        if (objRef instanceof String) {
                            objRef = StringUtil.removeIllegalRuntimeValueChars((String) objRef);
                        }
                        attr.setAttributeValue(objRef);
                    }
                } else {
                    Logger.print(Logger.WARN, LOG_CATEGORY, "Object is not of ETLPersistent Message; setting default value.");
                }
            }
        } catch (Exception e) {
            Logger.print(Logger.ERROR, LOG_CATEGORY, "Error caught while getting the value from runtime inputs in ETLEngineImpl", e);
        }
    }

    /**
     * @see com.sun.etl.engine.ETLEngine#setRunningOnAppServer(boolean)
     */
    public void setRunningOnAppServer(boolean runningOnAppServer) {
        onAppServer = runningOnAppServer;
    }

    /**
     * Starts ETLEngine
     */
    public void start() {
        try {
            loadTaskRef();
        } catch (ETLException e) {
            String exMsg = messageManager.getString("EX_LOADING_TASK_REFS", TASK_DEF_FILE, e);
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, exMsg, e);
        }
    }

    /**
     * Stop the ETLEngine using taskmanager
     */
    public void stopETLEngine() {
        if (taskManager != null) {
            taskManager.stopETLEngine();
        }
        String infoMsg = messageManager.getString("INFO_ETL_ENGINE_STOP");
        Logger.print(Logger.INFO, LOG_CATEGORY, infoMsg);
    }

    /**
     * Gets the XML String for Engine
     * 
     * @return String XML form of engine tasks
     */
    public String toXMLString() {
        StringBuffer xml = new StringBuffer(500);

        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<" + TAG_ETLENGINE + " name=\"" + getDisplayName() + "\">\n");

        Iterator it = taskNodeMap.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            ETLTaskNode taskNode = (ETLTaskNode) taskNodeMap.get(key);

            xml.append(taskNode.toXMLString(TAB));
        }

        it = conDefList.iterator();
        while (it.hasNext()) {
            DBConnectionParameters conDef = (DBConnectionParameters) it.next();
            xml.append(conDef.toXMLString(TAB));
        }

        // Runtime input attributes
        it = inputAttrMap.values().iterator();
        if (it.hasNext()) {
            xml.append(TAB).append("<").append(TAG_INPUTATTR).append(">\n");

            while (it.hasNext()) {
                RuntimeAttribute attr = (RuntimeAttribute) it.next();
                xml.append(attr.toXMLString(TAB + TAB));
            }

            xml.append(TAB).append("</").append(TAG_INPUTATTR).append(">\n");
        }

        // Runtime output attributes
        it = outputAttrMap.values().iterator();
        if (it.hasNext()) {
            xml.append(TAB).append("<").append(TAG_OUTPUTATTR).append(">\n");

            while (it.hasNext()) {
                RuntimeAttribute attr = (RuntimeAttribute) it.next();
                xml.append(attr.toXMLString(TAB + TAB));
            }

            xml.append(TAB).append("</").append(TAG_OUTPUTATTR).append(">\n");
        }

        xml.append("</" + TAG_ETLENGINE + ">\n");

        XmlUtil.dumpXMLString("Engine_" + getDisplayName() + ".xml", xml.toString());
        return xml.toString();
    }

    /**
     * Loads the ETLEngine Tasks from the enginetaskdef.xml
     * 
     * @exception com.sun.sql.framework.exception.BaseException thrown while Loading Tasks
     */
    private boolean loadTaskRef() throws ETLException {

        if (StringUtil.isNullString(this.taskDefFile)) {
            this.taskDefFile = TASK_DEF_FILE;
        }
        Element etlEngineTaskRootElement = XmlUtil.loadXMLFile(taskDefFile, this.getClass().getClassLoader());

        String errMsg = messageManager.getString("ERR_ROOT_NODE");
        if (etlEngineTaskRootElement == null) {
            throw new ETLException(errMsg);
        }

        if (!etlEngineTaskRootElement.getTagName().equals(TAG_TASK_IMPL_CLASSNAME_MAP)) {
            String err = messageManager.getString("ERR_UNKNOWN_TASK", TASK_DEF_FILE, etlEngineTaskRootElement.getTagName());
            throw new ETLException(LOG_CATEGORY, err);
        }
        NodeList taskDefList = etlEngineTaskRootElement.getElementsByTagName(TAG_TASK_IMPL_CLASS);

        if (taskImplClassNameMap != null) {
            this.taskImplClassNameMap = parseTaskDefList(taskDefList);
        }

        return true;
    }

    /**
     * Parse task node list in the DOM tree and put the task nodes in metamodel. Return a
     * HashMap of task nodes.
     * 
     * @param taskNodeList task node list in the DOM tree.
     * @return HashMap of task nodes.
     */
    private HashMap parseTaskDefList(NodeList taskNodeList) {

        HashMap tasks = new HashMap(100);
        Element taskElement;

        for (int i = 0; i < taskNodeList.getLength(); i++) {
            taskElement = (Element) taskNodeList.item(i);
            String taskId = taskElement.getAttribute(TAG_TASK_IMPL_CLASS_NAME);
            String taskClass = taskElement.getAttribute(TAG_TASK_IMPL_CLASS_FQ_NAME);

            if (taskId != null && taskClass != null) {
                tasks.put(taskId, taskClass);
            }
        }
        return tasks;
    }

}
