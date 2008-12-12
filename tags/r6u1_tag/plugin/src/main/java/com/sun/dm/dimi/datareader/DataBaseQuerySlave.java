/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2003-2007 Sun Microsystems, Inc. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the Common 
 * Development and Distribution License ("CDDL")(the "License"). You 
 * may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * https://open-dm-mi.dev.java.net/cddl.html
 * or open-dm-mi/bootstrap/legal/license.txt. See the License for the 
 * specific language governing permissions and limitations under the  
 * License.  
 *
 * When distributing the Covered Code, include this CDDL Header Notice 
 * in each file and include the License file at
 * open-dm-mi/bootstrap/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the 
 * fields enclosed by brackets [] replaced by your own identifying 
 * information: "Portions Copyrighted [year] [name of copyright owner]"
 */

/*
 * DataBaseQueryThread.java
 *
 * Created on Oct 18, 2007, 11:02:20 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.dataobject.metadata.MetaDataManager;
import com.sun.dm.dimi.dataobject.metadata.MetaDataService;
import com.sun.dm.dimi.qmquery.QueryObjectBuilder;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.mdm.index.dataobject.ChildType;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import com.sun.mdm.index.dataobject.objectdef.Field;
import com.sun.mdm.index.objects.ObjectField;
import com.sun.mdm.index.objects.ObjectNode;
import com.sun.mdm.index.objects.exception.ObjectException;
import com.sun.mdm.index.query.AssembleDescriptor;
import com.sun.mdm.index.query.QMException;
import com.sun.mdm.index.query.QMIterator;
import com.sun.mdm.index.query.QueryManager;
import com.sun.mdm.index.query.QueryManagerImpl;
import com.sun.mdm.index.query.QueryObject;
import com.sun.mdm.index.query.ResultObjectAssembler;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish Bharani
 */
public class DataBaseQuerySlave extends Thread {

    ChildType[] childTypeContainer = null;
    HashMap childIndexRefMap = new HashMap();
    Connection conn = null;
    List doLinkedList = null;
    ArrayList filteredSelects;
    MetaDataService mdservice;
    DatabaseDataReader dbreader;
    // Thread Constants
    private boolean isSlaveRunning = false;
    private boolean isThreadSleeping = true;
    private boolean isBatchFull = true;
    // File Handling Constants
    private char CHILD_CHAR = '*';
    StringBuilder dataObjStrBldr = new StringBuilder(); // This will be reused for all the data object building (primary builder)
    StringBuilder dataObjStrBldrTemp = new StringBuilder(); // This will be reused for all the data object building (temporary builder)
    //Query Manager
    AssembleDescriptor assDesc = null;
    QueryManager queryManager = null;
    //Flags
    private boolean isAbronmalHalt = false;
    //logger
    private static Logger sLog = LogUtil.getLogger(DataBaseQuerySlave.class.getName());
    private static Localizer sLoc = Localizer.get();

    /**
     * Constructor for DataBaseQuerySlave
     */
    public DataBaseQuerySlave() {
    }

    /**
     * Constructor for DataBaseQuerySlave
     * @param dbconn
     * @param dolinkedlist
     * @param filters
     * @param dbreader
     */
    public DataBaseQuerySlave(Connection dbconn, List dolinkedlist, ArrayList filters, DatabaseDataReader dbreader) {
        this.conn = dbconn;
        this.doLinkedList = dolinkedlist;
        this.filteredSelects = filters;
        this.dbreader = dbreader;
        this.mdservice = MetaDataManager.getMetaDataManager().getMetaDataService();
        
        //Initialize the chidl type container to maximum size of children that are possible in the model
        this.childTypeContainer = new ChildType[this.mdservice.getChildIndexMap().keySet().size()];
        initChildIndexRefMap();
        
        //Query Manager init
        initQueryManager();
    }
    
    private void initChildIndexRefMap(){
        Iterator i = this.mdservice.getChildIndexMap().keySet().iterator();
        while (i.hasNext()){
            Object key = i.next();
            String indexvalue = this.mdservice.getChildIndexMap().get(key).toString();
            String childQname = key.toString();
            String childname = childQname.substring(childQname.lastIndexOf(".") + 1, childQname.length());
            this.childIndexRefMap.put(childname, indexvalue);
        }
    }

    /**
     * Thread Run Method
     */
    @Override
    public void run() {
        sLog.info(sLoc.x("PLG010: Query Manager Slave : Starting Process to fetch data ..."));
        isSlaveRunning = true;
        sLog.fine(" is Slave Running : " + isSlaveRunning);
        isThreadSleeping = false;

        try {
            // Get PK Column Name for the Root Table [Assuming only one column is a primary key]
            Statement stmt = this.conn.createStatement();
            ResultSet rootpkcolnameRS = stmt.executeQuery("select PKCOLUMN_NAME from " + PluginConstants.AXION_KEY_TABLE + " where PK_NAME IS NOT NULL AND PKTABLE_NAME='" + PluginConstants.QueryManagerTablePrefix + this.mdservice.getHirarchicalRootName().toUpperCase() + "' ORDER BY PKTABLE_NAME");

            String rootpkcolumn = null;
            while (rootpkcolnameRS.next()) {
                rootpkcolumn = rootpkcolnameRS.getString(1);
                sLog.fine("Primary Key for the Root Table is : " + rootpkcolumn);
            }

            //Query Root Table for the PK_COLUMN Values. Create Queries only
            ResultSet countRootPKs = stmt.executeQuery("select count(" + rootpkcolumn + ") from " + PluginConstants.QueryManagerTablePrefix + mdservice.getHirarchicalRootName().toUpperCase());
            QueryObjectBuilder qob = null;
            ResultSet rootpks = null;
            if (countRootPKs.next()) {
                if (Integer.parseInt(countRootPKs.getString(1)) > 0) {
                    // Start Building Query Here
                    qob = createQueryObjectBuilder();
                    rootpks = stmt.executeQuery("select " + rootpkcolumn + " from " + PluginConstants.QueryManagerTablePrefix + mdservice.getHirarchicalRootName().toUpperCase());
                    while (isSlaveRunning) {
                        // Execute Query with a single primary key at a time and create joins with it - refactor later (this performs the best at the moment)
                        if (rootpks.next()) {
                            QueryObject queryObject = buildQueryObject(qob, null, rootpks.getString(1));
                            queryDB(queryObject);
                        } else {
                            sLog.fine(LocalizedString.valueOf("Data Reader Thread Stopped."));
                            stopSlaveThread();
                        }
                    }
                } else {
                    sLog.warnNoloc("No Primary Data Available in Parent Table to process.");
                    isAbronmalHalt = true;
                    stopSlaveThread();
                }
            }

            /*
            // This code tests IN Query operator with the axion queries.
            int dynamicbatchsize = PluginConstants.QueryBatchSize;
            String[] batch = new String[PluginConstants.QueryBatchSize];
            while(isSlaveRunning){
            // Execute Query in Batches of primary keys.
            batch = getNextBatch(rootpks, dynamicbatchsize, batch);
            if (this.isBatchFull){
            QueryObject queryObject = buildQueryObject(qob, null, batch);
            queryDB(queryObject);
            // Initialize variables
            dynamicbatchsize = PluginConstants.QueryBatchSize;
            for (int j=0; j < batch.length; j++){
            batch[j] = null;
            }
            } else{
            // This is the last set of data being executed
            QueryObject queryObject = buildQueryObject(qob, null, batch);
            queryDB(queryObject);
            stopSlaveThread();
            }
            }
            // This code works when db is queried without join condition - to remove later
            ResultSet rootpks = stmt.executeQuery("select " +  rootpkcolumn + " from " + PluginConstants.QueryManagerTablePrefix + mdservice.getHirarchicalRootName().toUpperCase());
            while(rootpks.next()){
            String rootpkval = rootpks.getString(1);
            // Create Query Object with join condition on current PK value from the root table
            QueryObject queryObject = buildQueryObject(qob, rootpkcolumn, rootpkval);
            queryDB(queryObject);
            logger.log(Level.FINE, "[DataBase File Reader] Current No Of Data Objects processed [PK : " + rootpkval + "] : " + doLinkedList.size());
            }
            // This code works when query is executed with left out joins - to remove later
            //QueryObject queryObject = buildQueryObject(qob, null, null);
            //queryDB(queryObject);
             */
            if (!isAbronmalHalt) {
                sLog.info(sLoc.x("PLG005: Query Manager Slave : ALL DATA FETCHED SUCCESSFULLY. Slave Process will end."));
            }
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("PLG006: Failed to Execute Query", ex));
        } finally {
            this.isSlaveRunning = false; // All the data has been retrieved
            this.dbreader.wakeUpMaster(); // Finally process all the pending data
        }
    }

    private String[] getNextBatch(ResultSet rs, int dynamicbatchsize, String[] batch) {
        try {
            while (rs.next()) {
                if (dynamicbatchsize != 0) {
                    batch[PluginConstants.QueryBatchSize - dynamicbatchsize] = rs.getString(1);
                    --dynamicbatchsize;
                    if (dynamicbatchsize == 0) {
                        break;
                    }
                }
            }
        } catch (SQLException ex) {
            sLog.severe(LocalizedString.valueOf("SQLException"), ex);
        }

        if (dynamicbatchsize != 0) {
            this.isBatchFull = false;
        }
        return batch;
    }

    private void initQueryManager() {
        sLog.fine("Initializing Query Manager ...");
        this.assDesc = new AssembleDescriptor();
        ResultObjectAssembler roa = new DataObjectNodeAssembler();
        this.assDesc.setAssembler(roa);
        this.queryManager = new QueryManagerImpl();
    }

    /**
     * Querries the database with the queru object and fetches the result set iterator
     * @param qObj - Query Manager defines object for the query
     */
    private void queryDB(QueryObject qObj) {
        try {
            qObj.setAssembleDescriptor(this.assDesc);
            QMIterator iterator = this.queryManager.executeAssemble(this.conn, qObj);
            while (iterator.hasNext()) {
                Object rootObj = iterator.next();
                //ObjectNode on = (ObjectNode) rootObj;
                DataObjectNode don = (DataObjectNode) rootObj;
                getDataObject(don);
            //createFlattenedHirarchy(on);
            }
        } catch (QMException ex) {
            sLog.severe(sLoc.x("PLG007: Eview Query Manager Error during DB Query.", ex));
            sLog.warnNoloc("Following errors may have occured :\n" + ex.getMessage() + "\nOR\n" + "Check if plugin dependencies have been added into index-core runtime");
            this.isAbronmalHalt = true;
            stopSlaveThread();
        }
    }

    /**
     * This Data Object Builder directly maps Data Object Node to Data Objects.
     * No Validations have been implemented yet.
     */
    private void getDataObject(DataObjectNode dataObjectNode) {
        sLog.fine("DataBaseDataReader : Using DataObjectNode to DataObject mapping method");
        //sLog.infoNoloc("Processing Data Object :: \n" + dataObjectNode.toString());
        
        DataObject newDataObjectHolder = new DataObject();
        
        //Inset Queried System fields to Data Object
        insertSystemFields(newDataObjectHolder, dataObjectNode.getDefaultSystemFields());
        
        // Add Fields to the parent
        ObjectNode objectNode = dataObjectNode.getObjectNode();
        copyFields(objectNode, newDataObjectHolder, dataObjectNode);

        //Analyze children
        ArrayList allChildren = objectNode.getAllChildrenFromHashMap();
        
        // Init ChildType Container with dummy Child Type Objects for its capacity
        for (int j=0; j < this.childTypeContainer.length; j++){
            this.childTypeContainer[j] = new ChildType();
        }
        
        if (allChildren != null) {
            
            for (int i = 0; i < allChildren.size(); i++) {
                ObjectNode childnode = (ObjectNode) allChildren.get(i);
                int childindex = Integer.parseInt((String)this.childIndexRefMap.get(childnode.pGetTag()));
                this.childTypeContainer[childindex].addChild(copyFields(childnode, new DataObject(), null));
            }

            // Add All Children to the parant data object
            for (int i=0; i < this.childTypeContainer.length; i++){
                newDataObjectHolder.addChildType(this.childTypeContainer[i]);
            }
            
        }

        // Check if data object list is below threashold, in this case, keep adding to this list or else add and sleep
        if (doLinkedList.size() < PluginConstants.available_do) {
            addDataObjectToList(newDataObjectHolder, dataObjectNode);
        } else {
            dbreader.wakeUpMaster();
            addDataObjectToList(newDataObjectHolder, dataObjectNode);
            goSleep(PluginConstants.slave_retry_freq);
        }

    }

    /**
     * Add the generated Data Object to the list and Addrs DataObjectNode to the finalization list
     * @param dobj - Data Object Generated
     * @param dataObjectNode - DataObject Node generated out of Object Node
     */
    private void addDataObjectToList(DataObject dobj, DataObjectNode dataObjectNode) {
        
        // Add Data Object to List
        doLinkedList.add(dobj);
        
        //Clear Temporary Structures
        for (int i=0; i < this.childTypeContainer.length; i++){
            this.childTypeContainer[i] = null; //Clean up array for reuse
        }
        
        //Finalize Data Object Node
        this.dbreader.submitObjectForFinalization(dataObjectNode);

    }

    /**
     * Generic sleep method for the running thread
     */
    private void goSleep(long time) {
        if (!isThreadSleeping) {
            sLog.fine(" Slave Thread will sleep [" + time / 1000 + "] seconds till interrupted ...");
            try {
                isThreadSleeping = true;
                Thread.sleep(time);
                isThreadSleeping = false;
            } catch (InterruptedException ex) {
                isThreadSleeping = false;
                sLog.fine(" QMSlave Thread Interrupted. Will Create Data Objects");
            }
        }

        //Wake up only if list has fallen below threahold
        if (doLinkedList.size() >= PluginConstants.available_do) {
            this.dbreader.wakeUpMaster();
            goSleep(PluginConstants.slave_retry_freq);
        }
    }

    /**
     * Interrupt mechanism for the slave
     */
    public void wakeUpSlave() {
        if (isSlaveRunning) {
            if (isThreadSleeping) {
                sLog.fine("Slave waking up ... ");
                isThreadSleeping = false;
                this.interrupt();
            }
        } else {
            sLog.fine("Slave has already completed ... ");
        }
    }

    public boolean isSlaveRunning() {
        return this.isSlaveRunning;
    }

    public boolean isSlaveSleeping() {
        return this.isThreadSleeping;
    }

    public void stopSlaveThread() {
        try {
            sLog.severe(sLoc.x("PLG033: DataBase Query Process will STOP"));
            this.conn.close();
            isSlaveRunning = false;
            //Stop Object Finalise Thread
            this.dbreader.stopObjectFinalizer();
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("Failed To Close DB Connection \n{0}", ex));
        }
    }

    public boolean getAbnormalHaltStatus() {
        return this.isAbronmalHalt;
    }

    /**
     * Method to create QueryObject for QueryManager. Builds Seletables list, conditions and orderby for the QueryObject
     * using PK/FK and Metadata for the axion tables.
     * @param  rootPK - Primary key for the base table in the eView Object Hirarchy
     * @return QueryObject
     */
    private QueryObjectBuilder createQueryObjectBuilder() {
        QueryObjectBuilder qObjbuilder = new QueryObjectBuilder(this.conn);
        qObjbuilder.buildSelectables(filteredSelects);
        qObjbuilder.buildConditions();
        return qObjbuilder;
    }

    /**
     * Builds Seletables list, conditions and orderby for the QueryObject
     * using PK/FK and Metadata for the axion tables.
     * @param  rootPK - Primary key for the base table in the eView Object Hirarchy
     * @return QueryObject
     */
    private QueryObject buildQueryObject(QueryObjectBuilder qob, String rootPKColumn, String rootPKvalue) {
        qob.createNewQueryObject();
        qob.setSeletables();
        qob.setQueryConditions(rootPKvalue);
        //qob.buildOrderBy(mdmanager.getLookup().getRootName(), rootPKColumn);
        //qob.printQuerySQLString();
        return qob.getQueryObject();
    }

    private void insertSystemFields(DataObject dataObject, String[] systemFields) {
        for (int i = 0; i < systemFields.length; i++) {
            dataObject.add(i, systemFields[i]);
        }
    }

    private QueryObject buildQueryObject(QueryObjectBuilder qob, String rootPKColumn, String[] rootPKvalues) {
        qob.createNewQueryObject();
        qob.setSeletables();
        qob.setQueryConditions(rootPKvalues);
        //qob.buildOrderBy(mdmanager.getLookup().getRootName(), rootPKColumn);
        //qob.printQuerySQLString();
        return qob.getQueryObject();
    }

    /**
     * Copy Field Values from the Object Node to the Data Node in the order as per objectdef.xml (eview data model)
     * @param objectnode
     * @param dataobject
     */
    private DataObject copyFields(ObjectNode objectnode, DataObject dataobject, DataObjectNode donode) {
        ArrayList<Field> fields = mdservice.getFieldsList(objectnode.pGetTag());
        try {
            for (int i = 0; i < fields.size(); i++) {
                /*
                 * ObjectNode getField can process fields that are genuinely part of objectnode.
                 * Using objectnode.getField() for the fields injected (if any) into parents externally
                 * causes  an object exception. Thus, these fileds need to be filtered.
                 */
                ObjectField ofield = null;
                if (donode != null) {
                    if (!donode.isAttributeDefault(fields.get(i).getName())) {
                        ofield = objectnode.getField(fields.get(i).getName());
                    } else {
                        continue;
                    }
                } else {
                    ofield = objectnode.getField(fields.get(i).getName());
                }

                if (ofield != null) {
                    String ofieldval = null;
                    if (ofield.getValue() != null) {
                        ofieldval = ofield.getValue().toString();
                    }
                    sLog.fine("Adding Field to Data Object [ " + fields.get(i).getName() + "  ] Value : " + ofieldval);
                    dataobject.addFieldValue(ofieldval);
                } else {
                    LocalizedString msg = sLoc.x("Unable to find  [{0}] in ObjectField for ObjectNode {1}." +
                            " Verify if DataBase Schema has column [{0}] in the table {1}", fields.get(i).getName(), objectnode.pGetTag());
                    sLog.warn(msg);
                }
            }

        //Add Auto Generated Colum in the end of Data Object as this column is not generated with objectdef.xml (MetaData Base)
        //logger.fine("Adding Auto Gen Field [Name: " + objectnode.pGetTag() + "Id" +  "] to Data Object. Value : " + (objectnode.getField(objectnode.pGetTag() + "Id")).getValue().toString());
        //dataobject.addFieldValue(((ObjectField)objectnode.getField(objectnode.pGetTag() + "Id")).getValue().toString());
        } catch (ObjectException ex) {
            sLog.severe(sLoc.x("PLG034: Unable to process ObjectNode. Object Exception : " + ex.getMessage(), ex));
        }

        return dataobject;
    }

    /* ~~~~~~~~~~~~~~~~~ Data Object Creation using ASCII records ~~~~~~~~~~~~~~~~~~ */
    /**
     * Creates Data Objects by normalizing fields from the Object Node produced by the query manager.
     * Query Manager iterates through the Result Set and produces Object Node (Heavy Weight hirarchical objects)
     * for the each row of the result set. Object Node is then processed and relevent data is flattened out in the form of
     * good file format line. This line is further processed into the framework that validates and produces Data Objects that
     * get consumed by Data Analysis and Bulk Loader.
     * @param ObjectNode - Hirarchical object created by query manager for each resultset row.
     */
    private void createFlattenedHirarchy(DataObjectNode dataObjectNode) {
        try {
            sLog.fine("DataBaseDatareader : Using ObjectNode Normalization method");
            ObjectNode objectNode = dataObjectNode.getObjectNode();
            appendFields(objectNode);
            ArrayList allChildren = objectNode.getAllChildrenFromHashMap();
            if (allChildren != null) {
                for (int i = 0; i < allChildren.size(); i++) {
                    ObjectNode childnode = (ObjectNode) allChildren.get(i);
                    appendFields(childnode, childnode.pGetTag());
                }
            }
            boolean copyStr = true;
            for (int i = 0; i < dataObjStrBldr.length(); i++) {
                char c = dataObjStrBldr.toString().charAt(i);
                if (c == CHILD_CHAR) {
                    if (copyStr) {
                        copyStr = false;
                    } else {
                        copyStr = true;
                        i++;
                        c = dataObjStrBldr.toString().charAt(i);
                    }
                }
                if (copyStr) {
                    this.dataObjStrBldrTemp.append(c);
                }
            }

            doCleanupForTempSB();

            // Check if data object list is below threashold, in this case, keep adding to this list or else add and sleep
            DataObject dobj = this.dbreader.newDataObject(dataObjStrBldr.toString());
            if (doLinkedList.size() < PluginConstants.available_do) {
                addDataObjectToList(dobj, dataObjectNode);
            } else {
                dbreader.wakeUpMaster();
                addDataObjectToList(dobj, dataObjectNode);
                goSleep(PluginConstants.slave_retry_freq);
            }

            doCleanupForMainSB();

        } catch (InvalidRecordFormat ex) {
            sLog.severe(LocalizedString.valueOf("Invalid Record format :: " + dataObjStrBldr.toString()), ex);
        }

    }

    /**
     * Appends the fields for the Object Node
     * @param objectNode - Instance of ObjectNode
     */
    private void appendFields(ObjectNode objectNode) {
        for (int i = 0; i < objectNode.pGetFieldValues().size(); i++) {
            String ObjectFields = (objectNode.pGetFieldValues().get(i) == null) ? " " : objectNode.pGetFieldValues().get(i).toString();
            if ((i + 1) == objectNode.pGetFieldValues().size()) {
                dataObjStrBldr.append(ObjectFields);
            } else {
                dataObjStrBldr.append(ObjectFields + "|");
            }
        }
    }

    /**
     * Append fields from the child node in the given string builder.
     * Used for appending new child to the normalised record.
     * @param childNode - Hirarchical instance of the ObjectNode
     * @param childname - Name of the child to be added
     */
    private void appendFields(ObjectNode childNode, String childname) {
        int childNameIndex;
        if ((childNameIndex = dataObjStrBldr.toString().indexOf(childname)) != -1) {

            // Instance of this child is already present in the data object string builder
            dataObjStrBldrTemp.append(dataObjStrBldr.toString().substring(0, childNameIndex + childNode.pGetTag().length() + 1));
            appendFields(childNode, dataObjStrBldrTemp);
            dataObjStrBldrTemp.append(dataObjStrBldr.toString().substring(childNameIndex + childNode.pGetTag().length() + 1, dataObjStrBldr.length()));

            // Copy the value of temp into the main and clear temp
            doCleanupForTempSB();
        } else {

            // This is a first child instance in data object string builder
            dataObjStrBldr.append("#" + CHILD_CHAR + childNode.pGetTag() + CHILD_CHAR);
            appendFields(childNode, dataObjStrBldr);
        }
    }

    /**
     * Append fields from the child node in the given string builder.
     * Used for appending new instance of a child to the normalised record
     * @param childNode - Hirarchical instance of the ObjectNode
     * @param StrBldr - global/resuable string builder
     */
    private void appendFields(ObjectNode childNode, StringBuilder StrBldr) {
        StrBldr.append("$"); //Adding New Instance of a Child
        for (int i = 0; i < childNode.pGetFieldValues().size(); i++) {
            String ObjectFields = (childNode.pGetFieldValues().get(i) == null) ? " " : childNode.pGetFieldValues().get(i).toString();
            if ((i + 1) == childNode.pGetFieldValues().size()) {
                StrBldr.append(ObjectFields);
            } else {
                StrBldr.append(ObjectFields + "|");
            }
        }
    }

    /**
     * Clean up method for the temp string builders
     */
    private void doCleanupForTempSB() {
        dataObjStrBldr.delete(0, dataObjStrBldr.length());
        dataObjStrBldr.append(dataObjStrBldrTemp.toString());
        dataObjStrBldrTemp.delete(0, dataObjStrBldrTemp.length()); //Clean up temp StringBuilder for reuse
    }

    /**
     * Clean up method for the main string builders
     */
    private void doCleanupForMainSB() {
        dataObjStrBldr.delete(0, dataObjStrBldr.length());
    }
}
