/*
 * ReaderBase.java
 *
 * Created on Oct 16, 2007, 3:18:31 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.util.DBDelimiters;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.mdm.index.dataobject.ChildType;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public abstract class BaseDBDataReader implements GlobalDataObjectReader {

    //List of Data Object Created
    List<DataObject> doLinkedList = Collections.synchronizedList(new LinkedList());
    // List of data objects and object nodes available for finalization
    List<Object> usedObjectsList = Collections.synchronizedList(new LinkedList());
    // Object Finalizer Slave Thread
    ObjectFinalizerSlave objfin = null;
    private boolean specialMode = false;
    //Logger
    private static Logger sLog = LogUtil.getLogger(BaseDBDataReader.class.getName());

    /**
     * BaseDBDataReader Constructor
     */
    public BaseDBDataReader() {
        // Create Finalizer Instance
        objfin = new ObjectFinalizerSlave(this);
        objfin.start();
    }

    /**
     * BaseDBDataReader Constructor
     * @param specialmode 
     */
    public BaseDBDataReader(boolean specialmode) {
        this();
        specialMode = specialmode;
    }

    /**
     * check whether the full record or partial record has been read
     * @param record
     * @return
     */
    protected boolean checkValidRecord(String record) {
        int tildaCount = 0;
        for (int i = record.length() - 1; i > -1; i--) {
            if (record.charAt(i) == DBDelimiters.TILDA_CHAR) {
                tildaCount++;
            } else {
                break;
            }
        }

        int j = tildaCount % 2;
        return j == 0;
    }

    /**
     * Returns the new data record from the line read from the Good/Reject file
     * @param recordStr
     * @return
     * @throws InvalidRecordFormat
     */
    protected DataObject newDataObject(String recordStr) throws InvalidRecordFormat {
        // TODO Auto-generated method stub

        DataObject r = new DataObject();

        if (requireSpecialProcessing(recordStr)) {
            doSpecialProcessing(r, recordStr);
        } else {
            updateDataObject(r, recordStr);
        }

        return r;
    }

    /**
     * Returns if the record needs special processing
     * @param recordStr
     * @return
     */
    protected boolean requireSpecialProcessing(String recordStr) {
        return specialMode;
    }

    /**
     * Does special processing for the records read from the file
     * @param r
     * @param recordStr
     * @throws InvalidRecordFormat
     */
    private void doSpecialProcessing(DataObject r, String record)
            throws InvalidRecordFormat {

        int i = doSpecialProcessingForFields(r, record);

        if (i >= record.length()) {
            return;
        }

        doSpecialProcessingForChildTypes(r, record.substring(i));

    }

    /**
     * Do Special Processing for Data Object fields
     * @param r
     * @param record
     * @return
     */
    private int doSpecialProcessingForFields(DataObject r, String record) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < record.length(); i++) {
            switch (record.charAt(i)) {

                case (DBDelimiters.PIPE_CHAR):
                    r.addFieldValue(sb.toString());
                    sb = new StringBuilder();

                    break;

                case (DBDelimiters.HASH_CHAR):
                    r.addFieldValue(sb.toString());
                    return i;

                case (DBDelimiters.TILDA_CHAR):
                    i++;
                    sb.append(record.charAt(i));
                    break;

                default:
                    sb.append(record.charAt(i));
            }
        }
        r.addFieldValue(sb.toString());
        return record.length();
    }

    /**
     * Do Special Processing for Child Types
     */
    private void doSpecialProcessingForChildTypes(DataObject parent,
            String record) throws InvalidRecordFormat {

        StringBuilder sb = new StringBuilder();

        ChildType ct = null;
        DataObject child = null;
        for (int i = 0; i < record.length(); i++) {
            switch (record.charAt(i)) {

                case (DBDelimiters.PIPE_CHAR):
                    child.addFieldValue(sb.toString());
                    sb = new StringBuilder();

                    break;

                case (DBDelimiters.DOLLAR_CHAR):
                    if (child != null) {
                        child.addFieldValue(sb.toString());
                        sb = new StringBuilder();
                    }
                    child = new DataObject();
                    ct.addChild(child);
                    break;
                case (DBDelimiters.HASH_CHAR):
                    ct = new ChildType();
                    parent.addChildType(ct);

                    break;

                case (DBDelimiters.TILDA_CHAR):
                    i++;
                    sb.append(record.charAt(i));
                    break;

                default:
                    sb.append(record.charAt(i));
            }
        }

        child.addFieldValue(sb.toString());
    }

    /**
     * Update Data Objects
     * @param recordStr
     * @param r
     */
    private void updateDataObject(DataObject r, String recordStr)
            throws InvalidRecordFormat {

        try {
            int i = updateDataObjectFields(r, recordStr);
            if (i >= recordStr.length()) {
                return;
            }
            updateDataObjectChildTypes(r, recordStr.substring(i));
        } catch (Exception e) {
            throw new InvalidRecordFormat(
                    "bad record format, use special mode to read records");

        }
    }

    /**
     * Update DataObject Fileds
     * @param r - Parent Data Object
     * @param record - String Record
     */
    private int updateDataObjectFields(DataObject r, String record) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < record.length(); i++) {
            switch (record.charAt(i)) {

                case (DBDelimiters.PIPE_CHAR):
                    r.addFieldValue(sb.toString());
                    sb = new StringBuilder();

                    break;

                case (DBDelimiters.HASH_CHAR):
                    r.addFieldValue(sb.toString());
                    return i;

                default:
                    sb.append(record.charAt(i));
            }
        }
        r.addFieldValue(sb.toString());
        return record.length();
    }

    /**
     * Update DataObject Child Types
     * @param parent - Parent Data Object
     * @param record - String Record
     */
    private void updateDataObjectChildTypes(DataObject parent, String record) {
        StringBuilder sb = new StringBuilder();

        ChildType ct = null;
        DataObject child = null;
        for (int i = 0; i < record.length(); i++) {
            switch (record.charAt(i)) {

                case (DBDelimiters.PIPE_CHAR):
                    child.addFieldValue(sb.toString());
                    sb = new StringBuilder();

                    break;

                case (DBDelimiters.DOLLAR_CHAR):
                    if (child != null) {
                        child.addFieldValue(sb.toString());
                        sb = new StringBuilder();
                    }
                    child = new DataObject();
                    ct.addChild(child);
                    break;
                case (DBDelimiters.HASH_CHAR):
                    ct = new ChildType();
                    parent.addChildType(ct);

                    break;

                default:
                    sb.append(record.charAt(i));
            }
        }

        child.addFieldValue(sb.toString());
    }

    /**
     * This method takes care of finalizing data objectnode created by Query Manager.
     * @param dataObjectNode
     */
    public void submitObjectForFinalization(Object candidateObject) {
        usedObjectsList.add(candidateObject);
    }

    protected void stopObjectFinalizer() {
        sLog.infoNoloc("Stopping Finalizer with un-finalized objects. No of objects remaining : " + usedObjectsList.size());
        this.objfin.stopObjectFinalizer();
    }
}
