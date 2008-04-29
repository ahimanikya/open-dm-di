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
 * RejectFileDataReader.java
 *
 * Created on Jul 31, 2007, 5:35:59 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.util.DBDelimiters;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import com.sun.mdm.index.parser.ParserException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 * Title:         CLASS RejectFileDataReader.java
 * Description:   This class is a reader for Reject File Formats.
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class RejectFileDataReader extends BaseFileDataReader {

    //CONSTANTS
    private static final String ERROR_FIELD_BEGIN_CHAR = "{";
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(RejectFileDataReader.class.getName());
    Localizer sLoc = Localizer.get();

    /**
     * Constructor for GoodFileDataReader
     */
    public RejectFileDataReader() {
    }

    /**
     * Construstor for RejectFileDataReader
     * @param filepath
     * @param filename
     * @param specialMode
     */
    public RejectFileDataReader(String filepath, String filename, boolean specialMode) throws ParserException, FileNotFoundException {
        super(filepath, filename, specialMode);
    }

    /**
     * Construstor for RejectFileDataReader
     * @param fileObj
     * @param specialMode
     */
    public RejectFileDataReader(File fileObj, boolean specialMode) {
        super(fileObj, specialMode);
    }

    /**
     * Returns the list of data objects
     * @return DataObject
     * @throws com.sun.mdm.index.dataobject.InvalidRecordFormat 
     */
    public DataObject readDataObject() throws InvalidRecordFormat {
        String recordStr = readRecordString();

        if (recordStr != null) {
            DataObject dObj = newDataObject(recordStr);
            // Inject System fields into DO
            for (int i = 0; i < DefaultSystemFields.getDefaultSystemFields().length; i++) {
                dObj.add(0, null);
            }
            return dObj;
        } else {
            stopObjectFinalizer();
        }
        return null;
    }

    /**
     * Closes the reader stream
     * @throws java.lang.Exception
     */
    public void close() throws Exception {
        bdInputStream.close();
    }

    /**
     * Returns the Data Source Type
     * @return String - Data Type Source
     */
    public int getDataSourceType() {
        return PluginConstants.REJECT_FILE_DATASOURCE;
    }

    public static String trimErrorField(String record) {
        String trimmedRecord = null;
        if (record != null && !"".equalsIgnoreCase(record)) {
            int indexOfErrorField = record.indexOf(ERROR_FIELD_BEGIN_CHAR);
            trimmedRecord = record.substring(0, indexOfErrorField - 1);
        }
        return trimmedRecord;
    }

    /**
     * Reads the file record string
     */
    private String readRecordString() {
        try {
            String record = bdInputStream.readLine();


            if (record == null || !requireSpecialProcessing(record)) {
                return trimErrorField(record);
            } else {
                while (true) {
                    if (checkValidRecord(record)) {
                        return trimErrorField(record);
                    } else {
                        record = record + DBDelimiters.NEW_LINE + bdInputStream.readLine();
                    }
                }
            }
        } catch (IOException ex) {
            sLog.severe(sLoc.x("PLG020: Unable to read the Reject file \n{0}", ex));
        }
        return null;
    }

    public static void main(String[] args) {
        String rejectfile = "62614|MANISH|||BHARANI|||||00000002#$STD|00000002|2506 Callan Vale Apt 103|||||||||00002|{ Executing Rule Step Name = DataLength :Failure Reason = isMore = false::}";
        System.out.println(trimErrorField(rejectfile));
    }
}
