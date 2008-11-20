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
 * GoodFileDataReader.java
 *
 * Created on Jul 31, 2007, 5:35:00 PM
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
import com.sun.mdm.index.dataobject.objectdef.Field;
import com.sun.mdm.index.parser.ParserException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import net.java.hulp.i18n.Logger;
import org.axiondb.AxionException;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataInputStream;

/**
 * Title:         CLASS GoodFileDataReader.java
 * Description:   This class is a reader for Good File Formats.
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class GoodFileDataReader extends BaseFileDataReader {

    //int filedbtype = -1;
    int filedbtype = PluginConstants.GOOD_FILE_DATASOURCE;
    //logger
    private static Logger sLog = LogUtil.getLogger(GoodFileDataReader.class.getName());
    Localizer sLoc = Localizer.get();

    /**
     * Constructor for GoodFileDataReader
     */
    public GoodFileDataReader() {
    }

    /**
     * Constructor for GoodFileDataReader
     * @param filepath
     * @param filename
     * @param specialMode
     */
    public GoodFileDataReader(String filepath, String filename, boolean specialMode) throws ParserException, FileNotFoundException {
        super(filepath, filename, specialMode);
        //filedbtype = executeRawFileCheck();
    }

    /**
     * Constructor for GoodFileDataReader
     * @param fileObj
     * @param specialMode
     */
    public GoodFileDataReader(File fileObj, boolean specialMode) {
        super(fileObj, specialMode);
        //filedbtype = executeRawFileCheck();
    }

    /**
     * Returns the list of data objects
     * @return DataObject
     * @throws com.sun.mdm.index.dataobject.InvalidRecordFormat 
     */
    public DataObject readDataObject() throws InvalidRecordFormat {
        String recordStr = readRecordString(bdInputStream);

        if (recordStr != null) {
            DataObject dObj = newDataObject(recordStr);
            /*
            if (this.filedbtype == PluginConstants.RAW_FILE_DATASOURCE) {
                // Inject System fields into DO
                for (int i = 0; i < DefaultSystemFields.getDefaultSystemFields().length; i++) {
                    dObj.add(0, null);
                }
            }
            */
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
     * @return DataSourceType
     */
    public int getDataSourceType() {
        return filedbtype;
    }

    /**
     * Reads the file record string
     */
    private String readRecordString(BufferedDataInputStream bdis) {
        try {
            String record = bdis.readLine();
            if (record == null || !requireSpecialProcessing(record)) {
                return record;
            } else {
                while (true) {
                    if (checkValidRecord(record)) {
                        return record;
                    } else {
                        record = record + DBDelimiters.NEW_LINE + bdis.readLine();
                    }
                }
            }
        } catch (IOException ex) {
            sLog.severe(sLoc.x("PLG019: Unable to read the Good file \n{0}", ex.getMessage()));
        }
        return null;
    }

    /*
     * This method is used to differentiate between raw file and good file reading.
     * Raw file would not have system fields appended to the root object head.
     * Good file will have the systems fileds already generated and available for reading.
     * object.xml field could is used to create this difference.
     */
    /*
    private int executeRawFileCheck() {
        sLog.infoNoloc("Check to differentiate between raw file and good file reading ...");
        int metaFieldCountRoot = 0;
        int recordFieldCountRoot = 0;
        int stan_phon_field_count_root = 0;
        //Check of no of fields in the eView root.
        @SuppressWarnings("static-access")
        ArrayList fields = super.mdmanager.getObjectDefinition().getFields();
        if (fields != null) {
            for ( int i = 0; i < fields.size(); i++) {
                sLog.fine("ObjDef Field [ " + i + " ] : " + fields.get(i).toString());
                Field f = (Field) fields.get(i);
                // Leave aside standardization and Phonetization fileds from object.xml
                if ((f.getName().indexOf(PluginConstants.STAN_PATTERN) == -1) && (f.getName().indexOf(PluginConstants.PHON_PATTERN) == -1)) {
                    metaFieldCountRoot++;
                } else {
                    stan_phon_field_count_root++;
                }
            }
            sLog.infoNoloc("Root Field Count (Including system fields excluding Stan and Phon fields) : " + metaFieldCountRoot);
            sLog.infoNoloc("Root Field Count (Standardization and Phonetization only) : " + stan_phon_field_count_root);
        }

        //Check the first file record being read. Count the no of fields in the file
        BufferedDataInputStream bdis = null;
        try {
            AxionFileSystem axionFileSys = new AxionFileSystem();
            bdis = axionFileSys.openBufferedDIS(mFileObj);
            String firstrecord = readRecordString(bdis);
            if (firstrecord != null) {
                sLog.fine("First Record from the file read :: " + firstrecord);
                //Tokanize fields and find the count
                String parent = null;
                if (firstrecord.indexOf("#") != -1) {
                    parent = firstrecord.substring(0, firstrecord.indexOf("#"));
                } else {
                    parent = firstrecord;
                }

                StringTokenizer st = new StringTokenizer(parent, DBDelimiters.PIPE, true);
                while (st.hasMoreTokens()) {
                    String tkn = st.nextToken();
                    if (tkn.equals(DBDelimiters.PIPE)) {
                        recordFieldCountRoot++;
                    }
                }
                recordFieldCountRoot++; // Add extra to compensate start count
                sLog.infoNoloc("No of Tokens in the file record schema :: " + recordFieldCountRoot);
            }
        } catch ( AxionException ex) {
            sLog.severe(sLoc.x("Axion Exception : " + ex.getMessage()));
        } finally {
            try {
                bdis.close();
            } catch ( IOException ex) {
                sLog.warnNoloc("Unable to close stream :" + ex.getMessage());
            }
        }

        //Ascertain if its a raw file or good file. Raw file will not contain system fields.
        if (metaFieldCountRoot == recordFieldCountRoot) {
            // This use case is met when there are no standerdization/phonetization records are written to the good file being read.
            sLog.infoNoloc("This is a Good file already containing System fields !");
            return PluginConstants.GOOD_FILE_DATASOURCE;
        } else if (recordFieldCountRoot < metaFieldCountRoot) {
            int sysfieldsSize = DefaultSystemFields.getDefaultSystemFields().length;
            if ((metaFieldCountRoot - sysfieldsSize) == recordFieldCountRoot) {
                // Its a raw file. This would need system field injection to data object
                sLog.infoNoloc("This is a Raw file and does not contain System fields !");
                return PluginConstants.RAW_FILE_DATASOURCE;
            } else {
                sLog.severe(sLoc.x("PLG090: File being read does not conform to eView datamodel (object.xml)." +
                        "\n\tFile contains lesser fields than expected." +
                        "\n\tCorrect this issue and re-run the application"));
                System.exit(0);
            }
        } else {
            // This may be a valid case when Stan/Phon fileds are written to a good file.
            if (recordFieldCountRoot == (metaFieldCountRoot + stan_phon_field_count_root)) {
                sLog.infoNoloc("This is a Good file already containing System fields and Stan/Phon fileds !");
                return PluginConstants.GOOD_FILE_DATASOURCE;
            } else {
                sLog.severe(sLoc.x("PLG091: File being read does not conform to eView datamodel (object.xml)." +
                        "\n\tFile contains more fields than expected." +
                        "\n\tCorrect this issue and re-run the application"));
                System.exit(0);
            }
        }
        return -1;
    }
    */

    public void reset() throws Exception {

    }
}
