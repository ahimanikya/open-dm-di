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
import com.sun.mdm.index.dataobject.DataObject;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import com.sun.mdm.index.parser.ParserException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 * Title:         CLASS GoodFileDataReader.java
 * Description:   This class is a reader for Good File Formats.
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class GoodFileDataReader extends BaseFileDataReader {
    
    //Constants
    private String DATA_SOURCE_TYPE = "GOODFILE";
    
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
    }
    
    /**
     * Constructor for GoodFileDataReader
     * @param fileObj
     * @param specialMode
     */
    public GoodFileDataReader(File fileObj, boolean specialMode){
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
            return newDataObject(recordStr);
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
    public String getDataSourceType() {
        return this.DATA_SOURCE_TYPE;
    }
    
    /**
     * Reads the file record string
     */
    private String readRecordString(){
        try {
            String record = bdInputStream.readLine();
            if (record == null || !requireSpecialProcessing(record)) {
                return record;
            }
            else {
                while (true) {
                    if (checkValidRecord(record)) {
                        return record;
                    }
                    else {
                        record = record + DBDelimiters.NEW_LINE + bdInputStream.readLine();
                    }
                }
            }
        }
        catch (IOException ex) {
            sLog.severe(sLoc.x("PLG019: Unable to read the Good file \n{0}", ex));
        }
        return null;
    }    
    
}
