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
 * DataSourceReaderFactory.java
 *
 * Created on Jul 31, 2007, 4:00:42 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.qmquery.SelectableFilter;
import com.sun.dm.dimi.util.DBDelimiters;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.dm.dimi.util.PluginTools;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.mdm.index.parser.ParserException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;
/**
 * Title:         CLASS DataSourceReaderFactory.java
 * Description:   This is a factory class that allows data analysis/bulk loader to connect and read data sources.
 *                The class returns list of data objects created from the data from the data source.
 *                Currently it reads, Good File Format/Reject File Format/ Axion Database
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class DataSourceReaderFactory {
    private static GlobalDataObjectReader doReader = null;
    private static String configpath = null;
    
    //logger
    private static Logger sLog = LogUtil.getLogger(DataSourceReaderFactory.class.getName());
    private static Localizer sLoc = Localizer.get();
    
    /**
     * Constructor for the DataSourceReaderFactory
     */
    public DataSourceReaderFactory() {
    }
    
    /**
     * Static method to provide hook for a file reader
     * @param fileObj 
     * @param isGoodFile - user specifies if the good file is what needs to be read
     * @param specialMode - parse records in normal mode or special mode
     * @return GlobalDataObjectReader - interface instance
     */
    public static GlobalDataObjectReader getNewDataObjectReader(File fileObj, boolean isGoodFile, boolean specialMode){
        if (isGoodFile){
            sLog.fine("Creating a Good File Reader ..");
            doReader = new GoodFileDataReader(fileObj, specialMode);
        } else {
            sLog.fine("Creating a Reject File Reader ..");
            doReader = new RejectFileDataReader(fileObj, specialMode);
        }
        return doReader;
    }    
    
    /**
     * Static method to provide hook for a file reader
     * @param filepath
     * @param filename
     * @param isGoodFile - user specifies if the good file is what needs to be read
     * @param specialMode - parse records in normal mode or special mode
     * @return GlobalDataObjectReader - interface instance
     */    
    public static GlobalDataObjectReader getNewDataObjectReader(String filepath, String filename, boolean isGoodFile, boolean specialMode) throws ParserException, FileNotFoundException{
        if (isGoodFile){
            sLog.fine("Creating a Good File Reader ..");
            doReader = new GoodFileDataReader(filepath, filename, specialMode);
        } else {
            sLog.fine("Creating a Reject File Reader ..");
            doReader = new RejectFileDataReader(filepath, filename, specialMode);
        }
        return doReader;
    }
    
    /**
     * Static method to provide hook for a database reader
     * @param databaseUri - Uri of DB being connected
     * @param filter - Any filters of DB columns
     * @param specialMode - parse records in normal mode or special mode
     * @return GlobalDataObjectReader - interface instance
     */    
    public static GlobalDataObjectReader getNewDataObjectReader(String databaseUri, SelectableFilter filter, boolean specialMode) {
        sLog.fine("Creating Axion DB reader ..");
        doReader = new DatabaseDataReader(databaseUri,filter,specialMode);
        return doReader;
    }
    
    /**
     * Static method to provide hook for a database reader
     * @param dbname 
     * @param dbdir 
     * @param filter - Object for overriding selectables from the database (null if not required)
     * @param specialMode 
     * @return DataSourceReader - interface instance
     */
    public static GlobalDataObjectReader getNewDataObjectReader(String dbname, String dbdir, SelectableFilter filter, boolean specialMode) {
        sLog.fine("Creating Axion DB reader ..");
        doReader = new DatabaseDataReader(dbname, dbdir, filter, specialMode);
        return doReader;
    }
    
    /**
     * This method is used by cleanser/profiles to ascertain type of reader to use for the given data source pool
     * @param dataSourceURI
     * @param specialMode
     * @return GlobalDataObjectReader
     * @throws java.io.IOException
     */
    public static GlobalDataObjectReader getDataSourceReader(String dataSourceURI, boolean specialMode) throws IOException {
        GlobalDataObjectReader dsReader = null;
        int datasourceType = getDataSourceType(dataSourceURI);
        if(datasourceType==PluginConstants.JDBC_DATASOURCE) {
            dsReader = getNewDataObjectReader(dataSourceURI, null, specialMode);
        } else if(datasourceType == PluginConstants.REJECT_FILE_DATASOURCE) {
            dsReader = getNewDataObjectReader(new File(dataSourceURI), false, specialMode);
        } else if(datasourceType == PluginConstants.GOOD_FILE_DATASOURCE) {
            dsReader = getNewDataObjectReader(new File(dataSourceURI), true, specialMode);
        }
        return dsReader;
    }
    
    private static int getDataSourceType(String dataSourceLoc) throws IOException {
        File f = new File(dataSourceLoc);
        int dataSourceType = (f.isDirectory()) ? PluginConstants.JDBC_DATASOURCE : getFileSourceType(dataSourceLoc);
        return dataSourceType;
    }    
    
    private static int getFileSourceType(String fileURI) throws IOException {
        int fileType =  -1;
        BufferedReader bis = null;
    
        try {
            bis = new BufferedReader(new FileReader(new File(fileURI)));
            String firstLine = bis.readLine();
            if( firstLine != null ) {
                fileType = ( firstLine.indexOf(DBDelimiters.OPEN_BRACE_CHAR) != - 1) ? 
                        PluginConstants.REJECT_FILE_DATASOURCE : PluginConstants.GOOD_FILE_DATASOURCE;
            }
        } catch(IOException e) {
            sLog.info(sLoc.x("Invalid DataSource \n{0}", e));
            throw e;
        } finally {
            bis.close();
        }
        return fileType;
    }    
    
    /**
     * Sets eViewConfig File Object (objectmap.xml conventionally)
     * @param configpath 
     * @param configfilename 
     * @return boolean
     */
    public static boolean setEViewConfigFilePath(String configpath, String configfilename) throws ParserException, FileNotFoundException{
        if(PluginTools.validateDir(configpath)){
            if(PluginTools.validateFile(configpath, configfilename, true)){
                DataSourceReaderFactory.configpath = configpath + File.separator + configfilename;
                return true;
            }
        }
        return false;
    }
    
    /**
     * Sets eViewConfig File Object Path (object.xml conventionally)
     * @param configfilepath 
     * @return boolean
     */
    public static boolean setEViewConfigFilePath(String configfilepath) throws ParserException, FileNotFoundException{
        File f = new File(configfilepath);
        if (PluginTools.validateAndSeteViewConfigFile(f)){
            return true;
        }
        return false;
    }    
    
    /**
     * Gets eViewConfig File Object absolute path (object.xml conventionally)
     * @return String - abs path for object.xml
     */
    public static String getEViewConfigFilePath() {
        return PluginConstants.EVIEW_CONFIG_FILE.getAbsolutePath();
    }
    
    public static void main(String[] args) throws IOException {
        String dataSource = "jdbc:axion:testdb:C:\temp\testdb";
        String goodFileSource = "C:\\temp\\goodfile.txt";
        String rejectFileSource = "C:\\temp\\rejectfile.txt";
        System.out.println("jdbc source: " + getDataSourceType(dataSource));
        System.out.println("good file source: " + getDataSourceType(goodFileSource));
        System.out.println("reject file source: " + getDataSourceType(rejectFileSource));
    }
    
}
