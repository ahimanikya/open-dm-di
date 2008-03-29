/*
 * DataObjectWriterFactory.java
 *
 * Created on Sep 4, 2007, 12:39:03 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datawriter;

import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.dm.dimi.util.PluginTools;
import java.io.File;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
/**
 * Title:         CLASS DataObjectWriterFactory.java
 * Description:   This class is a factory for Data Object FlatFile Writers
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public class DataObjectWriterFactory {
    
    private static DOWriter doWriter = null;
        
    /**
     * Constructor for the DataObjectWriterFactory
     */
    public DataObjectWriterFactory() {
    }
    
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(DataObjectWriterFactory.class.getName());
    
    
    /**
     * Gets a New File Writer Using Axion Buffered I/O
     * @param fileObj
     * @param isGoodFile
     * @return DOWriter
     */
    public static DOWriter getNewDataObjectWriter(File fileObj, boolean isGoodFile){
        if (isGoodFile){
            sLog.fine(LocalizedString.valueOf("Creating a new Good File writer .."));
        } else{
            sLog.fine(LocalizedString.valueOf("Creating a new Reject File file writer .."));
        }
        DOWriter doWriter = new DOFileWriter(fileObj, isGoodFile, true);
        return doWriter;
    }
    
    /**
     * Gets a New File Writer Using Axion Buffered I/O
     * @param filename
     * @param filepath
     * @param isGoodFile
     * @return DOWriter
     */
    public static DOWriter getNewDataObjectWriter(String filepath, String filename, boolean isGoodFile){
        if (isGoodFile){
            sLog.fine(LocalizedString.valueOf("Creating a new Good File writer .."));
        } else{
            sLog.fine(LocalizedString.valueOf("Creating a new Reject File file writer .."));
        }
        
        DOWriter doWriter = null;
        
        if (PluginTools.validateDir(filepath)){
            File f = new File(filepath + PluginConstants.fs + filename);
            doWriter = new DOFileWriter(f, isGoodFile, true);
        }
        
        return doWriter;
    }
}
