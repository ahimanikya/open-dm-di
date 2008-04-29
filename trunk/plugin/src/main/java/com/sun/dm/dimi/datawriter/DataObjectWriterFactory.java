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
