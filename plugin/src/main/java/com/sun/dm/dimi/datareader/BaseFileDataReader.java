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
 * FlatFileDataReader.java
 *
 * Created on Jul 31, 2007, 4:13:37 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datareader;

import com.sun.dm.dimi.dataobject.metadata.MetaDataManager;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.dm.dimi.util.PluginTools;
import com.sun.mdm.index.parser.ParserException;
import java.io.File;
import java.io.FileNotFoundException;
import net.java.hulp.i18n.Logger;
import org.axiondb.AxionException;
import org.axiondb.io.*;

/**
 * @Title:        CLASS BaseDataReader.java
 * @Description:  This is a base class for all readers (File and DataBase). Provides methods
 *                for basic operations that readers need to implement  Class is abstract.
 * @Company:      Sun Microsystems
 * @author        Manish Bharani
 */
public abstract class BaseFileDataReader extends BaseDBDataReader{
    
    File mFileObj = null;
    BufferedDataInputStream bdInputStream = null;
    MetaDataManager mdmanager = MetaDataManager.getMetaDataManager();
    
    //logger
    private static Logger sLog = LogUtil.getLogger(BaseFileDataReader.class.getName());
    Localizer sLoc = Localizer.get();
    
    /**
     * Constructor for the BaseDataReader
     */
    public BaseFileDataReader() {
    }
    
    /**
     * Constructor for the BaseDataReader
     * @param filepath - Absolute path to the directory where file can be found
     * @param filename - String filename of the file to be read
     * @param specialMode - parse records in normal mode or special mode
     */
    public BaseFileDataReader(String filepath, String filename, boolean specialMode) throws ParserException, FileNotFoundException{
        super(specialMode);
        if (PluginTools.validatePath(filepath, filename)){
            String fullFilePath = filepath + PluginConstants.fs + filename;
            mFileObj = new File(fullFilePath);
            initReader();
        }
    }
    
    /**
     * Construetor for the Class BaseDataReader
     * @param fileObj - File object of the file to be read
     * @param specialMode
     */
    public BaseFileDataReader(File fileObj, boolean specialMode){
        super(specialMode);
        mFileObj = fileObj;
        initReader();
    }
    
    /**
     * Performs initialization for the Axion File System readers
     */
    private void initReader(){
        try {
            AxionFileSystem axionFileSys = new AxionFileSystem();
            this.bdInputStream = axionFileSys.openBufferedDIS(this.mFileObj);
        } catch (AxionException ex) {
            sLog.severe(sLoc.x("Axion Exception : " + ex.getMessage()));
        }
    }
        
}
