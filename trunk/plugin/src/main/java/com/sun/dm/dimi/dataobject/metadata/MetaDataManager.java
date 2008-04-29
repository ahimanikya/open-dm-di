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
 * MetaDataManager.java
 *
 * Created on Sep 17, 2007, 12:09:41 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.dataobject.metadata;

import com.sun.dm.dimi.datareader.DefaultSystemFields;
import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
import com.sun.mdm.index.dataobject.objectdef.Field;
import com.sun.mdm.index.dataobject.objectdef.Lookup;
import com.sun.mdm.index.dataobject.objectdef.ObjectDefinition;
import com.sun.mdm.index.dataobject.objectdef.ObjectDefinitionBuilder;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;
import org.axiondb.AxionException;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataInputStream;

/**
 *
 * @author Manish Bharani
 */
public class MetaDataManager {
    
    private static Lookup lookup  = null;
    private static ObjectDefinition objDef = null;
    private static MetaDataService mdservice = null;
    private static MetaDataManager metadatamanager = null;
    
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(MetaDataManager.class.getName());
    private static Localizer sLoc = Localizer.get();
    
    private MetaDataManager() {
    }
    
    /**
     *
     * @return
     */
    public static MetaDataManager getMetaDataManager(){
        if (metadatamanager == null){
            sLog.fine("Initializing MetaData Manager ..");
            metadatamanager = new MetaDataManager();
            createObjectDefModel();
        }
        return metadatamanager;
    }
        
    public MetaDataService getMetaDataService(){
        return mdservice;
    }
    
    public static ObjectDefinition getObjectDefinition() {
        return objDef;
    }
    
    public static HashMap<String,ObjectDefinition> getObjectDefMap() {
        List<ObjectDefinition> childObjDefs = objDef.getChildren();
        Iterator<ObjectDefinition> iter = childObjDefs.iterator();
        HashMap objDefMap = new HashMap<String,ObjectDefinition>();
        while(iter.hasNext()) {
            ObjectDefinition childObjDef = iter.next();
            objDefMap.put(childObjDef.getName(),childObjDef);
        }
        return objDefMap;
    }
    
    private static void createObjectDefModel(){
        AxionFileSystem afs = new AxionFileSystem();
        File configfile = PluginConstants.EVIEW_CONFIG_FILE;
        if (configfile != null){
            if (configfile.exists()){
                BufferedDataInputStream bdis = null;
                try {
                    bdis = afs.openBufferedDIS(configfile);
                    objDef = new ObjectDefinitionBuilder().parse(bdis);
                    addExtraFieldsToParent(objDef);
                    lookup = Lookup.createLookup(objDef);
                    mdservice = new MetaDataService(lookup,objDef);
                } catch (AxionException ex) {
                    sLog.severe(sLoc.x("PLG001: Error Reading eview config file {0}",ex.getMessage()));
                } finally {
                    try {
                        bdis.close();
                    } catch (IOException ex) {
                        sLog.severe(sLoc.x("PLG002: Error Closing Axion BufferedDataInputStream \n{0}", ex));
                    }
                }
            } else{
                sLog.severe(sLoc.x("PLG003: Unable to find file {0}",PluginConstants.EVIEW_CONFIG_FILE.getAbsolutePath()));
            }
        }else{
            sLog.severe(sLoc.x("PLG004: EView Config File is not available. Set the file using DataSourceReaderFactory.setEViewConfigFilePath()"));
            
        }
    }
    
    private static void addExtraFieldsToParent(ObjectDefinition objDef){
        String[] sysfields = DefaultSystemFields.getDefaultSystemFields();
        for (int i=0; i < sysfields.length; i++){
            objDef.addField(i, createExtraFieldObj(sysfields[i]));
        }
    }
    
    private static Field createExtraFieldObj(String name){
        Field newfield = new Field();
        newfield.setName(name);
        return newfield;
    }    
    
}
