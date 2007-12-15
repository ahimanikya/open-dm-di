/*
 * MetaDataManager.java
 *
 * Created on Sep 17, 2007, 12:09:41 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.dataobject.metadata;

import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.dm.dimi.util.PluginConstants;
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
                    lookup = Lookup.createLookup(objDef);
                    mdservice = new MetaDataService(lookup,objDef);
                } catch (AxionException ex) {
                    sLog.severe(LocalizedString.valueOf(sLoc.t("PLG001: Error Reading eview config file {0}",ex.getMessage())));
                } finally {
                    try {
                        bdis.close();
                    } catch (IOException ex) {
                        sLog.severe(LocalizedString.valueOf(sLoc.t("PLG002: Error Closing Axion BufferedDataInputStream \n{0}", ex)));
                    }
                }
            } else{
                sLog.severe(LocalizedString.valueOf(sLoc.t("PLG003: Unable to find file {0}",PluginConstants.EVIEW_CONFIG_FILE.getAbsolutePath())));
            }
        }else{
            sLog.severe(LocalizedString.valueOf(sLoc.t("PLG004: EView Config File is not available. Set the file using DataSourceReaderFactory.setEViewConfigFilePath()")));
            
        }
    }
}