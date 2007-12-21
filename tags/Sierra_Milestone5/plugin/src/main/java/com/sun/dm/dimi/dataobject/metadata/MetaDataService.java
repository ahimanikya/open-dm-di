/*
 * MetaDataService.java
 *
 * Created on Oct 15, 2007, 3:40:53 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.dataobject.metadata;

import com.sun.dm.dimi.util.Localizer;
import com.sun.dm.dimi.util.LogUtil;
import com.sun.mdm.index.dataobject.objectdef.Field;
import com.sun.mdm.index.dataobject.objectdef.Lookup;
import com.sun.mdm.index.dataobject.objectdef.ObjectDefinition;
import java.util.ArrayList;
import java.util.HashMap;
import net.java.hulp.i18n.LocalizedString;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class MetaDataService {
    
    Lookup lookup = null;
    ObjectDefinition objDef = null;
    HashMap<String,ArrayList<Field>> objFieldMap = null;
    
    /**
     * logger
     */
    private static Logger sLog = LogUtil.getLogger(MetaDataService.class.getName());
    private static Localizer sLoc = Localizer.get();
    
    protected MetaDataService() {
        sLog.fine("Initializing MetaData Service ...");
        objFieldMap = new HashMap();
    }
    
    protected MetaDataService(Lookup lookup, ObjectDefinition objDef){
        this();
        this.lookup = lookup;
        this.objDef = objDef;
    }
    
    
    public String getHirarchicalRootName(){
        return objDef.getName();
    }
    
    public HashMap getLookupMap(){
        return lookup.getLookupMap();
    }
    
    public HashMap getChildIndexMap(){
        return lookup.getChildIndex();
    }
    
    
    /**
     * Returns the list of Field Objects in the order as modelled into objectdef.xml
     * @param objectname 
     * @return 
     */
    public ArrayList getFieldsList(String objectname){
        if (this.objFieldMap.containsKey(objectname)){
            //Check if the field was already created earlier
            return this.objFieldMap.get(objectname);
        } else{
            //Create this list now and keep it safe for use by other calls
            if (objectname.equals(this.objDef.getName())){
                // Its a parent 
                this.objFieldMap.put(objectname, this.objDef.getFields());
                return this.objDef.getFields();
            } else{
                // Its a child , find and process it
                for (int i=0; i < this.objDef.getChildren().size(); i++){
                    ObjectDefinition childObjDef = this.objDef.getchild(i);
                    if (objectname.equals(childObjDef.getName())){
                        this.objFieldMap.put(objectname, childObjDef.getFields());
                        return childObjDef.getFields();
                    }
                }
            }
            return null;
        }
    }
        
        /**
         * populate the lookupMap
         *
         */
        private void createLooupMap(HashMap<String, HashMap<String, Integer>> lmap,
                ObjectDefinition context, String prefix) {
            
            lmap.put(prefix, createFieldMap(context));
            
            for (int i = 0; i < context.getChildren().size(); i++) {
                
                ObjectDefinition child = context.getChildren().get(i);
                String cname = child.getName();
                String key = prefix + "." + cname;
                createLooupMap(lmap, child, key);
            }
        }
        
        /**
         * create the field map for a given ObjectDefinition
         *
         * @param context
         * @return
         */
        private HashMap<String, Integer> createFieldMap(ObjectDefinition context) {
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            ArrayList<Field> fields = context.getFields();
            
            for (int i = 0; i < fields.size(); i++) {
                map.put(fields.get(i).getName(), i);
            }
            
            return map;
        }
        
    }
