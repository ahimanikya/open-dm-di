/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datareader;

import com.sun.mdm.index.objects.ObjectNode;
import java.util.HashMap;

/**
 *
 * @author Manish
 */
public class DataObjectNode {
    
    private String[] defaultFieldsValue = null;
    private ObjectNode on = null;
    private int extFldLen = DefaultSystemFields.getDefaultSystemFields().length;
    private String[] defaultFieldsNames = DefaultSystemFields.getDefaultSystemFields();
    
    public DataObjectNode(){
        defaultFieldsValue = new String[extFldLen];
    }
    
    public void setSystemFieldValue(int index, String value ){
        defaultFieldsValue[index] = value;
    }
    
    public String[] getDefaultSystemFields(){
        return defaultFieldsValue;
    }
    
    public void setObjectNode(ObjectNode objectnode){
        this.on = objectnode;
    }
    
    public ObjectNode getObjectNode(){
       return this.on;
    }
    
    public boolean isAttributeDefault(String attrib) {
        for (int i=0; i < extFldLen; i++){
            if (defaultFieldsNames[i].equals(attrib)){
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Default System Fields : ");
        for (int i=0; i<extFldLen; i++){
            sb.append("\n     " + defaultFieldsValue[i]);
        }
        sb.append(on.toString());
        return sb.toString();
    }
    
    

}
