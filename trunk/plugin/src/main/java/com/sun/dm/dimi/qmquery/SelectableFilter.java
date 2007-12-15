/*
 * SeletableFilter.java
 * 
 * Created on Sep 26, 2007, 3:37:53 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.qmquery;

import com.sun.mdm.index.util.Logger;
import java.util.ArrayList;

/**
 *
 * @author Manish Bharani
 */
public class SelectableFilter {
    
    ArrayList seletables = null;
    
    /**
     * logger
     */
    private static Logger sLog = Logger.getLogger(SelectableFilter.class.getName());    

    public SelectableFilter() {
        seletables = new ArrayList();
    }
    
    public void addSeletable(String qualifiedPath){
        seletables.add(qualifiedPath);
    }
    
    public void addSeletable(String[] filterlist){
        for (int i=0; i < filterlist.length; i++){
            seletables.add(filterlist[i]);
        }
    }
    
    public ArrayList getAddedSelectables(){
        return seletables;
    }
}
