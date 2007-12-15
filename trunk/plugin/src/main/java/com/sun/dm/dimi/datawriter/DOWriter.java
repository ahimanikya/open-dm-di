/*
 * DataObjectWriter.java
 * 
 * Created on Sep 4, 2007, 12:47:42 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datawriter;

import com.sun.mdm.index.dataobject.DataObject;



/**
 * Title:         CLASS DOWriter.java
 * Description:   Interface to be implemented by all the writers. 
 * Company:       Sun Microsystems
 * @author        Manish Bharani
 */
public interface DOWriter {
    
    public void write(DataObject dataobj);
    public void flush();

}
