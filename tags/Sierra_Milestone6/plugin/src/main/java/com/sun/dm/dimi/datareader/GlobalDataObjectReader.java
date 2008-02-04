/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datareader;

import com.sun.mdm.index.dataobject.DataObjectReader;

/**
 *
 * @author Manish
 */
public interface GlobalDataObjectReader extends DataObjectReader {
    
    public void submitObjectForFinalization(Object genObj);
    public int getDataSourceType();

}
