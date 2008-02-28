/*
 * DOBaseWriter.java
 * 
 * Created on Oct 16, 2007, 5:29:24 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.dimi.datawriter;

/**
 *
 * @author Manish
 */
public abstract class DOBaseWriter {
    
    private boolean specialMode = false;

    public DOBaseWriter() {
    }
    
    public DOBaseWriter(boolean specialmode){
        this.specialMode = specialmode;
    }

}
