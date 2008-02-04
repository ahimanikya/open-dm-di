/*
 * TargetTableMetaDataObject.java
 * 
 * Created on Nov 13, 2007, 11:17:12 AM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.di.bulkloader.dbconnector;

/**
 *
 * @author Manish
 */
public class TableMetaDataObject {

    String columnName = null;
    String columnDataType = null;
    int columnLength = 0;
    
    public TableMetaDataObject() {
    }
    
    public String getColumnName(){
        return this.columnName;
    }
    public void setColumnName(String fieldname){
        this.columnName = fieldname;
    }
    
    public String getColumnDataType(){
        return this.columnDataType;
    }
    public void setColumnDataType(String datatype){
        this.columnDataType = datatype;
    }
    
    public int getColumnLength(){
        return this.columnLength;
    }
    
    public void setColumnLength(int fieldlen){
        this.columnLength = fieldlen;
    }    

}
