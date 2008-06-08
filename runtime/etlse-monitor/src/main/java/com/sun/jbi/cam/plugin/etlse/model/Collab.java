/*
 * Collab.java
 *
 * Created on November 10, 2006, 4:20 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

/**
 * This represents an etl collaboration
 * 
 * @author Sujit
 */
public class Collab {
    
    
    private String name;
    private String id;
    /**
     * Creates a new instance of Collab
     */
    public Collab(String name, String id) {
        this.setName(name);
        this.setId(id);
    }
    
    public Collab(){
        
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

	@Override
	public String toString() {
		return "collab="+ name + ",id="+ id;
	}
    
    
    
}
