/*
 * CollabSummaryDetails.java
 *
 * Created on December 5, 2006, 1:10 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import javax.faces.context.FacesContext;

/**
 *
 * @author Sujit
 */
public class CollabSummaryDetails {
    
    /** Creates a new instance of CollabSummaryDetails */
    public CollabSummaryDetails() {
    }
    
    public void details(){
        FacesContext ctx = FacesContext.getCurrentInstance();
        ctx.getExternalContext().getRequestParameterMap();
    }
    
}
