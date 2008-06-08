/*
 * SystemInfo.java
 *
 * Created on December 5, 2006, 12:12 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.jbi.cam.plugin.etlse.model;

import javax.faces.context.FacesContext;
import javax.servlet.http.HttpServletRequest;

/**
 *
 * @author Sujit Biswas
 */
public class SystemInfo {
    
    
    private String user="anonymous";
    private String server;
    
    /** Creates a new instance of SystemInfo */
    public SystemInfo() {
        FacesContext ctx = FacesContext.getCurrentInstance();
        String s = ctx.getExternalContext().getRemoteUser();
        if(s!=null){
            user = s;
        }
        HttpServletRequest req = (HttpServletRequest) ctx.getExternalContext().getRequest();
        server = req.getServerName() + ":" + req.getServerPort();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }
    
}
