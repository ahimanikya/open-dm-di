/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sun.jbi.engine.etl;

import javax.jbi.messaging.InOut;
import javax.xml.namespace.QName;
import org.w3c.dom.Document;

/**
 *
 * @author Manish
 */
public class ETLInOutQueueElement {
    
    // List the parameters that you want to scramble into this object
    private String normalizedMessage =  null;
    private String serviceName = null;
    private String operationName = null;
    private String msgExchId = null;
    
    protected ETLInOutQueueElement(){
        
    }
    
    protected ETLInOutQueueElement(String msgId, String normalizedMessage, String serviceName, String operationName) {
        this.msgExchId = msgId;
        this.normalizedMessage = normalizedMessage;
        this.serviceName = serviceName;
        this.operationName = operationName;
    }
    
    public String getNormalizedMsg(){
        return this.normalizedMessage;
    }
    
    public String getServiceName() {
        return this.serviceName;
    }
    
    public String getOperationName() {
        return this.operationName;
    }
    
    public String getMsgExchId() {
        return this.msgExchId;
    }

}
