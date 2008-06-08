/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)ETLSERuntimeConfigurationMBean.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */
/**
 * 
 */
package com.sun.jbi.engine.etl.mbean;

import java.util.Map;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;

/**
 * @author Sujit Biswas
 *
 */
public interface ETLSERuntimeConfigurationMBean {

    /**
     * Save the configuration to persistent storage.
     *
     * @param propFile DOCUMENT ME!
     *
     * @return 0 if successful.
     */
    boolean save(String propFile);

    /**
     * Restore the configuration from persistent storage.
     *
     * @param propFile DOCUMENT ME!
     *
     * @return 0 if successful.
     */
    boolean restore(String propFile);

    /**
     * MBean Setter/Getter for web page configuration
     *
     * @param count is Maximum Thread Count
     *
     * @throws InvalidAttributeValueException
     * @throws MBeanException
     */
    public void setMaxThreadCount(String count) throws InvalidAttributeValueException, MBeanException;

    /**
     * MBean Setter/Getter for web page configuration
     *
     * @return Maximum Thread Count
     */
    public String getMaxThreadCount() throws InvalidAttributeValueException, MBeanException;

    /**
     * @return the axiondbInstanceName
     */
    public String getAxiondbInstanceName() throws InvalidAttributeValueException, MBeanException;

    /**
     * @param axiondbInstanceName
     *            the axiondbInstanceName to set
     */
    public void setAxiondbInstanceName(String axiondbInstanceName) throws InvalidAttributeValueException, MBeanException;

    /**
     * @return the axiondbWorkingDir
     */
    public String getAxiondbWorkingDir() throws InvalidAttributeValueException, MBeanException;

    /**
     * @param axiondbWorkingDir
     *            the axiondbWorkingDir to set
     */
    public void setAxiondbWorkingDir(String axiondbWorkingDir) throws InvalidAttributeValueException, MBeanException;

    //Added For SystemicQualities 
    //TODO -  Remove the Unwanted methods
    public String getProxyUserID();

    public void setProxyUserID(String val) throws InvalidAttributeValueException, MBeanException;

    public String getProxyPassword() throws Exception;

    public void setProxyPassword(String val) throws InvalidAttributeValueException, MBeanException;
    
    
    public String getPersistenceDBDriverClass();
    
    public void setPersistenceDBDriverClass(String val) throws InvalidAttributeValueException, MBeanException;
    
    
    public String getPersistenceDBUrl();
    
    public void setPersistenceDBUrl(String val) throws InvalidAttributeValueException, MBeanException;
    
     public String getPersistenceDBUserId();

    public void setPersistenceDBUserId(String val) throws InvalidAttributeValueException, MBeanException;

    public String getPersistenceDBPassword() throws Exception;

    public void setPersitenceDBPassword(String val) throws InvalidAttributeValueException, MBeanException;
    
    public int getRetryMaxCount();
    
    public void setRetryMaxCount(int val) throws InvalidAttributeValueException, MBeanException;
    
    public long getRetryMaxInterval();
    
    public void setRetryMaxInterval(long val) throws InvalidAttributeValueException, MBeanException;
    
    

    public Map retrieveEnvVariablesMap();

    public void updateEnvVariablesMap(Map val) throws MBeanException;

    /* The retrieveConfigurationDisplaySchema operation returns the schema (defined in XSD) of the attributes 
     * that the Component Config MBean exposes. The schema can define restrictions and thus allows the developer to 
     * specify Enumerated Strings (which may be displayed as DropDowns in the UI) or restrict the integer fields to 
     * positive integers, specify totalDigits, and minInclusive or maxExclusive ? in effect any definition that can 
     * be expressed in XSD can be placed on these attributes.
     */
    public String retrieveConfigurationDisplaySchema();

    /*The retrieveConfigurationDisplayData operation returns the XML data corresponding to the schema (defined in 
     * XSD) of the attributes that the Component Config MBean exposes. It is here that the component developer can specify 
     * descriptive names for the attributes that can appear in label fields of the UI, descriptions of the fields that can appear in 
     *ToolTips, or whether the field is a secret field (like a password field) or not so the UI can hide the actual data the user be
     *allowed to see from the UI. 
     */
    public String retrieveConfigurationDisplayData();
    
        /**
     * This operation adds a new application variable. If a variable with the same name 
     * already exists, the operation fails.
     * 
     * @param name - name of the application variable
     * @param appVar - this is the application variable compoiste
     * @throws MBeanException if an error occurs in adding the application variable to the 
     *         component. 
     */
     public void addApplicationVariable(String name, CompositeData appVar) throws InvalidAttributeValueException, MBeanException;
     
    /**
     * This operation sets an application variable. If a variable does not exist with 
     * the same name, its an error.
     * 
     * @param name - name of the application variable
     * @param appVar - this is the application variable compoiste to be updated.
     * @throws MBeanException if one or more application variables cannot be deleted
     */
    public void setApplicationVariable(String name, CompositeData appVar) throws InvalidAttributeValueException, MBeanException; 
     
    /**
     * This operation deletes an application variable, if a variable with the specified name does
     * not exist, it's an error.
     *
     * @param name - name of the application variable
     * @throws MBeanException on errors.
     */
     public void deleteApplicationVariable(String name) throws MBeanException;
     
     /**
      * Get the Application Variable set for a component.
      *
      * @return  a TabularData which has all the applicationvariables set on the component. 
      */
     public TabularData getApplicationVariables();

    
    /**
      * Get the CompositeType definition for the components application configuration 
      *
      * @return the CompositeType for the components application configuration.
      */
     public CompositeType queryApplicationConfigurationType();
     
     /**
      * Add an application configuration. The configuration name is a part of the CompositeData.
      * The itemName for the configuration name is "configurationName" and the type is SimpleType.STRING
      *
      * @param name - configuration name, must match the value of the field "name" in the namedConfig
      * @param appConfig - application configuration composite 
      * @throws MBeanException if the application configuration cannot be added.
      */
    public void addApplicationConfiguration(String name, CompositeData appConfig) throws InvalidAttributeValueException, MBeanException;
    
     /**
      * Delete an application configuration. 
      *
      * @param name - identification of the application configuration to be deleted
      * @throws MBeanException if the configuration cannot be deleted.
      */
    public void deleteApplicationConfiguration(String name) throws MBeanException;
     
     /**
      * Update a application configuration. The configuration name is a part of the CompositeData.
      * The itemName for the configuration name is "configurationName" and the type is SimpleType.STRING
      *
      * @param name - configuration name, must match the value of the field "configurationName" in the appConfig
      * @param appConfig - application configuration composite
      * @throws MBeanException if there are errors encountered when updating the configuration.
      */
    public void setApplicationConfiguration(String name, CompositeData appConfig) throws InvalidAttributeValueException, MBeanException;
    
    /**
     * Get a Map of all application configurations for the component.
     *
     * @return a TabularData of all the application configurations for a 
     *         component keyed by the configuration name. 
     */
    public TabularData getApplicationConfigurations(); 
    
    /** Retrieves the application variables map. The map key is the application 
      * variable name, the value is a String[] containing detailed information
      * about the application variable. This method is used to communicate
      * application variable data with in the component and is not intended 
      * for MBean clients
      *
      * @return a Map containing application variable information
      */
    public Map retrieveApplicationVariablesMap();
    
    /** Updates the application variable map.
      * This method is used to communicate application configuration data within the component, and 
      * not intended for MBean clients
      *
      * @param a Map containing application variable information
      */
    public void updateApplicationVariablesMap(Map val) throws MBeanException;
    
    
    /** Retrieves the application configuration map. The map key is the  
      * configuration name, the value is a String representing a HTTP URL.
      * This method is used to communicate application configuration data
      * within the component and is not intended for MBean clients.
      *
      * @return a Map containing application configuration information
      */
    public Map retrieveApplicationConfigurationsMap();
    
    /** Updates the application configuration map.
      * This method is used to communicate application configuration data within the 
      * component, and not intended for MBean clients
      *
      * @param a Map containing application variable information
      */
    public void updateApplicationConfigurationsMap(Map val) throws MBeanException;
}
