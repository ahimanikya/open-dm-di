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
 * @(#)RuntimeAttribute.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.sql.Types;

import org.w3c.dom.Element;

import com.sun.sql.framework.exception.BaseException;

/**
 * Encapsulates the value of a state variable as a name-value tuple.
 * 
 * @author Ritesh Adval
 * @version 
 */
public class RuntimeAttribute {

    public static final String TAG_ATTR = "runtimeAttr";

    /* Log4J category string */
    static final String LOG_CATEGORY = RuntimeAttribute.class.getName();

    private static final String ATTR_NAME = "name";
    private static final String ATTR_TYPE = "type";
    private static final String ATTR_VALUE = "value";

    private String attributeName;
    private Object attributeValue;
    private int jdbcType;

    /** Creates a default instance of RuntimeAttribute */
    public RuntimeAttribute() {
    }

    /**
     * Creates an instance of RuntimeAttribute with the given attribute name and value.
     * 
     * @param name name of new RuntimeAttribute
     * @param value value of new RuntimeAttribute
     * @param type JDBC type of new RuntimeAttribute
     */
    public RuntimeAttribute(String name, String value, int type) {
        if (StringUtil.isNullString(name)) {
            throw new IllegalArgumentException("Must supply non-empty String value for name.");
        }

        if (value == null) {
            throw new IllegalArgumentException("Must supply non-null Object ref for value.");
        }

        jdbcType = type;

        attributeName = name;
        attributeValue = value;
    }

    /**
     * Overrides default implementation to compute hashcode based on any associated
     * attributes as well as values of non-transient member variables.
     * 
     * @param o Object to be compared against for equality
     * @return hashcode for this instance
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o == this) {
            return true;
        }

        boolean response = false;

        if (o instanceof RuntimeAttribute) {
            RuntimeAttribute attr = (RuntimeAttribute) o;

            response = (attributeName != null) ? attributeName.equals(attr.attributeName) : (attr.attributeName == null);
            response &= (jdbcType == attr.jdbcType);
            response &= (attributeValue != null) ? attributeValue.equals(attr.attributeValue) : (attr.attributeValue != null);
        }

        return response;
    }

    /**
     * Gets attribute name.
     * 
     * @return attribute name
     */
    public String getAttributeName() {
        return this.attributeName;
    }

    /**
     * Gets attribute value as Object.
     * 
     * @return attribute value
     */
    public Object getAttributeObject() {
        return this.attributeValue;
    }

    /**
     * Gets attribute value as String.
     * 
     * @return attribute value
     */
    public String getAttributeValue() {
        return this.attributeValue.toString();
    }

    /**
     * Gets String representation of attribute type.
     * 
     * @return attribute type
     */
    public int getJdbcType() {
        return this.jdbcType;
    }

    /**
     * Overrides default implementation to compute hashcode based on values of member
     * variables.
     * 
     * @return hashcode for this instance
     */
    public int hashCode() {
        int hashCode = 0;

        hashCode += (attributeName != null) ? attributeName.hashCode() : 0;
        hashCode += jdbcType;
        hashCode += (attributeValue != null) ? attributeValue.hashCode() : 0;

        return hashCode;
    }

    /**
     * Populates this instance by parsing content nodes from given DOM element.
     * 
     * @param xmlElement Element to parse for attribute information
     */
    public void parseXMLString(Element xmlElement) throws BaseException {
        attributeName = xmlElement.getAttribute(ATTR_NAME);
        attributeValue = xmlElement.getAttribute(ATTR_VALUE);
        String typeStr = xmlElement.getAttribute(ATTR_TYPE);
        try {
            jdbcType = Integer.parseInt(typeStr);
        } catch (NumberFormatException e) {
            throw new BaseException("Invalid JDBC type in RuntimeAttribute element.");
        }
    }

    /**
     * Sets attribute name to the given String.
     * 
     * @param aName new attribute name
     */
    public void setAttributeName(String aName) {
        this.attributeName = aName;
    }

    public void setAttributeValue(Object aValue) {
        this.attributeValue = aValue;
    }

    /**
     * Sets attribute value to given Object.
     * 
     * @param aValue new value
     */
    public void setAttributeValue(String aValue) {
        this.attributeValue = aValue;
    }

    /**
     * Sets JDBC representation of attribute type.
     * 
     * @param aType new attribute type
     */
    public void setJdbcType(int aType) {
        this.jdbcType = aType;
    }

    /**
     * Overrides default implementation to provide user-readable output of attribute
     * name-value pair.
     * 
     * @return user-readable String representation of name-value pair.
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();

        buf.append("RuntimeAttribute: {").append(attributeName).append("=");
        if (attributeValue == null) {
            buf.append("<null>");
        } else {
            switch (jdbcType) {
                case Types.CHAR:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                case Types.VARCHAR:
                    buf.append("'").append(attributeValue).append("'");
                    break;

                default:
                    buf.append(attributeValue);
            }
        }
        buf.append("}");

        return buf.toString();
    }

    /**
     * Marshals contents of this RuntimeAttribute to an XML element.
     * 
     * @return XML representation of this RuntimeAttribute's contents
     */
    public String toXMLString() {
        return toXMLString("");
    }

    /**
     * Marshals contents of this RuntimeAttribute to an XML element, using the given
     * String as a prefix for each line.
     * 
     * @param prefix String to be prepended to each line of the generated XML document
     * @return XML representation of this RuntimeAttribute's contents
     */
    public String toXMLString(String prefix) {
        StringBuffer xml = new StringBuffer();

        if (prefix == null) {
            prefix = "";
        }

        if (attributeValue != null) {
            xml.append(prefix);
            xml.append("<" + TAG_ATTR + " ");
            xml.append(ATTR_NAME + "=\"" + attributeName).append("\" ");
            xml.append(ATTR_TYPE + "=\"" + jdbcType).append("\" ");
            xml.append(ATTR_VALUE + "=\"");
            xml.append((attributeValue != null) ? XmlUtil.escapeXML(attributeValue.toString()) : "");
            xml.append("\" />\n");
        }

        return xml.toString();
    }
}
