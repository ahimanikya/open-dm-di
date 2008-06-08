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
 * @(#)Attribute.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.w3c.dom.Element;

import com.sun.sql.framework.exception.BaseException;

/**
 * Encapsulates the value of a state variable as a name-value tuple.
 * 
 * @author Ritesh Adval
 * @version 
 */
public class Attribute implements Cloneable {

    public static final String TAG_ATTR = "attr";

    /* Log4J category string */
    static final String LOG_CATEGORY = Attribute.class.getName();

    private static final String ATTR_NAME = "name";
    private static final String ATTR_TYPE = "type";
    private static final String ATTR_VALUE = "value";

    private static final List LEGAL_TYPES = Arrays.asList(new String[] { String.class.getName(), Integer.class.getName(), Boolean.class.getName(),
            List.class.getName(), ArrayList.class.getName()});

    /**
     * Indicates whether given type String is valid for storage as an Attribute.
     * 
     * @param typeName type name to test
     * @return boolean true if typeName represents a valid Attribute type; false otherwise
     */
    public static boolean isValidType(String typeName) {
        return LEGAL_TYPES.contains(typeName);
    }

    private String attributeName;
    private String attributeType;
    private Object attributeValue;

    /** Creates a default instance of Attribute */
    public Attribute() {
    }

    public Attribute(Attribute src) throws BaseException {
        if (src == null) {
            throw new IllegalArgumentException("can not create new instance of Attribute using copy constructor for " + src);
        }

        copyFrom(src);
    }

    /**
     * Creates an instance of Attribute with the given attribute name and value.
     * 
     * @param name name of new Attribute
     * @param value value of new Attribute
     */
    public Attribute(String name, Object value) {
        if (StringUtil.isNullString(name)) {
            throw new IllegalArgumentException("Must supply non-empty String value for name.");
        }

        if (value == null) {
            throw new IllegalArgumentException("Must supply non-null Object ref for value.");
        }

        attributeType = value.getClass().getName();
        if (!Attribute.isValidType(attributeType)) {
            throw new IllegalArgumentException("Invalid type - Attribute cannot accept a value of type " + attributeType);
        }

        attributeName = name;
        attributeValue = value;
    }

    public Object clone() throws CloneNotSupportedException {
        Attribute attr;
        try {
            attr = new Attribute(this);
        } catch (BaseException ex) {
            throw new CloneNotSupportedException("can not create clone of " + this.toString());
        }
        return attr;
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

        if (o instanceof Attribute) {
            Attribute attr = (Attribute) o;

            response = (attributeName != null) ? attributeName.equals(attr.attributeName) : (attr.attributeName == null);
            response &= (attributeType != null) ? attributeType.equals(attr.attributeType) : (attr.attributeType == null);
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
     * Gets String representation of attribute type.
     * 
     * @return attribute type
     */
    public String getAttributeType() {
        return this.attributeType;
    }

    /**
     * Gets attribute value.
     * 
     * @return attribute value
     */
    public Object getAttributeValue() {
        return this.attributeValue;
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
        hashCode += (attributeType != null) ? attributeType.hashCode() : 0;
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
        String value = xmlElement.getAttribute(ATTR_VALUE);
        if (value != null) {
            createValueFor(value, xmlElement.getAttribute(ATTR_TYPE));
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

    /**
     * Sets String representation of attribute type.
     * 
     * @param aType new attribute type
     */
    public void setAttributeType(String aType) {
        this.attributeType = aType;
    }

    /**
     * Sets attribute value to given Object.
     * 
     * @param aValue new value
     */
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

    public String toString() {
        StringBuffer buf = new StringBuffer(50);
        buf.append("name = " + this.getAttributeName() + "\n");
        buf.append("type = " + this.getAttributeType() + "\n");
        buf.append("value = " + this.getAttributeValue());
        return buf.toString();
    }

    /**
     * Marshals contents of this Attribute to an XML element.
     * 
     * @return XML representation of this Attribute's contents
     */
    public String toXMLString() {
        return toXMLString("");
    }

    /**
     * Marshals contents of this Attribute to an XML element, using the given String as a
     * prefix for each line.
     * 
     * @param prefix String to be prepended to each line of the generated XML document
     * @return XML representation of this Attribute's contents
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
            xml.append(ATTR_TYPE + "=\"" + attributeType).append("\" ");
            xml.append(ATTR_VALUE + "=\"");
            if (attributeType.equals(List.class.getName()) || attributeType.equals(ArrayList.class.getName())) {
                xml.append((StringUtil.createDelimitedStringFrom((List) attributeValue)));
            } else {
                xml.append((attributeValue != null) ? XmlUtil.escapeXML(attributeValue.toString()) : "null");
            }
            xml.append("\" />\n");
        }

        return xml.toString();
    }

    private void copyFrom(Attribute src) throws BaseException {
        this.setAttributeName(src.getAttributeName());
        this.setAttributeType(src.getAttributeType());
        if (src.getAttributeValue() != null) {
            createValueFor(src.getAttributeValue().toString(), src.getAttributeType());
        }
    }

    private void createValueFor(String value, String typeName) throws BaseException {
        if (StringUtil.isNullString(typeName)) {
            throw new IllegalArgumentException("Must supply non-empty String value for typeName.");
        }

        if (!isValidType(typeName)) {
            throw new IllegalArgumentException("Invalid type - Attribute cannot accept a value of type " + typeName);
        }

        if (value == null) {
            throw new IllegalArgumentException("Must supply non-null Object ref for value.");
        }

        attributeType = typeName;
        if (typeName.equals(List.class.getName()) || typeName.equals(ArrayList.class.getName())) {
            attributeValue = StringUtil.createStringListFrom(value);
        } else if (typeName.equals("java.lang.String")) {
            attributeValue = value;
        } else {
            try {
                Class objClass = Class.forName(typeName, true, getClass().getClassLoader());
                Constructor constructor = objClass.getConstructor(new Class[] { String.class});
                attributeValue = constructor.newInstance(new Object[] { value});
            } catch (NoSuchMethodException e) {
                throw new BaseException("Type does not have a constructor with a single String parameter: " + typeName, e);
            } catch (Exception e) {
                throw new BaseException("Could not load class for type '" + attributeType + "'.", e);
            }
        }
    }
}
