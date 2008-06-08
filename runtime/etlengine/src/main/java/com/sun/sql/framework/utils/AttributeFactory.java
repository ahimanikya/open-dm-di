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
 * @(#)AttributeFactory.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * @author Ritesh Adval
 * @version 
 */
public class AttributeFactory {

    /* Log4J category string */
    static final String LOG_CATEGORY = AttributeFactory.class.getName();

    private static HashMap tagToClassMap = new HashMap();

    static {
        tagToClassMap.put("attr", "com.sun.sql.framework.utils.Attribute");
    }

    public static Object invokeGetter(Object bean, String propertyName) throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IntrospectionException {
        return invokeGetter(bean, propertyName, null, null);
    }

    public static Object invokeGetter(Object obj, String propertyName, Class[] parameterTypes, Object[] params) throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, IntrospectionException {

        String base = capitalize(propertyName);
        Method readMethod;

        // Since there can be multiple setter methods but only one getter
        // method, find the getter method first so that you know what the
        // property type is. For booleans, there can be "is" and "get"
        // methods. If an "is" method exists, this is the official
        // reader method so look for this one first.
        try {
            readMethod = obj.getClass().getMethod("is" + base, parameterTypes);
        } catch (Exception getterExc) {
            // no "is" method, so look for a "get" method.
            readMethod = obj.getClass().getMethod("get" + base, parameterTypes);
        }

        return readMethod.invoke(obj, params);

    }

    public static Object invokeSetter(Object bean, String propertyName, Object val) throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IntrospectionException {

        PropertyDescriptor pd = new PropertyDescriptor(propertyName, bean.getClass());
        Method method = pd.getWriteMethod();
        return method.invoke(bean, new Object[] { val});
    }

    public static void invokeSetters(Object obj, Attributes attrs) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            IntrospectionException {

        for (int i = 0; i < attrs.getLength(); i++) {
            if (attrs.getQName(i).equals("class")) {
                continue;
            }
            Class[] cls = new Class[] { String.class};
            String base = capitalize(attrs.getQName(i));

            Method method = obj.getClass().getMethod("set" + base, cls);
            method.invoke(obj, new Object[] { attrs.getValue(attrs.getQName(i))});
        }

    }

    static String capitalize(String s) {
        if (s.length() == 0) {
            return s;
        }
        char chars[] = s.toCharArray();
        chars[0] = Character.toUpperCase(chars[0]);
        return new String(chars);
    }

    private Map attributeMap = new HashMap();

    /** Creates a new instance of PropertyFactory */
    public AttributeFactory() {
    }

    public void endElement(String uri, String localName, String qName) {

    }

    public Map getAttributeMap() {
        return this.attributeMap;
    }

    public void startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException {
        try {
            if (qName.equals("attr")) {
                Attribute attr = (Attribute) createObject(uri, localName, qName, attrs);
                attributeMap.put(attr.getAttributeName(), attr);
            }

        } catch (Exception ex) {
            String msg = "Error occured while parsing following :" + "\n uri = " + uri + "\n localName = " + localName + "\n qName = " + qName;
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, msg, ex);
            throw new SAXException(msg, ex);
        }
    }

    private Object createObject(String uri, String localName, String qName, Attributes attrs) throws NoSuchMethodException,
            InvocationTargetException, ClassNotFoundException, InstantiationException, IllegalAccessException, IntrospectionException {

        String className = attrs.getValue("class");
        if (className == null) {
            className = (String) tagToClassMap.get(qName);
            if (className == null) {
                return null;
            }
        }

        Class cl = Class.forName(className, true, getClass().getClassLoader());
        Object obj = cl.newInstance();
        invokeSetters(obj, attrs);
        return obj;
    }
}
