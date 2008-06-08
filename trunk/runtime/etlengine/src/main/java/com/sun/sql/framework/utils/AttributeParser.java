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
 * @(#)AttributeParser.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.utils;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author Ritesh Adval
 * @version 
 */
public class AttributeParser extends DefaultHandler {

    /* Log4J category string */
    static final String LOG_CATEGORY = AttributeParser.class.getName();

    private AttributeFactory factory;
    private InputStream inStream;

    public AttributeParser(InputStream stream, AttributeFactory fac) {
        this.factory = fac;
        this.inStream = stream;
    }

    public AttributeParser(String fileName, AttributeFactory fac) {
        this.factory = fac;
        InputStream stream = AttributeParser.class.getClassLoader().getResourceAsStream(fileName);
        this.inStream = stream;
    }

    public void characters(char buf[], int offset, int len) throws SAXException {
    }

    public void endDocument() throws SAXException {
    }

    // SAX 2.0
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (factory != null) {
            factory.endElement(uri, localName, qName);
        }
    }

    public void parse() {
        // Use the default (non-validating) parser
        SAXParserFactory saxpFactory = SAXParserFactory.newInstance();
        try {
            // Parse the input
            SAXParser saxParser = saxpFactory.newSAXParser();
            saxParser.parse(this.inStream, this);
        } catch (Exception ex) {
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Error occured while parsing.", ex);
        }
    }

    public void startDocument() throws SAXException {
    }

    // SAX 2.0
    public void startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException {
        if (factory != null) {
            factory.startElement(uri, localName, qName, attrs);
        }
    }

}
