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
 * @(#)XMLWriterVisitor.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package org.axiondb.service.client;

import java.util.Iterator;
import java.util.Stack;

/**
 * @author Ritesh Adval
 */
public class XMLWriterVisitor implements Visitor {

    private final String DATA_COLUMN = "data-column";
    private final String END_BRACKET = ">";

    private final String INDENT = "\t";
    private final String META_COLUMN = "meta-column";
    private final String ROW = "row";
    private final String SELECT_DATA = "select-data";
    private final String SELECT_META_DATA = "select-meta-data";
    private final String SELECT_RESULT = "select-result";

    private final String START_BRACKET = "<";

    public String getXml() {
        return _xmlBuf.toString();
    }

    public void visit(Attr attr) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("attr ");
        _xmlBuf.append("name = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(attr.getName());
        _xmlBuf.append("\"");

        _xmlBuf.append(" ");
        _xmlBuf.append("value = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(attr.getValue());
        _xmlBuf.append("\"");
        _xmlBuf.append(" /");
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(DataColumn dColumn) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(DATA_COLUMN);

        _xmlBuf.append(" position = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(dColumn.getPosition());
        _xmlBuf.append("\"");
        _xmlBuf.append(" ");

        _xmlBuf.append(" value = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(toAttributeValue(dColumn.getValue()));
        _xmlBuf.append("\"");
        _xmlBuf.append(" ");

        _xmlBuf.append(" /");
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(MetaColumn mColumn) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(META_COLUMN);

        _xmlBuf.append(" position = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(mColumn.getPosition());
        _xmlBuf.append("\"");
        _xmlBuf.append(" ");
        _xmlBuf.append(END_BRACKET);

        Iterator it = mColumn.getAttributes().values().iterator();
        while (it.hasNext()) {
            Attr attr = (Attr) it.next();
            visit(attr);
        }

        applyIndent();
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("/");
        _xmlBuf.append(META_COLUMN);
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(Row row) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(ROW);

        _xmlBuf.append(" position = ");
        _xmlBuf.append("\"");
        _xmlBuf.append(row.getPosition());
        _xmlBuf.append("\"");
        _xmlBuf.append(" ");
        _xmlBuf.append(END_BRACKET);

        Iterator it = row.getColumns().iterator();
        while (it.hasNext()) {
            DataColumn dColumn = (DataColumn) it.next();
            visit(dColumn);
        }

        applyIndent();
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("/");
        _xmlBuf.append(ROW);
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(SelectData sData) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(SELECT_DATA);
        _xmlBuf.append(END_BRACKET);

        Iterator it = sData.getRows().iterator();
        while (it.hasNext()) {
            Row row = (Row) it.next();
            visit(row);
        }

        applyIndent();
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("/");
        _xmlBuf.append(SELECT_DATA);
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(SelectMetaData sMetaData) {
        applyIndent();
        _indentStack.push(INDENT);
        _xmlBuf.append(INDENT);

        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(SELECT_META_DATA);
        _xmlBuf.append(END_BRACKET);

        Iterator it = sMetaData.getMetaColumns().iterator();
        while (it.hasNext()) {
            MetaColumn metaColumn = (MetaColumn) it.next();
            visit(metaColumn);
        }

        applyIndent();
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("/");
        _xmlBuf.append(SELECT_META_DATA);
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    public void visit(SelectResult sResult) {
        _indentStack.push(INDENT);
        _xmlBuf.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        _xmlBuf.append("\n");
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append(SELECT_RESULT);
        _xmlBuf.append(END_BRACKET);

        visit(sResult.getSelectMetaData());
        visit(sResult.getSelectData());

        applyIndent();
        _xmlBuf.append(START_BRACKET);
        _xmlBuf.append("/");
        _xmlBuf.append(SELECT_RESULT);
        _xmlBuf.append(END_BRACKET);

        _indentStack.pop();
    }

    private void applyIndent() {
        _xmlBuf.append("\n");
        Iterator it = _indentStack.iterator();
        while (it.hasNext()) {
            String indent = (String) it.next();
            _xmlBuf.append(indent);
        }
    }

    public static String toAttributeValue(String val) {
        try {
            if ((val == null) || (checkAttributeCharacters(val)))
                return val;
        }catch (Exception ex) {
            // For now, consume exception due to invalid character.
            // If XSLT does not handle satisfactorily then propagate exception.
            return val;
        }

        StringBuffer buf = new StringBuffer();

        for (int i = 0; i < val.length(); i++) {
            char ch = val.charAt(i);
            if ('<' == ch)
                buf.append("&lt;");
            else if ('&' == ch)
                buf.append("&amp;");
            else if ('\'' == ch)
                buf.append("&apos;");
            else if ('"' == ch)
                buf.append("&quot;");
            else
                buf.append(ch);
        }
        return buf.toString();
    }

    private static boolean checkAttributeCharacters(String chars) throws Exception {
        boolean escape = false;
        for (int i = 0; i < chars.length(); i++) {
            char ch = chars.charAt(i);
            if (ch <= ']')
                switch (ch) {
                    case 9:
                    case 10:
                    case 13:
                        break;
                    case 34:
                    case 38:
                    case 39:
                    case 60:
                        escape = true;
                        break;
                    default:
                        if (ch < ' ')
                            throw new Exception("Invalid XML character &#" + (int) ch + ";.");
                        break;
                }
        }
        return !escape;
    }
    private Stack _indentStack = new Stack();
    private StringBuffer _xmlBuf = new StringBuffer(100);

}
