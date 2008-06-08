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
 * @(#)Visitor.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package org.axiondb.service.client;

/**
 * @author Ritesh Adval
 */

public interface Visitor {
    /**
     * visit a row
     * 
     * @param attr
     */
    public void visit(Attr attr);

    /**
     * visit a data column
     * 
     * @param dColumn
     */
    public void visit(DataColumn dColumn);

    /**
     * visit a meta column
     * 
     * @param mColumn
     */
    public void visit(MetaColumn mColumn);

    /**
     * visit a row
     * 
     * @param row
     */
    public void visit(Row row);

    /**
     * visit select data.
     * 
     * @param sData
     */
    public void visit(SelectData sData);

    /**
     * visit select meta data
     * 
     * @param sMetaData
     */
    public void visit(SelectMetaData sMetaData);

    /**
     * visit select result.
     * 
     * @param sResult
     */
    public void visit(SelectResult sResult);
}
