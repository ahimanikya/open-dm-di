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
 * @(#)SummaryTotalData.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.monitor.data;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Container to hold overall execution statistics for a particular deployed eTL collaboration.
 *
 * @author vvenkataraman
 */
public class SummaryTotalData {

    private int totalExtracted = 0;
    private int totalLoaded = 0;
    private int totalRejected = 0;
    private int averageExtracted = 0;
    private int averageLoaded = 0;
    private int averageRejected = 0;


    public SummaryTotalData() {
        super();
    }

    public int getTotalExtracted() {
        return this.totalExtracted;
    }

    public void setTotalExtracted(int value) {
        this.totalExtracted = value;
    }

    public int getTotalLoaded() {
        return this.totalLoaded;
    }

    public void setTotalLoaded(int value) {
        this.totalLoaded = value;
    }

    public int getTotalRejected() {
        return this.totalRejected;
    }

    public void setTotalRejected(int value) {
        this.totalRejected = value;
    }

    public int getAverageExtracted() {
        return this.averageExtracted;
    }

    public void setAverageExtracted(int value) {
        this.averageExtracted = value;
    }

    public int getAverageLoaded() {
        return this.averageLoaded;
    }

    public void setAverageLoaded(int value) {
        this.averageLoaded = value;
    }

    public int getAverageRejected() {
        return this.averageRejected;
    }

    public void setAverageRejected(int value) {
        this.averageRejected = value;
    }

    private void init() {
        this.totalExtracted = 0;
        this.totalLoaded = 0;
        this.totalRejected = 0;
        this.averageExtracted = 0;
        this.averageLoaded = 0;
        this.averageRejected = 0;
    }

    private int parseIntFromStringList(int index, List strList) {
        int ret = 0;

        if ((index >= 0) && (strList.size() > index)) {
            try {
                ret = Integer.parseInt((String) strList.get(index)) ;
            }catch (Exception ex) {
                // Log the exception
            }
        }

        return ret;
    }

    public void unmarshal(String data) {
        char delimiter = ',';
        this.init();
        if (data != null && data.trim().length() > 0) {

            ArrayList strings = new ArrayList();
            StringTokenizer tok = new StringTokenizer(data, String.valueOf(delimiter));
            if (tok.hasMoreTokens()) {
                do {
                    strings.add(tok.nextToken().trim());
                } while (tok.hasMoreTokens());
            }
            this.totalExtracted = parseIntFromStringList(0, strings);
            this.totalLoaded = parseIntFromStringList(1, strings);
            this.totalRejected = parseIntFromStringList(2, strings);
            this.averageExtracted = parseIntFromStringList(3, strings);
            this.averageLoaded = parseIntFromStringList(4, strings);
            this.averageRejected = parseIntFromStringList(5, strings);
        }
    }

    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append(this.getTotalExtracted());
        buff.append(",");
        buff.append(this.getTotalLoaded());
        buff.append(",");
        buff.append(this.getTotalRejected());
        buff.append(",");
        buff.append(this.getAverageExtracted());
        buff.append(",");
        buff.append(this.getAverageLoaded());
        buff.append(",");
        buff.append(this.getAverageRejected());

        return buff.toString();
    }

}
