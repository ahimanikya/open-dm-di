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
 * @(#)ValidatingTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.sun.sql.framework.utils.RuntimeAttribute;

/**
 * @author Girish Patil
 */
public class ValidatingTask extends PipelineTask {

    protected String getMessageFinished() {
        return PipelineTask.MSG_MGR.getString("MSG_validating_finished");
    }

    protected String getMessageStarted() {
        return MSG_MGR.getString("MSG_validating_started");
    }

    protected String getTaskName() {
        return "Validating";
    }

    protected void populateExecutionId(Map attribMap, List paramList, int value) {
        //	FIXME: This kind of a hack
        RuntimeAttribute execId = new RuntimeAttribute("SBYN_executionId", ("" + value), Types.INTEGER);
        attribMap.put(execId.getAttributeName(), execId);
        paramList.add(execId.getAttributeName());
    }
}
