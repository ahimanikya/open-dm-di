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
 * @(#)ETLTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import com.sun.etl.engine.utils.ETLException;

/**
 * ETLTask should be implemented by all tasks, so that ETLEngine can call them
 * dynamically.
 * 
 * @author Ahimanikya Satapathy
 * @version 
 */
public interface ETLTask {

    /** Indicates exception thrown during task. */
    public static final String EXCEPTION = "Exception";

    /** Indicates task success. */
    public static final String SUCCESS = "Success";

    /**
     * Cleans up resources.
     */
    public void cleanUp();

    /**
     * Handles ETLException
     * 
     * @param ex BaseException that needs to be handled
     */
    public void handleException(ETLException ex);

    /**
     * Process the given TaskNode
     * 
     * @param curETLTaskNode Current TaskNode
     * @throws ETLException indicating processing problem.
     * @return Sucess or failure of execution of tasknode
     */
    public String process(ETLTaskNode curETLTaskNode) throws ETLException;

}
