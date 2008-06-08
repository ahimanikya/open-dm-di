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
 * @(#)ETLSEListener.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.util.logging.Level;
import java.util.logging.Logger;
import com.sun.jbi.engine.etl.Localizer;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLEngineListener;
import com.sun.etl.engine.ETLEngineLogEvent;

/**
 * @author Sujit Biswas
 *
 */
public class ETLSEListener implements ETLEngineListener {
	
	private static transient final Logger mLogger = Logger.getLogger(ETLSEListener.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
	
	ETLEngine engine;
	
	

	public ETLSEListener(ETLEngine engine) {
		this.engine = engine;
	}

	/* (non-Javadoc)
	 * @see com.sun.etl.engine.ETLEngineListener#executionPerformed(com.sun.etl.engine.ETLEngineExecEvent)
	 */
	public void executionPerformed(ETLEngineExecEvent event) {
		if ((event.getStatus() == ETLEngine.STATUS_COLLAB_COMPLETED)
				|| (event.getStatus() == ETLEngine.STATUS_COLLAB_EXCEPTION)) {

			try {
				String msg = (event.getStatus() == ETLEngine.STATUS_COLLAB_COMPLETED) ? "MSG_executed_success"
						: "MSG_executed_errors"; // No I18N
				mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",(event.getStatus() == ETLEngine.STATUS_COLLAB_COMPLETED) ? "MSG_executed_success"
						: "MSG_executed_errors"));
			} catch (Exception ex) {
				;
			} finally {
				synchronized (this) {
					engine.stopETLEngine();
					notifyAll();
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.sun.etl.engine.ETLEngineListener#updateOutputMessage(com.sun.etl.engine.ETLEngineLogEvent)
	 */
	public void updateOutputMessage(ETLEngineLogEvent evt) {
		// TODO Auto-generated method stub

	}

}
