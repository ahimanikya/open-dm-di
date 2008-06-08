/**
 * 
 */
package com.sun.jbi.cam.plugin.etlse.model;

import java.io.IOException;
import java.util.logging.Logger;

import javax.faces.context.FacesContext;
import javax.faces.event.PhaseEvent;
import javax.faces.event.PhaseId;
import javax.faces.event.PhaseListener;
import javax.servlet.http.HttpSession;

/**
 * @author Sujit Biswas
 * 
 */
public class MonitorPhaseListener implements PhaseListener {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(MonitorPhaseListener.class.getName());

	
	public void afterPhase(PhaseEvent event) {
	
		logger.fine("after :" + event.getPhaseId().toString());

	}

	private boolean isValidSession(FacesContext ctx) {
		HttpSession s = (HttpSession) ctx.getExternalContext().getSession(false);
		if (s == null)
			return false;
		else
			return true;
	}

	public void beforePhase(PhaseEvent event) {
		FacesContext ctx = event.getFacesContext();
	
		logger.fine("before :" + event.getPhaseId().toString());

		if (!isValidSession(ctx)) {

			logger.fine("invalid session forwarding request to index page: "
					+ event.getPhaseId().toString());
	
			try {
				String ctxPath=ctx.getExternalContext().getRequestContextPath();
				logger.fine("request context path: " + ctxPath);
				ctx.getExternalContext().redirect(ctxPath + "/index.jsp");
			} catch (IOException e) {
				logger.info(e.getMessage());

			}
			ctx.responseComplete();	
			
		}

	}

	public PhaseId getPhaseId() {
		return PhaseId.RESTORE_VIEW;
	}

}
