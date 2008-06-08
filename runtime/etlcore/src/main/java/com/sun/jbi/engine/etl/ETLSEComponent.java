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
 * @(#)ETLSEComponent.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import javax.jbi.component.Component;
import javax.jbi.component.ComponentLifeCycle;
import javax.jbi.component.ServiceUnitManager;
import javax.jbi.servicedesc.ServiceEndpoint;
import javax.jbi.messaging.MessageExchange;

import com.sun.sql.framework.jdbc.DBConnectionFactory;

public class ETLSEComponent implements Component {
	private ComponentLifeCycle mComponentLifeCycle;

	private ServiceUnitManager mServiceUnitManager;

	public ETLSEComponent() {
		mComponentLifeCycle = new ETLSELifeCycle(this);
		mServiceUnitManager = new ETLSEServiceUnitManager(this);

		loadDBConnectionFactory();

	}

	/**
	 * this will associate jbi component classloader with DBConnectionFactory,
	 * later use the same classloader to load the etlse connection provider
	 * class
	 * 
	 */
	private void loadDBConnectionFactory() {
		DBConnectionFactory.getInstance();
	}

	/**
	 * Get the required life cycle control implementation for this component.
	 */
	public ComponentLifeCycle getLifeCycle() {
		return mComponentLifeCycle;
	}

	/**
	 * Get the Service Unit manager for this component.
	 */
	public ServiceUnitManager getServiceUnitManager() {
		return mServiceUnitManager;
	}

	/**
	 * Retrieves a DOM representation containing metadata which describes the
	 * service provided by this component, through the given endpoint. The
	 * result can use WSDL 1.1 or WSDL 2.0.
	 * 
	 * @param endpoint -
	 *            the service endpoint.
	 * @return the description for the specified service endpoint.
	 */
	public org.w3c.dom.Document getServiceDescription(ServiceEndpoint endpoint) {
		return null;
	}

	/**
	 * This method is called by JBI to check if this component, in the role of
	 * provider of the service indicated by the given exchange, can actually
	 * perform the operation desired.
	 * 
	 * @param endpoint -
	 *            the endpoint to be used by the consumer; must be non-null.
	 * @param exchange -
	 *            the proposed message exchange to be performed; must be
	 *            non-null.
	 * @return true if this provider component can perform the given exchange
	 *         with the described consumer.
	 */
	public boolean isExchangeWithConsumerOkay(ServiceEndpoint endpoint,
			MessageExchange exchange) {
		return true;
	}

	/**
	 * This method is called by JBI to check if this component, in the role of
	 * consumer of the service indicated by the given exchange, can actually
	 * interact with the provider properly. The provider is described by the
	 * given endpoint and the service description supplied by that endpoint.
	 * 
	 * @param endpoint -
	 *            the endpoint to be used by the provider; must be non-null.
	 * @param exchange -
	 *            the proposed message exchange to be performed; must be
	 *            non-null.
	 * @return true if this consumer component can interact with the described
	 *         provider to perform the given exchange.
	 */
	public boolean isExchangeWithProviderOkay(ServiceEndpoint endpoint,
			MessageExchange exchange) {
		return true;
	}

	/**
	 * Resolve the given endpoint reference. This is called by JBI when it is
	 * attempting to resolve the given EPR on behalf of a component. If this
	 * component returns a non-null result, it must conform to the following:
	 * This component implements the ServiceEndpoint returned. The result must
	 * not be registered or activated with the JBI implementation. Dynamically
	 * resolved endpoints are distinct from static ones; they must not be
	 * activated (see ComponentContext.activateEndpoint(QName, String)), nor
	 * registered (see ComponentContext) by components. They can only be used to
	 * address message exchanges; the JBI implementation must deliver such
	 * exchanges to the component that resolved the endpoint reference (see
	 * ComponentContext.resolveEndpointReference(DocumentFragment)).
	 * 
	 * @param epr -
	 *            the endpoint reference, in some XML dialect understood by the
	 *            appropriate component (usually a binding); must be non-null.
	 * @returns the service endpoint for the EPR; null if the EPR cannot be
	 *          resolved by this component.
	 */
	public ServiceEndpoint resolveEndpointReference(
			org.w3c.dom.DocumentFragment epr) {
		return null;
	}

}
