<jsp:root version="1.2" xmlns:f="http://java.sun.com/jsf/core"
	xmlns:h="http://java.sun.com/jsf/html"
	xmlns:jsp="http://java.sun.com/JSP/Page"
	xmlns:webuijsf="http://www.sun.com/webui/webuijsf">
	<jsp:directive.page contentType="text/html" />
	<f:loadBundle basename="com.sun.jbi.cam.plugin.etlse.model.resource"
		var="msg"></f:loadBundle>
	<f:view>
		<webuijsf:page>
			<webuijsf:html id="html">
			<webuijsf:head id="head" title="DataIntegrator Monitor">
				<webuijsf:link rel="shortcut icon" url="../config/favicon.ico"
					type="image/x-icon" />
			</webuijsf:head>
			<webuijsf:body id="body">
				<webuijsf:form id="form">

					<webuijsf:masthead id="masthead"
					productImageURL="../config/ETL.ETLDefinition.png"
					productImageHeight="30" productImageWidth="40"
					userInfo="#{SystemInfo.user}" serverInfo="#{SystemInfo.server}"
					productImageDescription="DataIntegrator Collaboration Monitor" />
			

					<!-- Content Page Title -->
					<webuijsf:contentPageTitle id="contentPageTitle"
						title="DataIntegrator Monitor Collabs" helpText="" />


					<div class="ConMgn"><!-- Messages --> <h:messages
						showSummary="true" showDetail="true" /> <br />

					<!-- Embedded Actions --> <webuijsf:table id="table1"
						paginateButton="true" paginationControls="true"
						title="DataIntegrator Collaboration List">
						<webuijsf:tableRowGroup id="rowGroup1"
							sourceData="#{mbeanlist.collabNames}" sourceVar="mbean" rows="10">


							<webuijsf:tableColumn id="col2" alignKey="first"
								headerText="Collaboration Name">
								<webuijsf:staticText text="#{mbean.value.name}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col0" embeddedActions="true"
								headerText="Collaboration Summary">
								<webuijsf:hyperlink id="action1" action="#{mbeanlist.summary}"
									text="Summary">
									<f:param name="collab" value="#{mbean.value.name}" />
								</webuijsf:hyperlink>

							</webuijsf:tableColumn>

						</webuijsf:tableRowGroup>
					</webuijsf:table> <br />
					</div>


				</webuijsf:form>



			</webuijsf:body>
			</webuijsf:html>
		</webuijsf:page>
	</f:view>
</jsp:root>
