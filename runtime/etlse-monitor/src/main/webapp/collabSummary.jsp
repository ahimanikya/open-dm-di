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
			<webuijsf:head id="head" title="DataIntegrator Monitor Summary">
				<webuijsf:link rel="shortcut icon" url="../config/favicon.ico"
					type="image/x-icon" />

			</webuijsf:head>
			<webuijsf:body id="body">

				<jsp:include page="/header.jsp"></jsp:include>

				<!-- Content Page Title 
				<webuijsf:contentPageTitle id="contentPageTitle"
					title="Collab Name:#{mbeanlist.currentCollab}"  /> -->



				<div class="ConMgn"><h:panelGrid columns="3" width="90%">

					<h:panelGroup>

						<webuijsf:form id="selectform">
							<h:panelGrid columns="1">

								<f:facet name="header">
									<h:outputText value="Selection Criteria"  />
								</f:facet>
								<webuijsf:calendar label="#{msg.startDate}"
									selectedDate="#{filter.startDate}" minDate="#{filter.minDate}"
									maxDate="#{filter.maxDate}"
									style="left: 24px; position: relative;"></webuijsf:calendar>
								<webuijsf:calendar label="#{msg.endDate}"
									selectedDate="#{filter.endDate}" minDate="#{filter.minDate}"
									maxDate="#{filter.maxDate}"
									style="left: 28px; position: relative;"></webuijsf:calendar>


								<h:commandButton action="#{filter.select}"
									value="#{msg.selectButton}" style="left: 28px; position: relative;" ></h:commandButton>
								<h:messages showSummary="true" showDetail="true" />

							</h:panelGrid>

						</webuijsf:form>

					</h:panelGroup>

					<h:panelGroup>
						<webuijsf:form id="purgeform">
							<h:panelGrid columns="1" cellspacing="6">

								<f:facet name="header">
									<h:outputText value="Purge Criteria"   />
								</f:facet>
								<h:panelGroup>
									<h:outputLabel value="Purge ALL: "></h:outputLabel>
									<h:selectBooleanCheckbox value="#{filter.purgeAll}"></h:selectBooleanCheckbox>
									<h:outputLabel value="  OR  "></h:outputLabel>
								</h:panelGroup>

								<webuijsf:calendar label="Older than Date:"
									selectedDate="#{filter.olderThanDate}"
									minDate="#{filter.minDate}" maxDate="#{filter.maxDate}"></webuijsf:calendar>

								<h:commandButton action="#{filter.purge}" value="Purge"></h:commandButton>
								<h:messages showSummary="true" showDetail="true" />

							</h:panelGrid>
						</webuijsf:form>

					</h:panelGroup>
					<h:panelGroup>

						<h:panelGrid columns="3" style="background-color:#D3DCE5"
							styleClass="Tbl">
							<f:facet name="header">
								<h:outputLabel value="Summary Total"></h:outputLabel>
							</f:facet>
							<h:outputText value=" " />
							<h:outputText styleClass="TblColHdr" value="Total   " />
							<h:outputText styleClass="TblColHdr" value="Average   " />
							<h:outputText styleClass="TblTdLyt" value="Extracted" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.totalExtracted}" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.averageExtracted}" />
							<h:outputText styleClass="TblTdLyt" value="Loaded" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.totalLoaded}" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.averageLoaded}" />
							<h:outputText styleClass="TblTdLyt" value="Rejected" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.totalRejected}" />
							<h:outputText
								value="#{MbeanSummaryList.summaryTotalData.averageRejected}" />
						</h:panelGrid>

					</h:panelGroup>
				</h:panelGrid></div>

				<webuijsf:form id="summaryTableForm">
					<div class="ConMgn"><br />

					<!-- Embedded Actions --> <webuijsf:table id="dataTable"
						paginateButton="true" paginationControls="true"
						title="DataIntegrator Collaboration(#{mbeanlist.currentCollab})Summary">
						<webuijsf:tableRowGroup id="rowGroup1" selected="true"
							sourceData="#{MbeanSummaryList.list}" sourceVar="mbeanSummary"
							rows="8">

							<webuijsf:tableColumn id="col0" alignKey="first"
								headerText="Execution ID">
								<webuijsf:staticText text="#{mbeanSummary.value.executionID}" />
							</webuijsf:tableColumn>


							<webuijsf:tableColumn id="col1" alignKey="first"
								headerText="Target Table">
								<webuijsf:staticText text="#{mbeanSummary.value.targetTable}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col2" alignKey="first"
								headerText="Start Date">
								<webuijsf:staticText text="#{mbeanSummary.value.startDate}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col3" alignKey="first"
								headerText="End Date">
								<webuijsf:staticText text="#{mbeanSummary.value.endDate}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col4" alignKey="first"
								headerText="Extracted ">
								<webuijsf:staticText text="#{mbeanSummary.value.extracted}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col5" alignKey="first"
								headerText="Loaded ">
								<webuijsf:staticText text="#{mbeanSummary.value.loaded}" />
							</webuijsf:tableColumn>

							<webuijsf:tableColumn id="col6" alignKey="first"
								headerText="Rejected ">

								<webuijsf:hyperlink target="_blank"
									text="#{mbeanSummary.value.rejected}" url="details.jspx"
									action="#{CollabSummaryDetails.details}">

									<f:param name="executionID"
										value="#{mbeanSummary.value.executionID}"></f:param>
									<f:param name="targetTable"
										value="#{mbeanSummary.value.targetTable}"></f:param>


								</webuijsf:hyperlink>

							</webuijsf:tableColumn>



							<webuijsf:tableColumn id="col7" alignKey="first"
								headerText="Exception Message">
								<webuijsf:staticText text="#{mbeanSummary.value.exceptionMsg}" />
							</webuijsf:tableColumn>



						</webuijsf:tableRowGroup>
					</webuijsf:table> <br />
					</div>

				</webuijsf:form>


				<webuijsf:form id="backform">
					<div class="ConMgn"><webuijsf:button action="mbeanlist"
						text="back"></webuijsf:button></div>
				</webuijsf:form>



			</webuijsf:body>
			</webuijsf:html>
		</webuijsf:page>
	</f:view>
</jsp:root>
