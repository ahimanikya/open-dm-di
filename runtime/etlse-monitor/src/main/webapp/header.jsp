<jsp:root version="1.2" xmlns:f="http://java.sun.com/jsf/core"
	xmlns:h="http://java.sun.com/jsf/html"
	xmlns:jsp="http://java.sun.com/JSP/Page"
	xmlns:webuijsf="http://www.sun.com/webui/webuijsf">
	<jsp:directive.page contentType="text/html" />
	<f:subview id="header">
		<webuijsf:page>
			<webuijsf:html id="html">
			
			<webuijsf:body id="body">
				<webuijsf:form id="form">

					<!-- Masthead -->
					<webuijsf:masthead id="masthead"
					productImageURL="../config/ETL.ETLDefinition.png"
					productImageHeight="30" productImageWidth="40"
					userInfo="#{SystemInfo.user}" serverInfo="#{SystemInfo.server}"
					productImageDescription="ETL Collaboration Monitor" />

				</webuijsf:form>



			</webuijsf:body>
			</webuijsf:html>
		</webuijsf:page>
	</f:subview>
</jsp:root>
