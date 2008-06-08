/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.jbi.engine.etl.persistence.impl;

import java.util.Properties;
import javax.jbi.messaging.DeliveryChannel;
import javax.jbi.messaging.ExchangeStatus;
import javax.jbi.messaging.InOut;
import javax.jbi.messaging.MessageExchange;
import javax.jbi.messaging.MessageExchangeFactory;
import javax.jbi.messaging.MessagingException;
import javax.jbi.messaging.NormalizedMessage;
import javax.wsdl.Output;
import javax.xml.transform.dom.DOMSource;

import com.sun.jbi.alerter.NotificationEvent;
import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.Localizer;
import com.sun.etl.engine.spi.DBConnectionProvider;
import com.sun.jbi.engine.etl.ETLInOutQueueElement;
import com.sun.jbi.engine.etl.ETLSEDBConnectionProvider;
import com.sun.jbi.engine.etl.XmlUtil;
import com.sun.jbi.engine.etl.mbean.ETLSERuntimeConfigurationMBean;
import com.sun.jbi.engine.etl.persistence.BaseDAO;
import com.sun.jbi.engine.etl.persistence.DAOFactory;
import com.sun.jbi.engine.etl.persistence.EngineState;
import com.sun.jbi.nms.exchange.ExchangePattern;
import com.sun.jbi.nms.wsdl11wrapper.HelperFactory;
import com.sun.jbi.nms.wsdl11wrapper.WrapperBuilder;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.utils.RuntimeAttribute;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author admin
 */
public class MessagePersistenceHandler {

    public static final int DERBY_DB_TYPE = 0;
    private static ETLSERuntimeConfigurationMBean runtimeMBean;
    private static transient final Logger mLogger = Logger.getLogger(MessagePersistenceHandler.class.getName());
    private static transient final Localizer mLoc = Localizer.get();

    public static void setPersistenceConfigMBean(ETLSERuntimeConfigurationMBean runtimeMBean) {
        MessagePersistenceHandler.runtimeMBean = runtimeMBean;
    }

    public static Properties getPersistenceDBConfig() throws Exception {

        Properties props = new Properties();

        props.put(DBConnectionFactory.PROP_DRIVERCLASS, runtimeMBean.getPersistenceDBDriverClass());
        props.put(DBConnectionFactory.PROP_USERNAME, runtimeMBean.getPersistenceDBUserId());
        props.put(DBConnectionFactory.PROP_PASSWORD, runtimeMBean.getPersistenceDBPassword());
        props.put(DBConnectionFactory.PROP_URL, runtimeMBean.getPersistenceDBUrl());


        return props;
    }

    private static Connection getConnection() throws BaseException, Exception {
        DBConnectionProvider connProvider = new ETLSEDBConnectionProvider();
        Connection conn = connProvider.getConnection(getPersistenceDBConfig());
        return conn;
    }

    public static void persistMessage(InOut inOut) throws SQLException {
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = MessagePersistenceHandler.getConnection();
            //mLogger.info("got db connection to derby");
            DAOFactory factory = DAOFactory.getDAOFactory(DERBY_DB_TYPE);
            String msgExchangeId = inOut.getExchangeId();
            String serviceName = inOut.getEndpoint().getServiceName().toString();
            String operationName = inOut.getOperation().toString();
            DOMSource content = (DOMSource) inOut.getInMessage().getContent();
            Writer writer = new StringWriter();

            String serializedMessage = XmlUtil.toXml(content.getNode(), "UTF-8", false);
            int status = EngineState.RUNNABLE;
            BaseDAO dao = factory.createETLMessagePipelineDAO(msgExchangeId, serviceName, operationName, serializedMessage, status);
            pstmt = conn.prepareStatement(dao.getInsertStmt());
            dao.fillInsertStmt(pstmt);
            pstmt.executeUpdate();
        //mLogger.info("created tables successfully");
        } catch (BaseException e) {
            mLogger.log(Level.SEVERE, e.getMessage());
        } catch (SQLException e) {
            mLogger.log(Level.SEVERE, e.getMessage());
            throw e;
        } catch (Exception e) {
            mLogger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void UpdateMessage(ETLInOutQueueElement inOut, int status) {
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            DAOFactory factory = DAOFactory.getDAOFactory(DERBY_DB_TYPE);
            String msgExchangeId = inOut.getMsgExchId();
            String serviceName = inOut.getServiceName();
            String operationName = inOut.getOperationName();
            BaseDAO dao = factory.createETLMessagePipelineDAO(msgExchangeId, serviceName, operationName, "", status);
            conn = MessagePersistenceHandler.getConnection();
            pstmt = conn.prepareStatement(dao.getUpdateStmt());
            dao.fillUpdateStmt(pstmt);
            pstmt.executeUpdate();
        } catch (BaseException e) {
            mLogger.log(Level.SEVERE, e.getMessage());
        } catch (SQLException e) {
            mLogger.log(Level.SEVERE, e.getMessage());
        } catch (Exception e) {
            mLogger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void deleteSuccessMessage(ETLInOutQueueElement inOut) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            DAOFactory factory = DAOFactory.getDAOFactory(DERBY_DB_TYPE);
            String msgExchangeId = inOut.getMsgExchId();
            String serviceName = inOut.getServiceName();
            String operationName = inOut.getOperationName();
            int status = EngineState.RUNNABLE;
            BaseDAO dao = factory.createETLMessagePipelineDAO(msgExchangeId, serviceName, operationName, "", status);
            conn = MessagePersistenceHandler.getConnection();
            pstmt = conn.prepareStatement(dao.getDeleteStmt());
            dao.fillDeleteStmt(pstmt);
            pstmt.executeUpdate();
        } catch (BaseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                mLogger.log(Level.SEVERE, e.getMessage());
            }
        }
    }
}
