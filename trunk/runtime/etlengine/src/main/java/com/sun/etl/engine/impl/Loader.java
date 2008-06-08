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
 * [year] [name of copyright ownFer]
 */

/*
 * @(#)Loader.java
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 *
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineExecEvent;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.File;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.concurrent.BlockingQueue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import com.sun.etl.engine.Localizer;
import org.axiondb.io.FileUtil;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.CharStreamTokenizer;
import org.axiondb.io.BufferedDataInputStream;

import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.utils.AttributeMap;

import com.sun.sql.framework.utils.StringUtil;

/**
 * This class loads the data directly from the flatfile into a database.
 * Similar to SQL*Loader utility of Oracle.
 *
 * @author karthikeyan s
 */
public class Loader extends SimpleTask {
    
    private ETLTaskNode taskNode;
    
    private AxionFileSystem FS;

	private static transient final Logger mLogger = Logger.getLogger(Loader.class.getName());

    private static transient final Localizer mLoc = Localizer.get();
    
    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");
    
    /** Attribute key for indicating job queue size */
    public static final String KEY_JOBQUEUESIZE = "jobQueueSize";
    
    /** Attribute key for indicating partitions size */
    public static final String KEY_PARTITIONSIZE = "partitionSize";
    
    private static int batchSize = 5000;
    private static String fileLocation;
    private static String _lineSep;
    private static String fieldDelimiter;
    private static String qualifier;
    private int partitions = 1;
    private int jobQueueSize = 2;
    private int rowsToSkip = 0;
    private int colCount = 0;
    private boolean _isQuoted = false;
    private char[] _fieldSepChar;
    private char[] _qualifierChar;
    protected String[] _lineSeps;
    protected char[] [] _lineSepsChar;
    protected Pattern _qPattern;
    protected Pattern _qqPattern;
    private CharStreamTokenizer _streamTokenizer;
    
    private ExecutorService consumerThread;
    private BlockingQueue<PreparedStatement> jobQueue;
    private BlockingQueue<PreparedStatement> preparedStmtPool;
    private BatchProcessingConsumer consumer;
    
    public Loader() {
        FS = new AxionFileSystem();
    }
    
    /**
     * Cleans up resources.
     */
    public void cleanUp() {
        super.cleanUp();
    }
    
    /**
     * Handles ETLException
     *
     * @param ex BaseException that needs to be handled
     */
    public void handleException(ETLException ex) {
        super.handleException(ex);
    }
    
    /**
     * Process the given TaskNode
     *
     * @param node
     * @throws ETLException indicating processing problem.
     * @return Sucess or failure of execution of tasknode
     */
    public String process(ETLTaskNode node) throws ETLException {
        if (node == null) {
            throw new ETLException("Task node is null....");
        } else {
            this.taskNode = node;
        }
        
        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }
        String msg = MSG_MGR.getString("MSG_LOADER_started");
		mLogger.log(Level.INFO,mLoc.loc("INFO285: Msg: {0}",msg));
		//node.fireETLEngineLogEvent(msg, Logger.DEBUG);
        try {
            List<DBConnectionParameters> connList = node.getParent().getConnectionDefList();
            AttributeMap attrMap = node.getAttributeMap();
            String batchSizeStr = (String) attrMap.getAttributeValue("batchSize"); //NOI18N
            if (!StringUtil.isNullString(batchSizeStr)) {
                int bsInt = StringUtil.getInt(batchSizeStr);
                if (bsInt > 0) {
                    batchSize = bsInt;
                }
            }
            
            // get insert SQLPart...
            SQLPart insertSQLPart = node.getStatement(SQLPart.STMT_INSERT); //NOI18N
            if (insertSQLPart == null) {
                throw new ETLException(DN + "Missing required insert SQLPart element");
            }
            
            String insertStmt = insertSQLPart.getSQL();
            if (insertStmt == null) {
                throw new ETLException(DN + "Missing required insert statement");
            }
            
            // initialize all task related properties.
            initializeProperties(attrMap);
            
            Connection conn = getConnection(insertSQLPart.getConnectionPoolName(), connList);
            setAutoCommitIfRequired(conn, false);
            
            // create partitions and read.
            PartitionTable partitionTable = new PartitionTable(conn, insertStmt);
            partitionTable.partitionAndRead();
            
            ETLEngineExecEvent evnt =
                    new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_COMPLETED,
                    node.getTableName(), "" +
                    node.getContext().getStatistics().getTableExecutionId(node.getTableName()));
            taskNode.fireETLEngineExecutionEvent(evnt);
            
            String successMsg = MSG_MGR.getString(
                    "MSG_extractor_insert_success", new Long(partitionTable.getInsertCount()));
			mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",successMsg));
            //taskNode.fireETLEngineLogEvent(successMsg, Logger.INFO);
            
            taskNode.getContext().getStatistics().setRowsInsertedCount(
                    taskNode.getTableName(), partitionTable.getInsertCount());
            
        } catch(Exception ex) {
            String failureMsg = MSG_MGR.getString("MSG_LOADER_failed");
			mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",failureMsg));
           //node.fireETLEngineLogEvent(failureMsg, Logger.DEBUG);
            //Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, this, DN + failureMsg, ex);
            mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",DN + failureMsg), ex);
            throw new ETLException(ex);
        }
        return "";
    }
    
    private class PartitionTable {
        
        private ExecutorService partitionReaderPool;
        private Connection conn;
        private String sqlStmt;
        private long insertCount = 0;
        
        public PartitionTable(Connection conn, String stmt){
            this.conn = conn;
            this.sqlStmt = stmt;
        }
        
        public void partitionAndRead() throws Exception {
            
            long fileLength = FileUtil.getLength(new File(fileLocation));
            long approxPartitionSize = fileLength;
            if(fileLength > 8192){
                // use partitions only if file length is more than 8 KB.
                approxPartitionSize = fileLength/partitions;
            }
            
            BufferedDataInputStream dataStream = new BufferedDataInputStream(
                    FS.open(new File(fileLocation)), 8192);
            partitionReaderPool = Executors.newFixedThreadPool(partitions);
            Future<Long> result = consumerThread.submit(consumer);
            long offset = getStartOffset(dataStream);
            for(int i = 0; i < partitions; i++) {
                long start = offset;
                offset += approxPartitionSize;
                if(offset > fileLength){
                    offset = fileLength;
                } else {
                    dataStream.seek(offset);
                    _streamTokenizer.skipLine(dataStream);
                    offset = dataStream.getPos();
                }
                // use this offset and read the file.
                PartitionReader reader = new PartitionReader(
                        start, offset, conn, this.sqlStmt, i + 1);
                partitionReaderPool.submit(reader);
            }
            this.insertCount += result.get();
            
            // finally commit the changes.
            
            try {
                conn.commit();
            } catch (SQLException ex) {
                // failed to commit. log this.
                this.insertCount = 0;
            }
        }
        
        public long getInsertCount() {
            return this.insertCount;
        }
    }
    
    private class PartitionReader implements Runnable {
        
        private long startOffset;
        private long endOffset;
        private BufferedDataInputStream dataStream;
        private Connection conn;
        private String statement;
        private PreparedStatement prepStmt;
        private int id;
        
        public PartitionReader(long start, long end,
                Connection conn, String stmt, int id) {
            try {
                startOffset = start;
                endOffset = end;
                this.conn = conn;
                this.statement = stmt;
                this.id = id;
                dataStream = new BufferedDataInputStream(FS.open(new File(fileLocation)), 8192);
                dataStream.seek(startOffset);
            } catch (Exception ex) {
				mLogger.log(Level.INFO,mLoc.loc("ERRO092: global"), ex);
                //java.util.logging.Logger.getLogger("global").log(Level.SEVERE, null, ex);
            }
        }
        
        public void run() {
            long insertCount = 0;
            System.out.println("Initializing Producer " + this.id + " ...");
            try {
                prepStmt = conn.prepareStatement(statement);
                long fileOffset = startOffset;
                int localBatchSize = 0;
                
                String insertStartMsg = MSG_MGR.getString("MSG_extractor_insert_attempt");
				mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",insertStartMsg));
                //taskNode.fireETLEngineLogEvent(insertStartMsg, Logger.DEBUG);
                
                String prepStmtMsg = MSG_MGR.getString("MSG_extractor_show_prep_stmt", statement);
				mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",prepStmtMsg));
                //taskNode.fireETLEngineLogEvent(prepStmtMsg, Logger.DEBUG);
                if (prepStmt == null) {
                    String errMsg = MSG_MGR.getString("MSG_extractor_invalid_insert");
					mLogger.log(Level.INFO,mLoc.loc("INFO085: {0}",errMsg));
                    //taskNode.fireETLEngineLogEvent(errMsg, Logger.DEBUG);
                    throw new BaseException("Invalid insert statement.");
                }
                
                while (true) {
                    if (-1 == fileOffset || (fileOffset  >= endOffset)) {
                        break;
                    }
                    // do validation.
                    String[] results = _streamTokenizer.readAndSplitLine(dataStream, fileOffset, colCount, false);
                    for (int i = 1; i <= colCount; i++) {                        
                        prepStmt.setObject(i, results[i-1]);
                    }
                    try {
                        if (batchSize > 1) {
                            prepStmt.addBatch();
                            localBatchSize++;
                            if (localBatchSize == batchSize) {
                                String engineState = (String) taskNode.getContext().getValue("engineState"); //NO18N
                                if (engineState != null && engineState.trim().equalsIgnoreCase("not-active")) { //NOI18N
                                    prepStmt.close();
                                    throw new ETLException("Engine is no longer active"); // -- engine no longer active
                                }
                                
                                jobQueue.put(prepStmt);
                                
                                prepStmt = preparedStmtPool.take();
                                if(null == prepStmt) {
                                    prepStmt = conn.prepareStatement(statement);
                                }
                                localBatchSize = 0;
                            }
                        } else {
                            prepStmt.executeUpdate();
                            insertCount++;
                        }
                    } catch (Exception se) {
                        String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", se.getMessage());
						mLogger.log(Level.INFO,mLoc.loc("ERRO085: {0} ",se.getMessage()), se);
                        //Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, null, errMsg, se);
						mLogger.log(Level.INFO,mLoc.loc("ERRO185: Info-{0}",errMsg));
                        //taskNode.fireETLEngineLogEvent(errMsg, Logger.ERROR);
                    }
                    fileOffset = dataStream.getPos();
                }
                
                if (batchSize > 1) {
                    consumer.stopConsuming();
                    if(localBatchSize != 0) {
                        jobQueue.put(prepStmt);
                    }
                }
                System.out.println("Exiting Producer " + this.id);
            } catch (Exception ex) {
					mLogger.log(Level.INFO,mLoc.loc("ERRO092: global"), ex);
                //java.util.logging.Logger.getLogger("global").log(Level.SEVERE, null, ex);
            } finally {
                if(conn != null) {
                    try {
                        conn.close();
                    } catch(Exception ex) {
                        conn = null;
                    }
                }
            }
        }
    }
    
    /**
     * Skips the specified number of rows
     * and positions the file pointer at the start of first valid row.
     *
     * @param inputstream to read
     * @return file offset
     */
    private long getStartOffset(BufferedDataInputStream stream) {
        try {
            // set it to the beginning.
            stream.seek(0);
            int i = 0;
            while(i++ < rowsToSkip) {
                _streamTokenizer.skipLine(stream);
            }
            return stream.getPos();
        } catch (Exception ex) {
				mLogger.log(Level.INFO,mLoc.loc("ERRO092: global"), ex);
            //java.util.logging.Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
        return -1;
    }
    
    public String fixEscapeSequence(String srcString) {
        // no escape sequences, return string as is
        if (srcString == null || srcString.length() < 2)
            return srcString;
        
        char[] srcArray = srcString.toCharArray();
        char[] tgtArray = (char[]) srcArray.clone();
        Arrays.fill(tgtArray, ' ');
        int j = 0;
        // e.g. curString = "\\r\\n" = {'\\', 'r', '\\', 'n'}
        for (int i = 0; i < srcArray.length; i++) {
            char c = srcArray[i];
            if ((i + 1) == srcArray.length) {
                // this is last char, so put it as is
                tgtArray[j++] = c;
                break;
            }
            
            if (c == '\\') {
                switch (srcArray[i + 1]) {
                case 't':
                    c = '\t';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 'n':
                    c = '\n';
                    break;
                case 'f':
                    c = '\f';
                    break;
                }
                i++;
            }
            tgtArray[j++] = c;
        }
        
        return new String(tgtArray, 0, j);
    }
    
    /**
     * Creates a partition table for the given attributes.
     *
     * @param map of attributes
     */
    private void initializeProperties(AttributeMap attrMap) {
        // Create a partition table using file location,
        // no of partitions, type of flatfile(fixed width / delimited),etc.
        // each parameter is a name=value pair seperated by semicolon.
        String lineSep = System.getProperty("line.separator");
        String recordDelimiter = (String) attrMap.get("RECORDDELIMITER").getAttributeValue();
        if(recordDelimiter != null) {
            _lineSep = fixEscapeSequence(recordDelimiter);
        } else {
            _lineSep = fixEscapeSequence(lineSep);
        }
        // Support multiple record delimiter for delimited
        StringTokenizer tokenizer = new StringTokenizer(_lineSep, " ");
        ArrayList tmpList = new ArrayList();
        while(tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            tmpList.add(token);
        }
        
        _lineSeps = (String[])tmpList.toArray(new String[0]);
        _lineSepsChar = new char [tmpList.size()][];
        for(int i=0, I=tmpList.size(); i < I; i++){
            _lineSepsChar[i] = ((String)tmpList.get(i)).toCharArray();
        }
        
        fieldDelimiter = (String) attrMap.get("FIELDDELIMITER").getAttributeValue();
        if(fieldDelimiter != null && fieldDelimiter.length() != 0) {
            _fieldSepChar = fieldDelimiter.toCharArray();
        } else {
            _fieldSepChar = new char[0];
        }
        qualifier = (String) attrMap.get("QUALIFIER").getAttributeValue();
        if(qualifier != null && qualifier.length() != 0) {
            _isQuoted = true;
            _qualifierChar = fixEscapeSequence(qualifier).toCharArray();
        } else {
            _qualifierChar = new char[0];
        }
        
        fileLocation = (String) attrMap.get("FILENAME").getAttributeValue();
        
        try {
            rowsToSkip = Integer.valueOf(((String)attrMap.get("ROWSTOSKIP").getAttributeValue()));
        } catch (Exception ex) {
            // ignore. Assume no rows to skip.
        }
        colCount = Integer.parseInt(((String)attrMap.get("COLUMNCOUNT").getAttributeValue()));
        _streamTokenizer = new CharStreamTokenizer(_fieldSepChar, _lineSepsChar,
                _qualifierChar, _isQuoted);
        
        String partitionSize = (String) attrMap.getAttributeValue(KEY_PARTITIONSIZE);
        try {
            partitions = Integer.parseInt(partitionSize);
        } catch(Exception ex) {
            // use the default.
        }
        
        String jobQ = (String) attrMap.getAttributeValue(KEY_JOBQUEUESIZE);
        try {
            jobQueueSize = Integer.parseInt(jobQ);
        } catch(Exception ex) {
            // use the default.
        }
        
        consumerThread = Executors.newSingleThreadExecutor();
        jobQueue = new ArrayBlockingQueue<PreparedStatement>(jobQueueSize, true);
        preparedStmtPool = new ArrayBlockingQueue<PreparedStatement>(jobQueueSize);
        consumer = new BatchProcessingConsumer(partitions);
    }
    
    private class BatchProcessingConsumer implements Callable<Long> {
        
        private AtomicInteger producerCount;
        
        public BatchProcessingConsumer(int producerCount) {
            this.producerCount = new AtomicInteger(producerCount);
        }
        
        public Long call() throws Exception {
            long insertCount = 0;
            try {
                while(producerCount.get() != 0) {
                    PreparedStatement prepStmt = jobQueue.take();
                    if(prepStmt != null) {
                        int[] rows = null;
                        try {
                            rows = prepStmt.executeBatch();
                            insertCount += rows.length;
                        } catch (SQLException ex) {
                            // log this.
                        }
                        prepStmt.clearBatch();
                        try {
                            preparedStmtPool.put(prepStmt);
                        } catch (Exception ex) {
                            prepStmt.close();
                        }
                    }
                }
            } catch (InterruptedException ex) {
                // come out and finish the remaining jobs.
            }
            // execute the remaining jobs and exit.
            for(int i = 0, I = jobQueue.size(); i < I; i++) {
                PreparedStatement prepStmt = jobQueue.poll();
                if(prepStmt != null) {
                    int[] rows = null;
                    try {
                        rows = prepStmt.executeBatch();
                        insertCount += rows.length;
                        prepStmt.close();
                    } catch (SQLException ex) {
                        // log this.
                        ex.printStackTrace();
                    }
                }
            }
            System.out.println("Consumer thread exiting...");
            return insertCount;
        }
        
        public void stopConsuming() {
            producerCount.decrementAndGet();
        }
    }
}