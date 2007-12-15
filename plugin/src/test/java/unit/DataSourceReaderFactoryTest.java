/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package unit;

import com.sun.dm.dimi.datareader.*;
import com.sun.mdm.index.dataobject.DataObjectReader;
import com.sun.mdm.index.dataobject.InvalidRecordFormat;
import com.sun.mdm.index.parser.ParserException;
import config.DBGenerator;
import config.TestConfig;
import config.TestSuiteTools;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestCase;

/**
 *
 * @author Manish
 */
public class DataSourceReaderFactoryTest extends TestCase {
    private String configpath;
    
    public DataSourceReaderFactoryTest(String testName) {
        super(testName);
    }            

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.configpath = TestConfig.configdir;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        
    }

    
    public void testSetEViewConfigFilePath() throws ParserException, FileNotFoundException {
        // Generate the DB to test on
        DBGenerator dbcodegen = new DBGenerator();
        dbcodegen.generate();
        System.out.println("Test API : testSetEViewConfigFilePath");
        boolean result = DataSourceReaderFactory.setEViewConfigFilePath(this.configpath, TestConfig.configfilename);
        assertTrue(result);
    }


    public void testGetEViewConfigFilePath() {
        System.out.println("Test API: testGetEViewConfigFilePath");
        String expResult = this.configpath + TestConfig.fs + TestConfig.configfilename;
        String result = DataSourceReaderFactory.getEViewConfigFilePath();
        assertEquals(expResult, result);
    }    

    
    public void testGetGoodFileDataObjectReader() throws Exception {
        System.out.println("Test API: testGetGoodFileDataObjectReader");
        String filepath = TestConfig.FILEDBLOC;
        String filename = TestConfig.GOODFILEDBNAME;
        boolean isGoodFile = true;
        boolean specialMode = true;
        DataObjectReader result = DataSourceReaderFactory.getNewDataObjectReader(filepath, filename, isGoodFile, specialMode);
        
        assertNotNull(result);
    }
    
    /*
    public void testGetDataObjectDBReader() throws IOException, InvalidRecordFormat{
        System.out.println("Test API: Database Reader : testGetDataObjectDBReader");
        DataObjectReader result = DataSourceReaderFactory.getDataSourceReader(TestConfig.DBCONNURI, true);
        assertNotNull(result);
    }
    
    
    public void testCountProducedDataObjects() throws InvalidRecordFormat, IOException, SQLException{
        System.out.println("Test API: testCountProducedDataObjects");
        Connection conn = TestSuiteTools.getInstance().getTestDBConnection();
        DataObjectReader doreader = DataSourceReaderFactory.getDataSourceReader(TestConfig.DBCONNURI, true);
        
        // Count Records in the DB
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SBYN_PATIENTVIEW LEFT OUTER JOIN SBYN_ADDRESS ON SBYN_PATIENTVIEW.PATIENTVIEWID = SBYN_ADDRESS.PATIENTVIEWID");
        rs.next();
        int dbquerycount = Integer.parseInt(rs.getString(1));
        
        // Count Data Objects Produced
        int docount = 0;
        while (doreader.readDataObject() != null){
            docount++;
        }
        assertEquals(dbquerycount,docount);
    }
*/
}
