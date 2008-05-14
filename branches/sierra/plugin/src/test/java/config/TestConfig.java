/*
 * TestConfig.java
 * 
 * Created on Nov 19, 2007, 2:48:06 PM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package config;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author Manish
 */
public class TestConfig {

    // File System Constants
    public static final String USER_DIR = System.getProperty("user.dir");
    public static final String PS = ":"; //Path Separator
    public static final String fs = System.getProperty("file.separator"); //File Separator
    
    public static final int JDBC_DATASOURCE = 0;
    public static final int GOOD_FILE_DATASOURCE = 1;
    public static final int REJECT_FILE_DATASOURCE = 2;
    
    //Test Axion Database Constants
    public static final String AXION_KEY_TABLE = "AXION_KEYS";
    public static final String DB_DRIVER = "org.axiondb.jdbc.AxionDriver";
    public static final String URI_PRIFIX = "jdbc" + PS +  "axiondb";
    public static final String AXION_DB_VERSION = ".VER";
    public static final String DBNAME = "testplugin";
    public static final String DBLOC = getCWD() + fs + "src" + fs  + "test" + fs + "java" + fs + "datapool" + fs + "SampleAxionDB";
    public static final String DBCONNURI = "jdbc:axiondb:" + DBNAME + ":" + DBLOC;
    public static final int HOWMANY = 100;
    
    //Test File DataBase Constants
    public static final String FILEDBLOC = getCWD() + fs + "src" + fs  + "test" + fs + "java" + fs + "datapool" + fs + "SampleFileDB";
    public static final String GOODFILEDBNAME = "GoodFileFormat.txt";
    
    //Eview Config Name
    public static final String configfilename  = "objectdef.xml";
    public static final String configdir = USER_DIR + fs + "src" + fs  + "test" + fs + "java" + fs + "config";
    
    //Current Working Dir
    //Current Working dir
    public static String getCWD(){
        try {
            return new File(".").getCanonicalPath();
        } catch (IOException ex) {
            
        }
        return null;
    }    
    
    public TestConfig() {
    }

}
