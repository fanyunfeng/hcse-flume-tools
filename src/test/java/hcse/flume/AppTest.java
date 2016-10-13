package hcse.flume;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        String name1 = "SINK.k1.EventDrainSuccessCount";
        assertTrue(name1.matches("SINK\\..*\\.EventDrainSuccessCount"));
        
        String name2 = "SOURCE.k1.EventDrainSuccessCount";
        assertTrue(!name2.matches("SINK\\..*\\.EventDrainSuccessCount"));
        
        
        //addStatConf("SOURCE\\..*\\.EventAcceptedCount", STAT_ITEM_ACTION_INC);
        //addStatConf("SOURCE\\..*\\.EventReceivedCount", STAT_ITEM_ACTION_INC);
    }
}
