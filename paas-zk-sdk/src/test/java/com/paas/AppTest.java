package com.paas;

import com.paas.zk.zookeeper.ZKClient;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

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
//        testFun();
        assertTrue( true );
    }


//    private static void testFun() {
//        try {
//            ZKClient zKClient =new ZKClient("114.215.202.56:32181",3000);
//            if(!zKClient.exists("/liwx/test/1"))
//                zKClient.createNode("/liwx/test/1", "{a:1,b:2}");
//            System.out.println("fun:"+zKClient.getNodeData("/liwx/test/1"));
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//    }

//    private static void testWatch() {
//        try {
//            ZKClient zKClient =new ZKClient("114.215.202.56:32181",3000);
//            zKClient.getNodeData("/liwx/test/1", new Watcher(){
//                @Override
//                public void process(WatchedEvent event) {
//                    // TODO Auto-generated method stub
//
//                }
//
//            });
//
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//    }
}
