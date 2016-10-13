package hcse.flume;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws UnknownHostException {
        InetAddress addresses[] = java.net.InetAddress.getAllByName("123.125.71.81");
        
        for(InetAddress address: addresses){
            System.out.println(address.getCanonicalHostName());
            System.out.println(address.getHostName());
        }
        
        /*
        String localHosts="010.129.001.024";
        
        localHosts = localHosts.replaceAll("^([0]{1,2})", "");
        localHosts = localHosts.replaceAll("\\.([0]{1,2})", ".");
        
        System.out.println(localHosts);
        
        System.out.println("hcse-flume-tools");
        }
        */
    }
}
