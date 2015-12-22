package hcse.flume;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        String localHosts="010.129.001.024";
        
        localHosts = localHosts.replaceAll("^([0]{1,2})", "");
        localHosts = localHosts.replaceAll("\\.([0]{1,2})", ".");
        
        System.out.println(localHosts);
        
        System.out.println("hcse-flume-tools");
    }
}
