package hcse.flume;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;

public class HcseGangliaServer extends GangliaServer {
    private final String CONF_HOST_NAME = "hostname";

    public void configure(Context context) {
        super.configure(context);
        
        String localHosts = context.getString(this.CONF_HOST_NAME);

        if (StringUtils.isNotEmpty(localHosts)) {
            hostname = localHosts;
        }
    }
}
