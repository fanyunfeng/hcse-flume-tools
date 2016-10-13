package hcse.flume;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;

public class HcseGangliaServer extends GangliaServer {
    public static int STAT_ITEM_ACTION_IGNOR = 1;
    public static int STAT_ITEM_ACTION_INC = 2;
    public static int STAT_ITEM_ACTION_CPS = 4;

    private final static StatisticItem DefaultConf = new StatisticItem(0);

    static class StatisticConfig {
        public int state;

        public StatisticConfig(int state) {
            this.state = state;
        }
    }

    static class StatisticItem {
        public int state;

        private long lastReport;
        public float lastValue;

        public StatisticItem(int state) {
            lastReport = 0;
            lastValue = 0;
            this.state = state;
        }
    }

    private long now;
    private final String CONF_HOST_NAME = "hostname";

    private HashMap<String, StatisticItem> data = new HashMap<String, StatisticItem>();
    private HashMap<String, StatisticConfig> config = new HashMap<String, StatisticConfig>();

    public void configure(Context context) {
        super.configure(context);

        String localHosts = context.getString(this.CONF_HOST_NAME);

        if (StringUtils.isNotEmpty(localHosts)) {
            localHosts = localHosts.replaceAll("^([0]{1,2})", "");
            localHosts = localHosts.replaceAll("\\.([0]{1,2})", ".");

            hostname = localHosts;
        }
    }

    public HcseGangliaServer() {
        addStatConf("SINK\\..*\\.EventDrainSuccessCount", STAT_ITEM_ACTION_INC);
        addStatConf("SOURCE\\..*\\.EventAcceptedCount", STAT_ITEM_ACTION_INC);
        addStatConf("SOURCE\\..*\\.EventReceivedCount", STAT_ITEM_ACTION_INC);
    }

    private void addStatConf(String name, int actions) {
        config.put(name, new StatisticConfig(actions));
    }

    private String getIncValue(StatisticItem item, float value) {
        value = value - item.lastValue;

        return Float.toString(value);
    }

    private String getCpsValue(StatisticItem item, float value) {
        value = value - item.lastValue;

        value /= (now - item.lastReport) / 1000;

        return Float.toString(value);
    }

    private StatisticItem InitializeStatisticItem(String name) {

        for (Map.Entry<String, StatisticConfig> c : config.entrySet()) {
            if (name.matches(c.getKey())) {
                StatisticItem ret = new StatisticItem(c.getValue().state);

                data.put(name, ret);
                return ret;
            }
        }

        data.put(name, DefaultConf);
        return DefaultConf;
    }

    @Override
    void startReport() {
        now = System.currentTimeMillis();
    }

    void createGangliaMessage(String name, String value) {
        StatisticItem item = data.get(name);

        if (item == null) {
            item = InitializeStatisticItem(name);
        }

        if (0 != (item.state & STAT_ITEM_ACTION_IGNOR)) {
            super.createGangliaMessage(name, value);
        }

        if (0 == (item.state & (~STAT_ITEM_ACTION_IGNOR))) {
            return;
        }

        float fValue;
        try {
            fValue = Float.parseFloat(value);
        } catch (NumberFormatException e) {
            return;
        }

        if (item.lastReport != 0) {
            if (0 != (item.state & STAT_ITEM_ACTION_INC)) {
                String nvalue = getIncValue(item, fValue);
                if (nvalue != null) {
                    super.createGangliaMessage(name + ".inc", nvalue);
                }
            }

            if (0 != (item.state & STAT_ITEM_ACTION_CPS)) {
                String nvalue = getCpsValue(item, fValue);
                if (nvalue != null) {
                    super.createGangliaMessage(name + ".cps", nvalue);
                }
            }
        }

        item.lastValue = fValue;
        item.lastReport = now;
    }
}
