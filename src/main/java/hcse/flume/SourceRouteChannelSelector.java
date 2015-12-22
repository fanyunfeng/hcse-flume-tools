package hcse.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

public class SourceRouteChannelSelector extends AbstractChannelSelector {
    private int index = 0;
    private LRUMap<String, Integer> route;

    private int size = 0;
    private ArrayList<ArrayList<Channel>> channels;
    private List<Channel> optionalChannels = new ArrayList<Channel>();

    private int getRoute(Event event) {
        int ret = -1;

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
            sb.append(entry.getValue());
        }

        String headers = sb.toString();

        synchronized (route) {
            Integer i = route.get(headers);

            if (i == null) {
                ret = index++;

                if (index >= size) {
                    index = 0;
                }

                route.put(headers, ret);
            } else {
                ret = i;
            }
        }

        return ret;
    }

    public void setChannels(List<Channel> channels) {
        super.setChannels(channels);

        this.size = channels.size();
        this.channels = new ArrayList<ArrayList<Channel>>(size);

        for (Channel c : channels) {
            ArrayList<Channel> channelArray = new ArrayList<Channel>(1);
            channelArray.add(c);

            this.channels.add(channelArray);
        }
    }

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        int index = getRoute(event);

        return channels.get(index);
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        return optionalChannels;
    }

    @Override
    public void configure(Context context) {
        route = new LRUMap<String, Integer>(2048);
    }
}
