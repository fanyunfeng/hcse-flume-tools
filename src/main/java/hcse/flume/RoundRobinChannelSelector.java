package hcse.flume;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

public class RoundRobinChannelSelector extends AbstractChannelSelector {
    private ArrayList<ArrayList<Channel>> channels = new ArrayList<ArrayList<Channel>>();
    private List<Channel> optionalChannels = new ArrayList<Channel>();

    private int size = 0;
    private AtomicInteger counter = new AtomicInteger(-1);

    public void setChannels(List<Channel> channels) {
        super.setChannels(channels);

        this.channels.clear();

        this.size = channels.size();

        for (Channel c : channels) {
            ArrayList<Channel> channelArray = new ArrayList<Channel>();
            channelArray.add(c);

            this.channels.add(channelArray);
        }
    }

    @Override
    public List<Channel> getRequiredChannels(Event event) {
        int index = 0;

        for (;;) {
            index = counter.incrementAndGet();

            if (index >= size) {
                if (counter.compareAndSet(index, 0)) {
                    index = 0;
                    break;
                } else {
                    continue;
                }
            } else {
                break;
            }
        }

        return channels.get(index);
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        return optionalChannels;
    }

    @Override
    public void configure(Context context) {

    }
}
