package hcse.flume;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeSuffixInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(DateTimeSuffixInterceptor.class);

    // private LRUMap<String, Integer> map;

    private int length = 15;
    private String sourceHeader = "basename";
    private String newHeader = "basename";

    DateTimeSuffixInterceptor(int maxlen, String sourceHeader, String newHeader) {
        this.length = maxlen;
        this.sourceHeader = sourceHeader;
        this.newHeader = newHeader;
    }

    @Override
    public void initialize() {
        // map = new LRUMap<String, Integer>(2048);
    }

    @Override
    public Event intercept(Event event) {
        String value = event.getHeaders().get(sourceHeader);

        if (value != null) {
            int start = value.lastIndexOf('.');
            if (start != -1) {
                start++;

                int end = start + length;

                if (end <= value.length()) {
                    String tag = value.substring(start, end);

                    event.getHeaders().put(newHeader, tag);
                }
            }
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        int start = 0;
        int end = 0;
        for (Event event : events) {
            String value = event.getHeaders().get(sourceHeader);

            if (value != null) {
                start = value.lastIndexOf('.');
                if (start != -1) {
                    start++;

                    end = start + length;

                    if (end <= value.length()) {
                        String tag = value.substring(start, end);

                        event.getHeaders().put(newHeader, tag);
                    }
                }
            }
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private int maxlen = 15;

        private String sourceHeader = Constants.DEFAULTHEADER;
        private String newHeader = Constants.DEFAULTHEADER;

        public Interceptor build() {
            logger.info(String.format("Creating DateTimeSuffixInterceptor: maxlen=%s,sourceHeader=%s,newHeader=%s",
                    maxlen, sourceHeader, newHeader));

            return new DateTimeSuffixInterceptor(maxlen, sourceHeader, newHeader);
        }

        @Override
        public void configure(Context context) {
            maxlen = context.getInteger(Constants.MAXLEN, Constants.MAXLEN_DEFAULT);

            sourceHeader = context.getString(Constants.SOURCEHEADER, Constants.DEFAULTHEADER);
            newHeader = context.getString(Constants.NEWHEADER, Constants.DEFAULTHEADER);
        }
    }

    public static class Constants {
        public static final String MAXLEN = "maxlen";

        public static final int MAXLEN_DEFAULT = 15;

        public static final String SOURCEHEADER = "sourceHeader";
        public static final String NEWHEADER = "newHeader";

        public static final String DEFAULTHEADER = "basename";
    }
}
