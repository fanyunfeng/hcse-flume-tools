package hcse.flume;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class DateTimeSuffixInterceptor implements Interceptor {
    // private LRUMap<String, Integer> map;

    private int length = 13;
    private String sourceHeader = "basename";
    private String newHeader = "basename";

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
                
                String tag = value.substring(start, start + length);

                event.getHeaders().put(newHeader, tag);
            }
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            String value = event.getHeaders().get("sourceHeader");

            if (value != null) {
                int start = value.lastIndexOf('.');
                if (start != -1) {
                    start++;
                    
                    String tag = value.substring(start, start + length);

                    event.getHeaders().put(newHeader, tag);
                }
            }
        }

        return events;
    }

    @Override
    public void close() {

    }

    public class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new DateTimeSuffixInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
