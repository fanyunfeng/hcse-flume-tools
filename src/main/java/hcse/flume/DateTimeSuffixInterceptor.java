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

    private int start = 11;
    private String sourceHeader = "basename";
    private String newHeader = "basename";
    
    private String tagHeader = "timetag";

    DateTimeSuffixInterceptor(int start, String sourceHeader, String newHeader) {
        this.start = start;
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
                StringBuffer buf = new StringBuffer(value.substring(start));

                for(int i=this.start; i<buf.length(); i++){
                    buf.setCharAt(i, '0');
                }

                event.getHeaders().put(tagHeader, buf.toString());
            }

            start = value.indexOf('.');
            event.getHeaders().put(newHeader, value.substring(0, start));
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        int start = 0;
        for (Event event : events) {
            String value = event.getHeaders().get(sourceHeader);

            if (value != null) {
                start = value.lastIndexOf('.');
                if (start != -1) {
                    start++;
                    StringBuffer buf = new StringBuffer(value.substring(start));

                    for(int i=this.start; i<buf.length(); i++){
                        buf.setCharAt(i, '0');
                    }

                    event.getHeaders().put(tagHeader, buf.toString());
                }
                
				start = value.indexOf('.');
				event.getHeaders().put(newHeader, value.substring(0, start));
            }
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private int start = 11;

        private String sourceHeader = Constants.DEFAULTHEADERNAME;
        private String newHeader = Constants.DEFAULTHEADERNAME;
        
        private String tagHeader = Constants.TAGHEADERNAME;

        public Interceptor build() {
            logger.info(String.format("Creating DateTimeSuffixInterceptor: start=%d,sourceHeader=%s,newHeader=%s,tagHeader",
                    start, sourceHeader, newHeader, tagHeader));

            return new DateTimeSuffixInterceptor(start, sourceHeader, newHeader);
        }

        @Override
        public void configure(Context context) {
            start = context.getInteger(Constants.START, Constants.START_DEFAULT);

            sourceHeader = context.getString(Constants.SOURCEHEADER, Constants.DEFAULTHEADERNAME);
            newHeader = context.getString(Constants.NEWHEADER, Constants.DEFAULTHEADERNAME);
            
            tagHeader = context.getString(Constants.TAGHEADER, Constants.DEFAULTHEADERNAME);
        }
    }

    public static class Constants {
        public static final String START = "start";
        public static final int START_DEFAULT = 11;


        public static final String SOURCEHEADER = "sourceHeader";
        public static final String NEWHEADER = "newHeader";

        public static final String DEFAULTHEADERNAME = "basename";
        
        public static final String TAGHEADER = "tagHeader";
        public static final String TAGHEADERNAME = "timetag";
    }
}
