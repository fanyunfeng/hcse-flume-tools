package hcse.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DateTimeSuffixInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(DateTimeSuffixInterceptor.class);

    private int validTimeTagLength;
    private String sourceHeader;
    private String newHeader;

    private String timeTagHeader;
    private String dateHeader;

    private char dateSeparator;

    public DateTimeSuffixInterceptor(int validTimeTagLength, String sourceHeader, String newHeader, String timeTagHeader, String dateHeader, char dateSeparator) {
        this.validTimeTagLength = validTimeTagLength;

        this.sourceHeader = sourceHeader;
        this.newHeader = newHeader;

        this.timeTagHeader = timeTagHeader;
        this.dateHeader = dateHeader;
        this.dateSeparator = dateSeparator;
    }

    public void initialize() {
    }

    private Event handleEvent(Event event) {
        Map<String, String> headers = event.getHeaders();
        String fileName = headers.get(sourceHeader);

        if (fileName == null) {
            return null;
        }

        //add timetag
        int timeStart = fileName.lastIndexOf('.');

        if (timeStart == -1) {
            return null;
        }

        timeStart++;
        StringBuffer buf = new StringBuffer(fileName.substring(timeStart));

        for (int i = this.validTimeTagLength; i < buf.length(); i++) {
            buf.setCharAt(i, '0');
        }

        headers.put(timeTagHeader, buf.toString());

        //add date
        int dateEnd = fileName.indexOf(dateSeparator, timeStart);

        if (dateEnd == -1) {
            return null;
        }

        headers.put(dateHeader, fileName.substring(timeStart,
                dateEnd));

        //add basename
        int baseNameEnd = fileName.indexOf('.');

        if (baseNameEnd == -1) {
            return null;
        }

        headers.put(newHeader, fileName.substring(0, baseNameEnd));

        return event;
    }

    public Event intercept(Event event) {
        return handleEvent(event);
    }

    public List<Event> intercept(List<Event> events) {
        int start = 0;
        Event result;

        ArrayList<Event> results = new ArrayList<Event>(events.size());

        for (Event event : events) {
            result = handleEvent(event);

            if (result != null) {
                results.add(result);
            }
        }

        return results;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        private String sourceHeader = Constants.DEFAULT_HEADER_NAME;
        private String newHeader = Constants.DEFAULT_HEADER_NAME;

        private String timeTagHeader = Constants.TIME_TAG_HEADER_NAME;
        private String dateHeader = Constants.DATE_HEADER_NAME;

        private int validTimeTagLength = Constants.DEFAULT_VALID_TIME_TAG_LENGTH;

        private char dateSeparator = Constants.DATE_SEPARATOR.charAt(0);

        public Interceptor build() {
            logger.info(String.format("Creating DateTimeSuffixInterceptor: validTimeTagLength=%d,sourceHeader=%s,newHeader=%s,timeTagHeader=%s,dateHeader=%s,dateSeparator=%c",
                    validTimeTagLength, sourceHeader, newHeader, timeTagHeader, dateHeader, dateSeparator));

            return new DateTimeSuffixInterceptor(validTimeTagLength, sourceHeader, newHeader, timeTagHeader, dateHeader, dateSeparator);
        }

        public void configure(Context context) {
            validTimeTagLength = context.getInteger(Constants.VALID_TIME_TAG_LENGTH, Constants.DEFAULT_VALID_TIME_TAG_LENGTH);

            sourceHeader = context.getString(Constants.SOURCE_HEADER, Constants.DEFAULT_HEADER_NAME);
            newHeader = context.getString(Constants.NEW_HEADER, Constants.DEFAULT_HEADER_NAME);

            timeTagHeader = context.getString(Constants.TIME_TAG_HEADER, Constants.TIME_TAG_HEADER_NAME);
            dateHeader = context.getString(Constants.DATE_HEADER, Constants.DATE_HEADER_NAME);

            dateSeparator = context.getString(Constants.DATE_SEPARATOR_NAME, Constants.DATE_SEPARATOR).charAt(0);
        }
    }

    public static class Constants {
        public static final String VALID_TIME_TAG_LENGTH = "validTimeTagLength";
        public static final Integer DEFAULT_VALID_TIME_TAG_LENGTH = 11;


        public static final String SOURCE_HEADER = "sourceHeader";
        public static final String NEW_HEADER = "newHeader";

        public static final String DEFAULT_HEADER_NAME = "basename";

        public static final String DATE_HEADER = "dateHeader";
        public static final String DATE_HEADER_NAME = "date";
        public static final String DATE_SEPARATOR_NAME = "dateSeparator";
        public static final String DATE_SEPARATOR = "_";

        public static final String TIME_TAG_HEADER = "timeTagHeader";
        public static final String TIME_TAG_HEADER_NAME = "timetag";
    }
}
