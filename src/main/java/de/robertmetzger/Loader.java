package de.robertmetzger;

import org.apache.flink.api.java.utils.ParameterTool;
import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.GelfMessageBuilder;
import org.graylog2.gelfclient.GelfMessageLevel;
import org.graylog2.gelfclient.GelfTransports;
import org.graylog2.gelfclient.transport.GelfTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class Loader {
    private static final Logger LOG = LoggerFactory.getLogger(Loader.class);
    public static void main( String[] args ) throws InterruptedException, IOException, ParseException {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        final String logIdentifier = pt.get("logIdentifier", null);

        LOG.info("Welcome to the data loader {}", pt.toMap());
        final GelfConfiguration config = new GelfConfiguration(new InetSocketAddress(pt.getRequired("host"), pt.getInt("port", 11201)))
                .transport(GelfTransports.TCP)
                .queueSize(512)
                .connectTimeout(5000)
                .reconnectDelay(1000)
                .tcpNoDelay(true)
                .sendBufferSize(32768);

        final GelfTransport transport = GelfTransports.create(config);

        String container = null;
        String host = null;
        Pattern containerStart = Pattern.compile("Container: ([a-zA-Z0-9_]+) on ([0-9a-z.-]+)(_[0-9]+)?");
                                           // 2016-12-08 23:00:53,473 INFO  org.apache.flink.yarn.YarnTaskManagerRunner                   -     -Xmx1448m
        Pattern logEntry = Pattern.compile(pt.get("logPattern", "([0-9]{4}-[0-9\\-\\ \\:]+\\,[0-9]*) ([A-Z]+)[ \\t]*([a-zA-Z0-9\\.]+)[ \\t]*\\-[ \\t]*([^\\n]*)"));
        SimpleDateFormat sdf  = new SimpleDateFormat(pt.get("datePattern","yyyy-MM-dd HH:mm:ss,SSS"));

        try (BufferedReader br = new BufferedReader(new FileReader(pt.getRequired("file")))) {
            String line;
            String message = null;
            String date  = null;
            String level = null;
            String clazz = null;
            // this reader supports multiline message parsing. it sends a log message once it found
            // the next message (so basically the log entry start is the delimiter)
            while ((line = br.readLine()) != null) {
                // search for next container:
                if(line.startsWith("Container: ")) {
                    // we found a new log:
                    Matcher matches = containerStart.matcher(line);
                    if(!matches.find()) {
                        throw new IllegalStateException("Unexpected format");
                    }
                    container = matches.group(1);
                    host = matches.group(2);
                    LOG.info("Found new container {} on host {}", container, host);
                } else {
                    // try to match a log line
                    Matcher logMatcher = logEntry.matcher(line);
                    if(logMatcher.find()) {
                        // we matched a line. Log current line
                        if(level != null) {
                            LOG.info("Parsed log {} {} {} '{}'", date, level, clazz, message);
                            GelfMessageBuilder builder = new GelfMessageBuilder(message, host);
                            builder.level(getLoglevel(level));
                            builder.timestamp(sdf.parse(date).getTime());
                            builder.additionalField("log_source", "yarn-app-log-to-gelf");
                            builder.additionalField("yarn_container", container);
                            if(logIdentifier != null) {
                                builder.additionalField("log_identifier", logIdentifier);
                            }
                            transport.send(builder.build());
                        } else {
                            LOG.info("Incomplete log {} {} {} '{}'", date, level, clazz, message);
                        }

                        // start new log entry
                        date = logMatcher.group(1);
                        level = logMatcher.group(2);
                        clazz = logMatcher.group(3);
                        message = logMatcher.group(4);
                    } else {
                        // append current line
                        message += "\n" + line;
                    }
                }
            }
        }

        Thread.sleep(5000);
    }

    private static GelfMessageLevel getLoglevel(String level) {
        switch(level) {
            case "WARN":
            case "WARNING":
                return GelfMessageLevel.WARNING;
            default:
                return GelfMessageLevel.valueOf(level);
        }
    }
}
