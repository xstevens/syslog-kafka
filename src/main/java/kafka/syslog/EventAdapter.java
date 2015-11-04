package kafka.syslog;

import java.util.function.Function;

import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.syslog.SyslogProto.SyslogKey;
import kafka.syslog.SyslogProto.SyslogMessage;

@FunctionalInterface
public interface EventAdapter extends Function<SyslogServerEventIF, ProducerRecord<SyslogKey, SyslogMessage>> {
}
