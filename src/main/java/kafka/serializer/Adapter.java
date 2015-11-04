package kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public abstract class Adapter {
    public void close() {}
    public void configure(Map<String,?> configs, boolean isKey) {}
}
