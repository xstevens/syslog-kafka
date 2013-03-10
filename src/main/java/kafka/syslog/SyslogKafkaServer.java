/*
 * Copyright 2013 Xavier Stevens
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.syslog;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.syslog.SyslogProto.SyslogMessage;

import org.apache.log4j.Logger;
import org.productivity.java.syslog4j.SyslogRuntimeException;
import org.productivity.java.syslog4j.server.SyslogServer;
import org.productivity.java.syslog4j.server.SyslogServerConfigIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;

public class SyslogKafkaServer {

    private static final Logger LOG = Logger.getLogger(SyslogKafkaServer.class);

    private static final String KAFKA_PROPERTIES_RESOURCE_NAME = "/kafka.producer.properties";

    protected static Properties getDefaultKafkaProperties() throws IOException {
        final Properties props = new Properties();
        final URL propUrl = SyslogKafkaServer.class.getResource(KAFKA_PROPERTIES_RESOURCE_NAME);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + KAFKA_PROPERTIES_RESOURCE_NAME);
        }

        final InputStream in = propUrl.openStream();
        try {
            props.load(in);
        } finally {
            in.close();
        }

        return props;
    }

    public static void main(String[] args) throws SyslogRuntimeException, IOException {
        final SyslogServerIF syslogServer = SyslogServer.getThreadedInstance("udp");

        SyslogServerConfigIF syslogServerConfig = syslogServer.getConfig();
        // syslogServerConfig.setCharSet("UTF-8");
        syslogServerConfig.setHost("192.168.11.6");
        syslogServerConfig.setPort(5140);

        final Properties kafkaProperties = getDefaultKafkaProperties();
        final ProducerConfig config = new ProducerConfig(kafkaProperties);
        final Producer<String, SyslogMessage> producer = new Producer<String, SyslogMessage>(config);

        // Add producer and syslog server shutdown hooks
        Runtime.getRuntime().addShutdownHook(new Thread() {
            /*
             * (non-Javadoc)
             * 
             * @see java.lang.Thread#run()
             */
            public void run() {
                if (producer != null) {
                    LOG.info("Closing producer...");
                    producer.close();
                }
                SyslogServer.shutdown();
            }
        });

        KafkaEventHandler kafkaEventHandler = new KafkaEventHandler(producer);
        syslogServerConfig.addEventHandler(kafkaEventHandler);

        try {
            syslogServer.getThread().join();
        } catch (InterruptedException e) {
            LOG.error("Main thread interrupted...");
        }
    }
}
