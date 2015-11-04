/*
 * Copyright 2013 Xavier Stevens
 * Copyright 2015 Christopher Smith
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.graylog2.syslog4j.SyslogRuntimeException;
import org.graylog2.syslog4j.server.SyslogServer;
import org.graylog2.syslog4j.server.SyslogServerConfigIF;
import org.graylog2.syslog4j.server.SyslogServerIF;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Slf4jReporter;

import kafka.syslog.SyslogProto.SyslogKey;
import kafka.syslog.SyslogProto.SyslogMessage;

public class SyslogKafkaServer implements Runnable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SyslogKafkaServer.class);

    private static final String DEFAULT_KAFKA_PROPERTIES_RESOURCE_PATH = "/kafka.producer.properties";
    public static final String KAFKA_PROPERTIES_PATH = "kafka.properties";
    public static final String SYSLOG_INTERFACE = "syslog.interface";
    public static final String SYSLOG_INTERFACE_DEFAULT = "udp";
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_HOST = "server.host";
    public static final String DEFAULT_SERVER_PORT = "514";
    public static final String METRICS_LOGGER = "metrics.logger";

    public SyslogKafkaServer() {
	final Properties appProperties = System.getProperties();
	syslogServer = getServer(appProperties);

        final Properties kafkaProperties = getDefaultKafkaProperties();
	LOG.info("Kafka properties {}", kafkaProperties);
        kafkaEventHandler = new KafkaEventHandler(kafkaProperties, EventAdapterFactory.newAdapter(appProperties));
        syslogServer.getConfig().addEventHandler(kafkaEventHandler);
    }
    
    public void run() {
	Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
	syslogServer.run();
    }

    protected static Properties getDefaultKafkaProperties() throws RuntimeException {
        final Properties props = new Properties();
	final String propertiesPath = System.getProperties().getProperty(KAFKA_PROPERTIES_PATH, DEFAULT_KAFKA_PROPERTIES_RESOURCE_PATH);
	LOG.info("Opening kafka properties file {}", propertiesPath);
        final URL propUrl = SyslogKafkaServer.class.getResource(propertiesPath);
        if (propUrl == null) {
            throw new IllegalArgumentException("Could not find the properties file: " + propertiesPath);
        }

	try (final InputStream in = propUrl.openStream()) {
	    props.load(in);
	} catch (final IOException e) {
	    LOG.error("Error while loading kafka properties", e);
	    throw new RuntimeException("Error while loading kafka properties", e);
	}
 
        return props;
    }

    public static SyslogServerIF getServer(final Properties appProperties) {
	final String syslogIF = appProperties.getProperty(SYSLOG_INTERFACE, SYSLOG_INTERFACE_DEFAULT);
	LOG.info("Binding to interface {}", syslogIF);
	if (SyslogServer.exists(syslogIF)) {
	    LOG.debug("{} is a supported protocol", syslogIF);
	} else {
	    LOG.error("{} is not a supported protocol", syslogIF);
	    System.exit(1);
	}

        SyslogServerIF syslogServer = SyslogServer.getInstance(syslogIF);
        final SyslogServerConfigIF syslogServerConfig = syslogServer.getConfig();
	final String serverPort = appProperties.getProperty(SERVER_PORT, DEFAULT_SERVER_PORT);
	LOG.info("Binding to server port {}", serverPort);
	syslogServerConfig.setPort(Short.valueOf(serverPort));
	final String serverHost = appProperties.getProperty(SERVER_HOST);
	LOG.info("Binding to server host {}", serverHost);
	if (serverHost != null) {
	    syslogServerConfig.setHost(serverHost);
	}

	return syslogServer;
    }

    @Override
    public void close() {
	LOG.info("Shutting down syslog server...");
	SyslogServer.shutdown();
	LOG.info("Closing producer...");
	kafkaEventHandler.close();
    }

    private void addMetrics() {
	final MetricRegistry metrics = kafkaEventHandler.getMetrics();

	if (System.getProperty(METRICS_LOGGER) != null) {
	    final Slf4jReporter slfReporter = Slf4jReporter.forRegistry(metrics)
		.outputTo(LOG)
		.convertRatesTo(TimeUnit.SECONDS)
		.convertDurationsTo(TimeUnit.MILLISECONDS)
		.build();
	    slfReporter.start(1, TimeUnit.MINUTES);
	}

	final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();
	jmxReporter.start();
    }
    
    public static void main(final String[] args) throws SyslogRuntimeException, IOException {
	try (final SyslogKafkaServer server = new SyslogKafkaServer()) {
	    server.run();
	    LOG.info("Server completed... exiting");
        } catch (final RuntimeException e) {
            LOG.error("Problem running server", e);
	    System.exit(2);
        }
    }

    private final SyslogServerIF syslogServer;
    private final KafkaEventHandler kafkaEventHandler;
}
