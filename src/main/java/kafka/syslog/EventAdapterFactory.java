/*
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

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.syslog.SyslogProto.SyslogKey;
import kafka.syslog.SyslogProto.SyslogMessage;
import kafka.syslog.SyslogProto.Facility;
import kafka.syslog.SyslogProto.Severity;

public class EventAdapterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EventAdapterFactory.class);

    public static final String DEFAULT_TOPIC = "syslog-kafka";
    public static final String KAFKA_PUBLISH_TOPIC = "kafka.publish.topic";
    public static final String KAFKA_KEY = "syslog.kafka.key";
    public static final String KAFKA_DYNAMIC_KEY = "syslog.kafka.key.dynamic";

    public EventAdapterFactory() {
	this(DEFAULT_TOPIC);
    }

    public EventAdapterFactory(final Properties properties) {
	this.topic = properties.getProperty(KAFKA_PUBLISH_TOPIC, DEFAULT_TOPIC);
    }

    public EventAdapterFactory(final String topic) {
	LOG.debug("Created converter factory for topic {}", topic);
	this.topic = topic;
    }

    static long getTimestamp(final SyslogServerEventIF event) {
	return event.getDate() != null ? event.getDate().getTime() : System.currentTimeMillis();
    }

    static SyslogMessage messageFromEvent(final SyslogServerEventIF event) {
	final SyslogMessage message = SyslogMessage.newBuilder()
	    .setTimestamp(getTimestamp(event))
	    .setFacility(Facility.valueOf(event.getFacility()))
	    .setSeverity(Severity.valueOf(event.getLevel()))
	    .setHost(event.getHost())
	    .setMsg(event.getMessage())
	    .build();
	LOG.debug("Generated message {}", message);
	return message;
    }

    public EventAdapter createFixedKeyAdapter(final String fixedValue) {
	LOG.info("Creating maker with key {}", fixedValue);
	final SyslogKey key = SyslogKey.newBuilder().setName(fixedValue).build();
	return (final SyslogServerEventIF event) -> { return new ProducerRecord<SyslogKey, SyslogMessage>(topic, key, messageFromEvent(event)); };
    }

    public EventAdapter createUnkeyedAdapter() {
	LOG.info("Creating unkeyed maker");
	return (final SyslogServerEventIF event) -> { return new ProducerRecord<SyslogKey, SyslogMessage>(topic, messageFromEvent(event)); };
    }

    public EventAdapter createDynamicAdapter() {
	LOG.info("Creating dynamic maker");
	return (final SyslogServerEventIF event) -> {
	    final Severity severity = Severity.valueOf(event.getLevel());
	    final Facility facility = Facility.valueOf(event.getFacility());
	    final String host = event.getHost();
	    final SyslogKey.Builder key = SyslogKey.newBuilder();
	    key.getDynamicBuilder()
		.setHost(host)
		.setFacility(facility)
		.setSeverity(severity);
	    final SyslogMessage message = SyslogMessage.newBuilder()
		.setTimestamp(getTimestamp(event))
		.setFacility(facility)
		.setSeverity(severity)
		.setHost(host)
		.setMsg(event.getMessage())
		.build();
	    return new ProducerRecord<SyslogKey, SyslogMessage>(topic, key.build(), message);
	};
    }

    public EventAdapter createAdapter(final Properties properties) {
	if (properties.getProperty(KAFKA_DYNAMIC_KEY) == null) {
	    final String kafkaKey = properties.getProperty(KAFKA_KEY);
	    return (kafkaKey == null) ? createUnkeyedAdapter() : createFixedKeyAdapter(kafkaKey);
	} else {
	    return createDynamicAdapter();
	}
    }

    public static EventAdapter newAdapter(final Properties properties) {
	return new EventAdapterFactory(properties.getProperty(KAFKA_PUBLISH_TOPIC, DEFAULT_TOPIC)).createAdapter(properties);
    }

    private final String topic;
}
