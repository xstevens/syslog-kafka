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

import java.io.Closeable;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import kafka.syslog.SyslogProto.SyslogKey;
import kafka.syslog.SyslogProto.SyslogMessage;

import org.graylog2.syslog4j.server.SyslogServerSessionlessEventHandlerIF;
import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;

public class KafkaEventHandler implements SyslogServerSessionlessEventHandlerIF, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaEventHandler.class);
    private static final long serialVersionUID = 1797629243068715681L;

    public KafkaEventHandler(final KafkaProducer<SyslogKey, SyslogMessage> producer, final EventAdapterFactory.EventAdapter adapter, final MetricRegistry metrics) {
	assert producer != null;
	assert adapter != null;
	LOG.debug("Creating event handler with adapter {}", adapter);
        this.producer = producer;
	this.adapter = adapter;
	this.metrics = metrics;
	receivedEvents = metrics.counter(MetricRegistry.name(KafkaEventHandler.class, "received-events"));
	sentEvents = metrics.counter(MetricRegistry.name(KafkaEventHandler.class, "sent-events"));
	handledExceptions = metrics.counter(MetricRegistry.name(KafkaEventHandler.class, "handled-exceptions"));
	eventHandling = metrics.timer(MetricRegistry.name(KafkaEventHandler.class, "event-handling"));
    }

    @Override
    public void destroy(final SyslogServerIF syslogServer) {
	LOG.debug("Destroy {} called", syslogServer);
    }

    @Override
    public void initialize(final SyslogServerIF syslogServer) {
	LOG.debug("Initialize {} called", syslogServer);
    }

    @Override
    public void close() {
	LOG.info("Closing event handler");

	if (!closed.getAndSet(true)) {
	    LOG.info("Closing producer");
	    producer.close();
	} else {
	    LOG.warn("Tried to close a second time, ignoring");
	}
    }

    @Override
    public void event(final SyslogServerIF server, final SocketAddress socketAddress, final SyslogServerEventIF event) {
	receivedEvents.inc();
	final Timer.Context context = eventHandling.time();
	try {
	    send(adapter.apply(event));
	} finally {
	    context.stop();
	}
    }

    private void send(final ProducerRecord<SyslogKey, SyslogMessage> record) {
	LOG.debug("Sending record {}", record);
	producer.send(record);
	sentEvents.inc();
    }

    @Override
    public void exception(final SyslogServerIF server, final SocketAddress socketAddress, final Exception exception) {
	LOG.error("Error reported", exception);
	handledExceptions.inc();
    }

    private final KafkaProducer<SyslogKey, SyslogMessage> producer;
    private final EventAdapterFactory.EventAdapter adapter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final MetricRegistry metrics;
    private final Counter receivedEvents;
    private final Counter sentEvents;
    private final Counter handledExceptions;
    private final Timer eventHandling;
}
