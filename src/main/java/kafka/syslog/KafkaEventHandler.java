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

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.syslog.SyslogProto.SyslogMessage;
import kafka.syslog.SyslogProto.SyslogMessage.Severity;

import org.productivity.java.syslog4j.server.SyslogServerEventHandlerIF;
import org.productivity.java.syslog4j.server.SyslogServerEventIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;

public class KafkaEventHandler implements SyslogServerEventHandlerIF {

    private static final long serialVersionUID = 1797629243068715681L;

    private Producer<String, SyslogMessage> producer;

    public KafkaEventHandler(Producer<String, SyslogMessage> producer) {
        this.producer = producer;
    }

    @Override
    public void event(SyslogServerIF server, SyslogServerEventIF event) {
        SyslogMessage.Builder smb = SyslogMessage.newBuilder();
        // if there wasn't a timestamp in the event for any reason use now
        long ts = event.getDate() != null ? event.getDate().getTime() : System.currentTimeMillis();
        smb.setTimestamp(ts);
        smb.setFacility(event.getFacility());
        smb.setSeverity(Severity.valueOf(event.getLevel()));
        smb.setHost(event.getHost());
        smb.setMsg(event.getMessage());
        send(smb.build());
    }

    private void send(SyslogMessage message) {
        ProducerData<String, SyslogMessage> pd = new ProducerData<String, SyslogMessage>("syslog-kafka", message);
        producer.send(pd);
    }
}
