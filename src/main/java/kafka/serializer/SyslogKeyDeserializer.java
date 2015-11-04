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
package kafka.serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

import kafka.syslog.SyslogProto.SyslogKey;

public class SyslogKeyDeserializer extends Adapter implements Deserializer<SyslogKey> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufSerializer.class);

    @Override
    public SyslogKey deserialize(final String topic, byte[] data) {
	try {
	    return SyslogKey.parseFrom(data);
	} catch (final InvalidProtocolBufferException e) {
	    LOG.error("Received unparseable message", e);
	    throw new RuntimeException("Received unparseable message " + e.getMessage(), e);
	}
    }

}
