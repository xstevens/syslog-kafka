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
package kafka.serializer;

import kafka.message.Message;
import kafka.syslog.SyslogProto;
import kafka.syslog.SyslogProto.SyslogMessage;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class SyslogMessageDecoder implements Decoder<SyslogMessage> {

    private static final Logger LOG = Logger.getLogger(SyslogMessageDecoder.class);

    @Override
    public SyslogMessage toEvent(Message msg) {
        SyslogMessage smsg = null;
        try {
            smsg = SyslogProto.SyslogMessage.parseFrom(ByteString.copyFrom(msg.payload()));
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Received unparseable message", e);
        }

        return smsg;
    }

}
