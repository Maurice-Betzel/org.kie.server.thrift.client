/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kie.server.thrift.client;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jboss.resteasy.util.ReadFromStream;
import org.kie.server.thrift.io.StreamReader;
import org.kie.server.thrift.protocol.KieServicesResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by x3.mbetzel on 12.06.2015.
 */
@Provider
@Consumes("application/x-thrift")
public class ThriftMessageReader implements MessageBodyReader<KieServicesResponse> {

    private static final Queue<TDeserializer> T_DESERIALIZER_QUEUE = new ConcurrentLinkedQueue<>();
    private static final TProtocolFactory T_PROTOCOL_FACTORY = new TCompactProtocol.Factory();

    public ThriftMessageReader() {
        System.out.println("ThriftMessageReader");
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return KieServicesResponse.class.isAssignableFrom(type);
    }

    @Override
    public KieServicesResponse readFrom(Class<KieServicesResponse> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        TDeserializer tDeserializer = null;
        KieServicesResponse KieServicesResponse = new KieServicesResponse();
        byte[] bytes = StreamReader.readFromStream(1024, entityStream);
        try {
            tDeserializer = pollTDeserializer();
            tDeserializer.deserialize(KieServicesResponse, bytes);
        } catch (TException e) {
            throw new IOException(e);
        } finally {
            if(tDeserializer != null) {
                T_DESERIALIZER_QUEUE.add(tDeserializer);
            }
        }
        return KieServicesResponse;
    }

    public static TDeserializer pollTDeserializer() {
        TDeserializer tDeserializer = T_DESERIALIZER_QUEUE.poll();
        if (tDeserializer == null) {
            tDeserializer = new TDeserializer(T_PROTOCOL_FACTORY);
        }
        return tDeserializer;
    }

    public static void addDeserializer(TDeserializer tDeserializer) {
        T_DESERIALIZER_QUEUE.add(tDeserializer);
    }
}