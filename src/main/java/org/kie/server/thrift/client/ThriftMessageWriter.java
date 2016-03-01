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

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.kie.server.thrift.protocol.KieServicesRequest;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by x3.mbetzel on 12.06.2015.
 */
@Provider
@Produces("application/x-thrift")
public class ThriftMessageWriter implements MessageBodyWriter<KieServicesRequest> {

    private static final Queue<TSerializer> T_SERIALIZER_QUEUE = new ConcurrentLinkedQueue<>();
    private static final TProtocolFactory T_PROTOCOL_FACTORY = new TCompactProtocol.Factory();

    public ThriftMessageWriter() {
        System.out.println("ThriftMessageWriter");
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return KieServicesRequest.class.isAssignableFrom(type);
    }

    @Override
    public long getSize(KieServicesRequest kieServicesRequest, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(KieServicesRequest kieServicesRequest, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        TSerializer tSerializer = null;
        try {
            tSerializer = pollTSerializer();
            entityStream.write(tSerializer.serialize(kieServicesRequest));
        } catch (TException e) {
            throw new IOException(e);
        } finally {
            if(tSerializer != null) {
                T_SERIALIZER_QUEUE.add(tSerializer);
            }
        }
    }

    public static TSerializer pollTSerializer() {
        TSerializer tSerializer = T_SERIALIZER_QUEUE.poll();
        if (tSerializer == null) {
            tSerializer = new TSerializer(T_PROTOCOL_FACTORY);
        }
        return tSerializer;
    }

    public static void addSerializer(TSerializer tSerializer) {
        T_SERIALIZER_QUEUE.add(tSerializer);
    }

}