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

import org.apache.thrift.TBaseHelper;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.kie.server.thrift.java.BigDecimal;
import org.kie.server.thrift.java.Date;
import org.kie.server.thrift.protocol.*;
import org.kie.server.thrift.client.facts.Element;
import org.kie.server.thrift.client.facts.EmbeddedMessage;
import org.kie.server.thrift.client.facts.Global;
import org.kie.server.thrift.client.facts.Message;

import javax.ws.rs.client.Entity;
import java.nio.ByteBuffer;

import static org.kie.server.thrift.protocol.protocolConstants.APPLICATION_XTHRIFT;
import static org.kie.server.thrift.client.facts.protocolConstants.HELLO;

/**
 * Created by x3.mbetzel on 11.01.2016.
 */
public class RSClient {

    public static void main(String[] args) throws Exception {

        ResteasyClientBuilder resteasyClientBuilder = new ResteasyClientBuilder();
        ResteasyClient resteasyClient = resteasyClientBuilder.build();
        JaxRsAuthenticator jaxRsAuthenticator = new JaxRsAuthenticator("ks-user", "ks-user");
        KieServicesClient kieServicesClient = new KieServicesClient();
        kieServicesClient.setClient("Test RSClient");
        KieServicesRequest kieServicesRequest;
        KieServicesResponse response;

        // ServerInfoTest

        ResteasyWebTarget webTarget = resteasyClient.target("http://localhost:8080").path("/kie-server/services/rest/server/thrift");
        webTarget.register(ThriftMessageReader.class);
        webTarget.register(ThriftMessageWriter.class);
        webTarget.register(jaxRsAuthenticator);
        kieServicesRequest = new KieServicesRequest();
        kieServicesRequest.setKieServicesClient(kieServicesClient);
        response = webTarget.request().accept(APPLICATION_XTHRIFT).header("Content-Type", APPLICATION_XTHRIFT).get(KieServicesResponse.class);
        System.out.println(response);

        // ListContainersTest

        webTarget = resteasyClient.target("http://localhost:8080").path("/kie-server/services/rest/server/thrift/containers");
        webTarget.register(ThriftMessageReader.class);
        webTarget.register(ThriftMessageWriter.class);
        webTarget.register(jaxRsAuthenticator);
        response = webTarget.request().accept(APPLICATION_XTHRIFT).header("Content-Type", APPLICATION_XTHRIFT).get(KieServicesResponse.class);
        System.out.println(response);

        // ContainerInfoTest

        webTarget = resteasyClient.target("http://localhost:8080").path("/kie-server/services/rest/server/thrift/containers/test");
        webTarget.register(ThriftMessageReader.class);
        webTarget.register(ThriftMessageWriter.class);
        webTarget.register(jaxRsAuthenticator);
        response = webTarget.request().accept(APPLICATION_XTHRIFT).header("Content-Type", APPLICATION_XTHRIFT).get(KieServicesResponse.class);
        System.out.println(response);

        // ContainerExecuteTest

        webTarget = resteasyClient.target("http://localhost:8080").path("/kie-server/services/rest/server/thrift/containers/instances/test/testStatelessKieSession");
        webTarget.register(ThriftMessageReader.class);
        webTarget.register(ThriftMessageWriter.class);
        webTarget.register(jaxRsAuthenticator);
        kieServicesRequest = new KieServicesRequest();
        kieServicesRequest.setKieServicesClient(kieServicesClient);

        TSerializer tSerializer = ThriftMessageWriter.pollTSerializer();
        TDeserializer tDeserializer = ThriftMessageReader.pollTDeserializer();

        SetGlobalCommand setGlobalCommand = new SetGlobalCommand();
        Global global = new Global();
        setGlobalCommand.setObject(tSerializer.serialize(global));
        setGlobalCommand.setClassCanonicalName(Global.class.getCanonicalName());
        setGlobalCommand.setIdentifier("globalList");
        setGlobalCommand.setOutIdentifier("GlobalList");

        SetGlobalCommand setGlobalCommand2 = new SetGlobalCommand();
        BigDecimal bigDecimal = new BigDecimal("45645.5678483633");
        setGlobalCommand2.setObject(tSerializer.serialize(bigDecimal));
        setGlobalCommand2.setClassCanonicalName(BigDecimal.class.getCanonicalName());
        setGlobalCommand2.setIdentifier("bigDecimal");
        setGlobalCommand2.setOutIdentifier("BigDecimalGlobal");

        SetGlobalCommand setGlobalCommand3 = new SetGlobalCommand();
        Date date = new Date(4325643464356L);
        setGlobalCommand3.setObject(tSerializer.serialize(date));
        setGlobalCommand3.setClassCanonicalName(Date.class.getCanonicalName());
        setGlobalCommand3.setIdentifier("executionDate");
        setGlobalCommand3.setOutIdentifier("ExecutionDate");

        InsertObjectCommand insertObjectCommand = new InsertObjectCommand();
        BigDecimal bigDecimal2 = new BigDecimal();
        bigDecimal2.setValue("1000");
        insertObjectCommand.setObject(tSerializer.serialize(bigDecimal2));
        insertObjectCommand.setClassCanonicalName(BigDecimal.class.getCanonicalName());
        insertObjectCommand.setOutIdentifier("BigDecimal2");

        InsertObjectCommand insertObjectCommand2 = new InsertObjectCommand();
        Message message = new Message();
        message.setMessageString("WORLD");
        message.setStatus(HELLO);
        EmbeddedMessage embeddedMessage = new EmbeddedMessage();
        embeddedMessage.setEmbeddedMessageString("Embedded message string");
        embeddedMessage.setBigdecimal(new BigDecimal("1234.5678"));
        message.setEmbeddedMessage(embeddedMessage);
        insertObjectCommand2.setObject(tSerializer.serialize(message));
        insertObjectCommand2.setClassCanonicalName(Message.class.getCanonicalName());
        insertObjectCommand2.setOutIdentifier("Message");

        InsertElementsCommand insertElementsCommand = new InsertElementsCommand();
        Element element = new Element("Element1");
        Element element1 = new Element("Element2");
        Element element2 = new Element("Element3");
        insertElementsCommand.setClassCanonicalName(Element.class.getCanonicalName());
        insertElementsCommand.addToObjects(ByteBuffer.wrap(tSerializer.serialize(element)));
        insertElementsCommand.addToObjects(ByteBuffer.wrap(tSerializer.serialize(element1)));
        insertElementsCommand.addToObjects(ByteBuffer.wrap(tSerializer.serialize(element2)));
        insertElementsCommand.setOutIdentifier("Elements");

        FireAllRulesCommand fireAllRulesCommand = new FireAllRulesCommand();
        AgendaFilter agendaFilter = new AgendaFilter();
        agendaFilter.setAgendaFilterType(AgendaFilterType.RULE_NAME_STARTS_WITH);
        agendaFilter.setExpression("Elem");
        //fireAllRulesCommand.setAgendaFilter(agendaFilter);

        BatchExecutionCommand batchExecutionCommand = new BatchExecutionCommand();
        batchExecutionCommand.addToSetGlobalCommands(setGlobalCommand);
        batchExecutionCommand.addToSetGlobalCommands(setGlobalCommand2);
        batchExecutionCommand.addToSetGlobalCommands(setGlobalCommand3);
        batchExecutionCommand.addToInsertObjectCommands(insertObjectCommand);
        batchExecutionCommand.addToInsertObjectCommands(insertObjectCommand2);
        batchExecutionCommand.addToInsertElementsCommands(insertElementsCommand);
        batchExecutionCommand.setFireAllRulesCommand(fireAllRulesCommand);

        kieServicesRequest.setBatchExecutionCommand(batchExecutionCommand);

        Entity<KieServicesRequest> entity = Entity.entity(kieServicesRequest, APPLICATION_XTHRIFT);
        response = webTarget.request(APPLICATION_XTHRIFT).post(entity, KieServicesResponse.class);
        System.out.println(response);
        if (response.getResponse().isSetKieServicesException()) {
            KieServicesException kieServicesException = response.getResponse().getKieServicesException();
            System.out.println(kieServicesException);
        } else {
            ExecutionResults executionResults = response.getResponse().getResults().getExecutionResults();
            System.out.println(executionResults);
            Message messageResult = new Message();
            ThriftMessageReader.pollTDeserializer().deserialize(messageResult, TBaseHelper.byteBufferToByteArray(executionResults.getObjects().get("Message")));
            System.out.println(messageResult);
            Global global1 = new Global();
            ThriftMessageReader.pollTDeserializer().deserialize(global1, TBaseHelper.byteBufferToByteArray(executionResults.getObjects().get("GlobalList")));
            System.out.println(global1);
            BigDecimal bigDecimalResult = new BigDecimal();
            ThriftMessageReader.pollTDeserializer().deserialize(bigDecimalResult, TBaseHelper.byteBufferToByteArray(executionResults.getObjects().get("BigDecimalGlobal")));
            System.out.println(bigDecimalResult);
            Date dateResult = new Date();
            ThriftMessageReader.pollTDeserializer().deserialize(dateResult, TBaseHelper.byteBufferToByteArray(executionResults.getObjects().get("ExecutionDate")));
            System.out.println(dateResult);
            System.out.println(new java.util.Date(dateResult.getValue()));
            Collection collection = new Collection();
            ThriftMessageReader.pollTDeserializer().deserialize(collection, TBaseHelper.byteBufferToByteArray(executionResults.getObjects().get("Elements")));
            for (ByteBuffer byteBuffer : collection.getObjects()) {
                Element elementResult = new Element();
                tDeserializer.deserialize(elementResult, TBaseHelper.byteBufferToByteArray(byteBuffer));
                System.out.println(elementResult);
            }
        }
    }

}
