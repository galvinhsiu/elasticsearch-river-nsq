/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.nsq;

import ly.bit.nsq.Message;
import ly.bit.nsq.SyncConnection;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.MessageHandler;
import ly.bit.nsq.sync.SyncReader;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class NsqRiverTest implements MessageHandler {

    public NsqRiverTest() {
    }

    public void start() {
    }

    public void stop(List<Message> messages) {
    }

    public boolean handleMessage(Message msg) throws NSQException {
        return true;
    }

    public static void main(String[] args) throws Exception {

        Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();

        node.client().prepareIndex("_river", "test1", "_meta").setSource(jsonBuilder().startObject().field("type", "nsq").endObject()).execute().actionGet();

        SyncReader reader = new SyncReader("elasticsearch", "elasticsearch", null);
        reader.addLookupd("http://localhost:4161");

        String message = "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }\n" +
                "{ \"delete\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"2\" } }\n" +
                "{ \"create\" : { \"_index\" : \"test\", \"_type\" : \"type1\", \"_id\" : \"1\" }\n" +
                "{ \"type1\" : { \"field1\" : \"value1\" } }";


        Thread.sleep(1000);

        SyncConnection conn = new SyncConnection("localhost", 4150, reader);
        conn.connect();
//        conn.send(ConnectionUtils.pub("elasticsearch"), message.getBytes());
        conn.close();

        Thread.sleep(100000);

    }
}
