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

import ly.bit.nsq.ConnectionUtils;
import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.exceptions.RequeueWithoutBackoff;
import ly.bit.nsq.sync.BatchReader;
import ly.bit.nsq.sync.SyncHandler;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 *
 */
public class NsqBatchRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private final String[] nsqAddresses;

    private final String nsqTopic;
    private final String nsqChannel;

    private final int workers;
    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private final int requeueDelay = 50;
    private final int maxRetries = 2;

    private volatile boolean closed = false;
    private volatile Thread[] thread;

    private ConcurrentLinkedQueue<Message> messages = new ConcurrentLinkedQueue<Message>();
    private ScheduledExecutorService timer;
    private AtomicLong bufferCount = new AtomicLong(0);

    @SuppressWarnings({"unchecked"})
    @Inject
    public NsqBatchRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("nsq")) {
            Map<String, Object> nsqSettings = (Map<String, Object>) settings.settings().get("nsq");

            if (nsqSettings.containsKey("addresses")) {
                List<String> addresses = new ArrayList<String>();
                for(Map<String, Object> address : (List<Map<String, Object>>) nsqSettings.get("addresses")) {
                    addresses.add( XContentMapValues.nodeStringValue(address.get("address"), "http://localhost:4161"));
                }
                nsqAddresses = addresses.toArray(new String[addresses.size()]);
            } else {
                String nsqHost = XContentMapValues.nodeStringValue(nsqSettings.get("address"), "http://localhost:4161");
                nsqAddresses = new String[]{ nsqHost };
            }

            nsqTopic = XContentMapValues.nodeStringValue(nsqSettings.get("topic"), "elasticsearch");
            nsqChannel = XContentMapValues.nodeStringValue(nsqSettings.get("channel"), "elasticsearch");
        } else {
            nsqAddresses = new String[]{ "http://localhost:4161" };

            nsqTopic = "elasticsearch";
            nsqChannel = "elasticsearch";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            workers = XContentMapValues.nodeIntegerValue(indexSettings.get("workers"), 1);
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            workers = 1;
            ordered = false;
        }
    }

    @Override
    public void start() {
        ThreadFactory factory = EsExecutors.daemonThreadFactory(settings.globalSettings(), "nsq_river");

        this.thread = new Thread[this.workers];
        for(int count = 0; count < this.thread.length; count++) {
            this.thread[count] = factory.newThread(new Consumer());
            this.thread[count].start();
        }

        logger.info("creating nsq river, addresses [{}] => [{}]", nsqAddresses, this.workers);

        this.timer = Executors.newSingleThreadScheduledExecutor();
        this.timer.scheduleAtFixedRate(new MessageBatchTimerTask(), 0, bulkTimeout.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing nsq river => [{}]", this.workers);
        closed = true;

        if (this.thread != null) {
            for (Thread aThread : this.thread) {
                aThread.interrupt();
            }

            this.thread = null;
        }

        if (this.timer != null) {
            this.timer.shutdownNow();
        }
        this.timer = null;
    }

    class MessageBatchTimerTask implements Runnable {

        public void requeueMessage(Message msg, boolean doDelay){
            if(msg.getAttempts() > maxRetries){
                this.finishMessage(msg);
                return;
            }else{
                int newDelay = doDelay ? 0 : requeueDelay * msg.getAttempts();
                try {
                    msg.getConn().send(ConnectionUtils.requeue(msg.getId(), newDelay));
                } catch (NSQException e) {
                    msg.getConn().close();
                }
            }
        }

        public void finishMessage(Message msg){
            try {
                msg.getConn().send(ConnectionUtils.finish(msg.getId()));
            } catch (NSQException e) {
                msg.getConn().close();
            }
        }

        protected void process(BulkRequestBuilder bulkRequestBuilder, final List<Message> messages_to_execute) {
            if (ordered) {
                try {
                    BulkResponse bulk_response = bulkRequestBuilder.execute().actionGet();
                    if (bulk_response.hasFailures()) {
                        logger.warn("failed to execute" + bulk_response.buildFailureMessage());
                    }

                    for(Message message : messages_to_execute) {
                        finishMessage(message);
                    }
                } catch (Exception e) {
                    logger.warn("failed to execute bulk", e);
                }
            } else {
                bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulk_response) {
                        if (bulk_response.hasFailures()) {
                            logger.warn("failed to execute" + bulk_response.buildFailureMessage());
                        }

                        for(Message message : messages_to_execute) {
                            finishMessage(message);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        if (e instanceof InvalidIndexNameException) {
                            logger.warn("failed to execute bulk, dropping message", e);

                            for(Message message : messages_to_execute) {
                                finishMessage(message);
                            }
                        } else {
                            logger.warn("failed to execute bulk, requeuing message", e);

                            for(Message message : messages_to_execute) {
                                requeueMessage(message, true);
                            }
                        }
                    }
                });
            }
        }

        public void run() {
            List<Message> worklist = new ArrayList<Message>();
            while(messages.peek() != null) {
                Message message = messages.poll();
                worklist.add(message);
            }

            if (!worklist.isEmpty()) {
                bufferCount.set(0);
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                for (Message message : worklist) {
                    try {
                        // parse via regex
                        message.getBody();

                        bulkRequestBuilder.add(message.getBody(), 0, message.getBody().length, false);
                    } catch (RequeueWithoutBackoff e) {
                        requeueMessage(message, false);
                    } catch (Exception e) {
                        // do nothing, success already false
                    }
                }
                process(bulkRequestBuilder, worklist);
            }
        }
    }

    private class Consumer implements Runnable, SyncHandler {

        private BatchReader batchReader;

        @Override
        public boolean handleMessage(Message msg) throws NSQException {
            messages.add(msg);

            long message_count = bufferCount.incrementAndGet();

            if (message_count >= bulkSize) {
                timer.submit(new MessageBatchTimerTask());
            }

            return true;
        }

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    break;
                }
                try {
                    batchReader = new BatchReader(nsqTopic, nsqChannel, this);

                    for(String nsqAddress : nsqAddresses) {
                        logger.info("performing lookupd [{}] => [{}] : [{}]", nsqAddress, nsqTopic, nsqChannel);
                        batchReader.addLookupd(nsqAddress);
                    }
                } catch (Exception e) {
                    if (!closed) {
                        logger.error("failed to created a connection / channel", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                // now use the queue to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }

                    try {
                        this.wait();
                    } catch(InterruptedException e) {
                        if (closed) {
                            break;
                        }
                    }
                }
            }
            cleanup(0, "closing river");
        }

        private void cleanup(int code, String message) {
            if (batchReader != null) {
                batchReader.shutdown();
                batchReader = null;
            }
        }
    }
}
