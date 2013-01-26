package org.elasticsearch.river.nsq;

import ly.bit.nsq.Message;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.sync.SyncHandler;
import ly.bit.nsq.sync.SyncReader;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 *
 */
public class NsqRiver extends AbstractRiverComponent implements River {
    private final Client client;

    private final String[] nsqAddresses;

    private final String nsqTopic;
    private final String nsqChannel;
    private final boolean nsqRetry;

    private final int workers;

    private volatile boolean closed = false;

    private volatile Thread[] thread;

    @SuppressWarnings({"unchecked"})
    @Inject
    public NsqRiver(RiverName riverName, RiverSettings settings, Client client) {
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
            nsqRetry = XContentMapValues.nodeBooleanValue(nsqSettings.get("retry"), true);
        } else {
            nsqAddresses = new String[]{ "http://localhost:4161" };

            nsqTopic = "elasticsearch";
            nsqChannel = "elasticsearch";
            nsqRetry = true;
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            workers = XContentMapValues.nodeIntegerValue(indexSettings.get("workers"), 1);
        } else {
            workers = 1;
        }
    }

    @Override
    public void start() {
        logger.info("creating nsq river, addresses [{}]", (Object[]) nsqAddresses);

        ThreadFactory factory = EsExecutors.daemonThreadFactory(settings.globalSettings(), "nsq_river");

        this.thread = new Thread[this.workers];
        for(int count = 0; count < this.thread.length; count++) {
            this.thread[count] = factory.newThread(new Consumer());
            this.thread[count].start();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing nsq river");
        closed = true;

        if (this.thread != null) {
            for(int count = 0; count < this.thread.length; count++) {
                this.thread[count].interrupt();
            }

            this.thread = null;
        }
    }

    private class Consumer implements Runnable, SyncHandler {

        private SyncReader syncReader;

        public void start() {
        }

        public void stop(List<Message> messages) {
        }

        public boolean handleMessage(Message msg) throws NSQException {
            boolean return_value = true;

            //if (logger.isDebugEnabled()) {
                logger.info("Message received: " + new String(msg.getBody()));
            //}

            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            try{
                bulkRequestBuilder.add(msg.getBody(), 0, msg.getBody().length, false);

                if (logger.isTraceEnabled()) {
                    logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                }

                BulkResponse response = bulkRequestBuilder.execute().actionGet();
                if (response.hasFailures()) {
                    logger.warn("failed to execute" + response.buildFailureMessage());
                    return_value = false;
                }
            } catch(DocumentAlreadyExistsException dae) {
                logger.warn("document already exists, skipping", dae);
                return_value = true;
            } catch(Exception e) {
                logger.error("exception triggered [{}]", e);
                return_value = false;
            }

            if (!return_value && !nsqRetry) {
                return true;
            } else if (!return_value && nsqRetry) {
                return false;
            } else {
                return return_value;
            }
        }

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    break;
                }
                try {
                    syncReader = new SyncReader(nsqTopic, nsqChannel, this);

                    for(String nsqAddress : nsqAddresses) {
                        logger.info("performing lookupd [{}] => [{}] : [{}]", nsqAddress, nsqTopic, nsqChannel);
                        syncReader.addLookupd(nsqAddress);
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
            if (syncReader != null) {
                syncReader.shutdown();
                syncReader = null;
            }
        }
    }
}
