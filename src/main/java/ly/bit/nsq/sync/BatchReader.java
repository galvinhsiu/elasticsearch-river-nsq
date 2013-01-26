package ly.bit.nsq.sync;

import ly.bit.nsq.Message;
import ly.bit.nsq.NSQReader;
import ly.bit.nsq.NSQReaderImpl;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.exceptions.RequeueWithoutBackoff;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupd;
import org.elasticsearch.river.nsq.NsqBatchRiver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BatchReader extends NSQReaderImpl implements NSQReader {

    private ConcurrentLinkedQueue<Message> messages;
    private SyncHandler handler;

    public BatchReader(String topic, String channel, SyncHandler handler, int retryCount, int maxInFlight) {
        super();

        this.handler = handler;
        this.init(topic, channel, retryCount, maxInFlight);
    }

    public void addMessageForProcessing(Message msg) {
        try {
            this.handler.handleMessage(msg);
        } catch(NSQException nse) {
            requeueMessage(msg, true);
        }
    }

    @Override
    public AbstractLookupd makeLookupd(String addr) {
        return new SyncLookupd(addr);
    }
}
