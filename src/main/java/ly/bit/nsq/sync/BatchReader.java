package ly.bit.nsq.sync;

import ly.bit.nsq.Message;
import ly.bit.nsq.MessageHandler;
import ly.bit.nsq.NSQReader;
import ly.bit.nsq.NSQReaderImpl;
import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupd;

public class BatchReader extends NSQReaderImpl implements NSQReader {

    private MessageHandler handler;

    public BatchReader(String topic, String channel, MessageHandler handler, int retryCount, int maxInFlight) {
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
