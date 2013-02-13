package ly.bit.nsq.sync;

import ly.bit.nsq.Message;
import ly.bit.nsq.MessageHandler;
import ly.bit.nsq.NSQReader;
import ly.bit.nsq.NSQReaderImpl;
import ly.bit.nsq.exceptions.RequeueWithoutBackoff;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupd;

import java.util.concurrent.Executors;

public class SyncReader extends NSQReaderImpl implements NSQReader {
	
	private MessageHandler handler;
	
	public SyncReader(String topic, String channel, MessageHandler handler) {
		super();

		this.handler = handler;
        this.executor = Executors.newSingleThreadExecutor();

        this.init(topic, channel, 2, 1);
	}

	private class SyncMessageRunnable implements Runnable {
		
		public SyncMessageRunnable(Message msg) {
			super();
			this.msg = msg;
		}

		private Message msg;

		public void run() {
			boolean success = false;
			boolean doDelay = true;
			try{
				success = handler.handleMessage(msg);
			}catch(RequeueWithoutBackoff e){
				doDelay = false;
			}catch(Exception e){
				// do nothing, success already false
			}
			
			// tell conn about success or failure
			if(success){
				finishMessage(msg);
			}else{
				requeueMessage(msg, doDelay);
			}
		}
	}

    public void addMessageForProcessing(Message msg){
        this.executor.execute(new SyncMessageRunnable(msg));
    }

	@Override
	public AbstractLookupd makeLookupd(String addr) {
		return new SyncLookupd(addr);
	}
}
