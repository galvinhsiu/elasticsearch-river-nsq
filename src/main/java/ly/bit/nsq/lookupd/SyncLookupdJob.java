package ly.bit.nsq.lookupd;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import ly.bit.nsq.NSQReaderImpl;
import ly.bit.nsq.exceptions.NSQException;

public class SyncLookupdJob implements Runnable {

    private static Logger LOGGER = Logger.getLogger(SyncLookupdJob.class.getName());

    private String identifier;
    private String address;
    private NSQReaderImpl reader;

    public SyncLookupdJob(String identifier, String address, NSQReaderImpl reader) {
        this.identifier = identifier;
        this.address = address;
        this.reader = reader;
    }

	public void run() {
        LOGGER.log(Level.FINER, "Running sync lookup job");

		Map<String, AbstractLookupd> lookupdConnections = reader.getLookupdConnections();
		AbstractLookupd lookupd = lookupdConnections.get(address);
		List<String> producers = lookupd.query(reader.getTopic());
		for(String producer : producers) {
			String[] components = producer.split(":");
			String nsqdAddress = components[0];
			int nsqdPort = Integer.parseInt(components[1]);
			try {
				reader.connectToNsqd(nsqdAddress, nsqdPort);
			} catch (NSQException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
			}
		}
	}
	
}
