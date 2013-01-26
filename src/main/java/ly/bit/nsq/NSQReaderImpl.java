package ly.bit.nsq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.AbstractLookupd;
import ly.bit.nsq.lookupd.SyncLookupdJob;


public abstract class NSQReaderImpl implements NSQReader {

    private static final Logger LOGGER = Logger.getLogger(NSQReaderImpl.class.getName());

	protected int requeueDelay;
	protected int maxRetries;
	protected int maxInFlight;
	
	protected String topic;
	protected String channel;
	protected String shortHostname;
	protected String hostname;
	
	protected ExecutorService executor;
	
	protected ConcurrentHashMap<String, Connection> connections;
	protected ConcurrentHashMap<String, AbstractLookupd> lookupdConnections;
	
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(5);

	public static final ConcurrentHashMap<String, NSQReaderImpl> readerIndex = new ConcurrentHashMap<String, NSQReaderImpl>();
	
	public void init(String topic, String channel, int retryCount, int maxInFlight){
		this.requeueDelay = 50;
		this.maxRetries = retryCount;
		this.maxInFlight = maxInFlight;
		this.connections = new ConcurrentHashMap<String, Connection>();
		this.topic = topic;
		this.channel = channel;
		try {
			this.hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			this.hostname = "unknown.host";
		}
		String[] hostParts = this.hostname.split("\\.");
		this.shortHostname = hostParts[0];
		
		this.lookupdConnections = new ConcurrentHashMap<String, AbstractLookupd>();

		// register action for shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				shutdown();
			}
		});
		readerIndex.put(this.toString(), this);
	}
	
	public void shutdown(){
		LOGGER.log(Level.INFO, "Received signal to shut down");

	    try {
            for(Connection conn: this.connections.values()){
                conn.close();
            }
        } finally {
            this.scheduler.shutdown();
        }
	}

	public void requeueMessage(Message msg, boolean doDelay){
		if(msg.getAttempts() > this.maxRetries){
			// TODO log giving up
			this.finishMessage(msg);
			return;
		}else{
			int newDelay = doDelay ? 0 : this.requeueDelay * msg.getAttempts();
			try {
				msg.getConn().send(ConnectionUtils.requeue(msg.getId(), newDelay));
			} catch (NSQException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
				msg.getConn().close();
			}
		}
	}

	public void finishMessage(Message msg){
		try {
			msg.getConn().send(ConnectionUtils.finish(msg.getId()));
		} catch (NSQException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
			msg.getConn().close();
		}
	}
	
	public void connectToNsqd(String address, int port) throws NSQException{
		Connection conn = new SyncConnection(address, port, this);
		String connId = conn.toString();
		Connection stored = this.connections.putIfAbsent(connId, conn);
		if(stored != null){
			return;
		}
		conn.connect();
		for(Connection cxn : this.connections.values()){
			cxn.maxInFlight = (int) Math.ceil(this.maxInFlight / (float)this.connections.size());
		}
		conn.send(ConnectionUtils.subscribe(this.topic, this.channel, this.shortHostname, this.hostname));
		conn.send(ConnectionUtils.ready(conn.maxInFlight));
		conn.readForever();
	}
	
	
	// lookupd stuff
	
	public ScheduledExecutorService getLookupdScheduler() {
		return this.scheduler;
	}
	
	public abstract AbstractLookupd makeLookupd(String addr);
	
	public void addLookupd(String addr) {
		AbstractLookupd lookupd = this.makeLookupd(addr);
		AbstractLookupd stored = this.lookupdConnections.putIfAbsent(addr, lookupd);
		if(stored != null){
			return;
		}

        SyncLookupdJob job = new SyncLookupdJob("lookupd-jobs-" + addr, addr, this);
        this.getLookupdScheduler().execute(job);
        this.getLookupdScheduler().scheduleAtFixedRate(job, 0, 40, TimeUnit.SECONDS);
	}
	
	// -----
	
	public String toString(){
		return "Reader<" + this.topic + ", " + this.channel + ">";
	}

	public String getTopic() {
		return topic;
	}

	public ConcurrentHashMap<String, AbstractLookupd> getLookupdConnections() {
		return lookupdConnections;
	}

    public ConcurrentHashMap<String, Connection> getConnections() {
        return connections;
    }
}
