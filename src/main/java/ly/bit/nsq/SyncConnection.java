package ly.bit.nsq;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import ly.bit.nsq.exceptions.NSQException;

public class SyncConnection extends Connection {

    private static final Logger LOGGER = Logger.getLogger(SyncConnection.class.getName());

	private Socket sock;
	private InputStream inputStream;
	
	public SyncConnection(String host, int port, NSQReader reader){
		this.host = host;
		this.reader = reader;
		this.port = port;
		this.sock = new Socket();
		this.init();
	}

	@Override
	public synchronized void send(String command, byte[]... datas) throws NSQException {
		try {
			OutputStream os = this.sock.getOutputStream();
			os.write(command.getBytes());

            DataOutputStream ds = new DataOutputStream(os);
            for(byte[] data : datas){
                ds.writeLong(data.length);
                ds.write(data);
            }
		} catch (IOException e) {
			throw new NSQException(e);
		}
	}
	
	public byte[] readN(int size) throws IOException{
		// Going with a super naive impl first...
		byte[] data = new byte[size];
		this.inputStream.read(data);
		return data;
	}
	
	public byte[] readResponse() throws NSQException{
		try{
			DataInputStream ds = new DataInputStream(this.inputStream);
			int size = ds.readInt();
			byte[] data = this.readN(size);
			return data;
		}catch(IOException e){
			throw new NSQException(e);
		}
	}

	@Override
	public void connect() throws NSQException {
		try{
			this.sock.connect(new InetSocketAddress(host, port));
			this.send(ConnectionUtils.MAGIC_V2);
			this.inputStream = new BufferedInputStream(this.sock.getInputStream());
		}catch(IOException e){
			throw new NSQException(e);
		}
	}

	@Override
	public void readForever() {
		class ReadThis implements Runnable {
			public void run() {
				while(closed.get() != true){
					byte[] response;
					try {
						response = readResponse();
					} catch (NSQException e) {
						// Assume this meant that we couldn't read somehow, should close the connection
						close();
						break;
					}
					try {
						handleResponse(response);
					} catch (NSQException e) {
                        LOGGER.log(Level.WARNING, e.getMessage(), e);
					}
				}
			}
		}
		Thread t = new Thread(new ReadThis(), this.toString());
		t.start();
	}

	@Override
	public void close() {
		boolean prev = this.closed.getAndSet(true);
		if(prev == true){
			return;
		}
		LOGGER.log(Level.FINE, "Closing connection " + this.toString());
		try {
			this.sock.close();
		} catch (IOException e) {
            LOGGER.log(Level.FINE, e.getMessage(), e);
		}
		this.reader.getConnections().remove(this.toString());
	}

}
