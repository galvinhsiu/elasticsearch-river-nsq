package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	
	public byte[] unpackResponse() throws NSQException{
        try{
            byte[] data_size = new byte[4];
            this.inputStream.read(data_size);

            ByteBuffer buffer = ByteBuffer.wrap(data_size);
            buffer.order(ByteOrder.BIG_ENDIAN);
			long data_length = buffer.getInt();

            byte[] return_value = new byte[ (int) data_length];
            this.inputStream.read(return_value);
            return  return_value;
		} catch(IOException e){
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
				while(!closed.get()){
					byte[] response = null;
					try {
						response = unpackResponse();
					} catch (NSQException e) {
                        LOGGER.log(Level.SEVERE, e.getMessage(), e);
						// Assume this meant that we couldn't read somehow, should close the connection
						close();
						break;
					}

                    if (response != null) {
                        try {
                            handleResponse(response);
                        } catch (NSQException e) {
                            LOGGER.log(Level.WARNING, e.getMessage(), e);

                            close();
                            break;
                        }
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
		if(prev){
			return;
		}
		LOGGER.log(Level.INFO, "Closing connection " + this.toString());
		try {
			this.sock.close();
		} catch (IOException e) {
            LOGGER.log(Level.FINE, e.getMessage(), e);
		}
		this.reader.getConnections().remove(this.toString());
	}

}
