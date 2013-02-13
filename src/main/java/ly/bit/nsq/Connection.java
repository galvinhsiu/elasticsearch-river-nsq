package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author dan
 *         <p/>
 *         This class (which we may want to make abstract later or something) should manage
 *         the connection to an instance of nsqd. It should have methods to send commands to nsqd,
 *         which I guess will get into the Netty stuff that you guys were talking about, as well as
 *         I guess some sort of callback function on data received - is that how Netty works?
 *         <p/>
 *         Anyway, I'm going to stub out what I think it should do when it receives a new message.
 *         We can all revisit as we flesh more stuff out.
 */
public abstract class Connection {

    private static Logger LOGGER = Logger.getLogger(Connection.class.getName());

    protected NSQReader reader;
    protected String host;
    protected int port;
    protected AtomicInteger readyCount;
    protected int maxInFlight; // TODO maybe replace this with something from reader, or else just set it from there
    protected AtomicBoolean closed = new AtomicBoolean(false);

    protected void init() {
        this.readyCount = new AtomicInteger(); // will init to 0, but that is fine on startup
    }

    public void messageReceivedCallback(Message message) {
        int curReady = this.readyCount.decrementAndGet();
        if (curReady < Math.max(2, 0.25 * (float) this.maxInFlight)) {
            // should send ready now
            try {
                this.send(ConnectionUtils.ready(maxInFlight));
            } catch (NSQException e) {
                // broken conn
                this.close();

                LOGGER.log(Level.WARNING, e.getMessage(), e);
                return;
            }
            this.readyCount.set(maxInFlight);
        }
        this.reader.addMessageForProcessing(message);
    }

    public abstract void send(String command, byte[]... data) throws NSQException;

    public abstract void connect() throws NSQException;

    public abstract void readForever() throws NSQException;

    public abstract void close();

    public Message decodeMessage(byte[] data) throws NSQException {
        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));
        try {
            long timestamp = ds.readLong(); // 8 bytes
            short attempts = ds.readShort(); // 2 bytes
            byte[] id = new byte[16];
            ds.read(id);
            byte[] body = new byte[data.length - 26];
            ds.read(body);

            return new Message(id, body, timestamp, attempts, this);
        } catch (IOException e) {
            throw new NSQException(e);
        }
    }

    public void handleResponse(byte[] response) throws NSQException {
        if (response.length <= 4) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.wrap(response, 0, 4);
        int frame_id = buffer.getInt();
        FrameType ft = FrameType.fromInt(frame_id);
        switch (ft) {
            case FRAMETYPERESPONSE:
                if (new String(response).equals("_heartbeat_")) {
                    this.send(ConnectionUtils.nop());
                }

                break;
            case FRAMETYPEMESSAGE:
                byte[] messageBytes = Arrays.copyOfRange(response, 4, response.length);
                Message msg = this.decodeMessage(messageBytes);
                this.messageReceivedCallback(msg);
                break;
            case FRAMETYPEERROR:
                String errMsg = new String(Arrays.copyOfRange(response, 4, response.length));
                throw new NSQException(errMsg);
        }

    }

    public String toString() {
        return this.host + ":" + this.port;
    }
}
