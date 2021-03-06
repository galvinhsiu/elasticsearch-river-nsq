package ly.bit.nsq.util;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class ConnectionUtils {
    private static final Logger LOGGER = Logger.getLogger(ConnectionUtils.class.getName());

	private static final char NL = '\n';
	public static final String MAGIC_V2 = "  V2";

	protected static String command(String cmd, String... params){
		StringBuilder commandBuilder = new StringBuilder(cmd);
		for(String param : params){
			commandBuilder.append(" ");
			commandBuilder.append(param);
		}
		commandBuilder.append(NL);
		String out = commandBuilder.toString();

        LOGGER.log(Level.FINE, out);
		return out;
	}

    public static String pub(String topic){
        return command("PUB", topic);
    }

    public static String subscribe(String topic, String channel, String shortId, String longId){
		// TODO: check validity of topc and channel
		return command("SUB", topic, channel, shortId, longId);
	}
	
	public static String ready(int count){
		return command("RDY", Integer.toString(count));
	}

	public static String finish(byte[] id){
		return command("FIN", new String(id));
	}
	
	public static String requeue(byte[] id, int timeMs){
		return command("REQ", new String(id), Integer.toString(timeMs));
	}
	
	public static String nop(){
		return command("NOP");
	}
}
