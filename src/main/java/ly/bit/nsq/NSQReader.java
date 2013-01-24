package ly.bit.nsq;


import java.util.concurrent.ConcurrentHashMap;

public interface NSQReader {
    ConcurrentHashMap<String, Connection> getConnections();

    void addMessageForProcessing(Message msg);
}
