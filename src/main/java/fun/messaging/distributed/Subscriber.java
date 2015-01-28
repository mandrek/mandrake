package fun.messaging.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.MessagePack;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public class Subscriber extends AbstractZMQPubSub implements  Runnable{
    private static final Logger logger = LogManager.getLogger(Subscriber.class);

    private Map<String,ZMQ.Socket> dealerCache = new HashMap<>();
    private final ZContext ctx = new ZContext();
    private final String hostname;
    private final byte[] identity;
    private final MessagePack msgpack = new MessagePack();
    CopyOnWriteArrayList<ZMQ.Socket> dealers = new CopyOnWriteArrayList<>();

    public Subscriber(String hostname) {
        this.hostname = hostname;
        identity = getIdentity(DEFAULT_PID
                , hostname);
    }

    void subscribe(String connectionString, byte[] expression, HandShake.HANDHAKE_TYPE mode) {
        final ZMQ.Socket socket = getDealer(connectionString);
        try {
            synchronized (socket) {
                socket.sendMore(identity);
                socket.send(msgpack.write(new HandShake(mode, mode.name(), expression)));
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    ZMQ.Socket getDealer(String connectionString) {
        ZMQ.Socket socket = dealerCache.get(connectionString);
        if(socket == null) {
            socket = SocketFactory.getInstance().getDealerSocket(ctx, connectionString, identity);
            dealerCache.put(connectionString, socket);
            dealers.add(socket);
        }
        return socket;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()) {
            for (ZMQ.Socket dealer : dealers) {
                synchronized (dealer) {
                    final byte[] topic = dealer.recv(ZMQ.DONTWAIT);
                    if(topic != null)  {
                        byte[] msg = null;
                        while((msg = dealer.recv(ZMQ.DONTWAIT)) != null);


                    }

                }
            }
        }
    }
}
