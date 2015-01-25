package fun.messaging.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.MessagePack;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by kuldeep
 */
public class Publisher implements Runnable {
    private static final String DEFAULT_PID = "" + System.nanoTime();
    private static final Logger logger = LogManager.getLogger(Publisher.class);

    private static final ZContext context = new ZContext();
    private final List<ZMQ.Socket> dealers = new ArrayList<ZMQ.Socket>();
    private final ZMQ.Socket router;
    private static final AtomicLong identityCounter = new AtomicLong();
    private final String routerString;
    private final Map<String, ZMQ.Socket> dealersCache = new HashMap<String, ZMQ.Socket>();
    private final byte[] identity;
    private final MessagePack msgpack = new MessagePack();

    public Publisher(List<String> brokerStrings, String routerString) {
        this.identity = getIdentity(DEFAULT_PID, routerString);
        if (brokerStrings != null)
            for (String broker : brokerStrings)
                addBroker(broker);

        this.routerString = routerString;
        router = SocketFactory.getInstance().getRouterSocket(context, routerString);
    }


    static final byte[] getIdentity(String pid, String hostname) {
        return (pid + "." + hostname + "." + identityCounter.incrementAndGet()).getBytes();
    }


    public void addBroker(String broker) {
        synchronized (dealers) {
            try {
                if (!dealersCache.containsKey(broker)) {
                    ZMQ.Socket dealer = SocketFactory.getInstance().getDealerSocket(context, broker, identity);
                    dealers.add(dealer);
                    dealersCache.put(broker, dealer);
                    initBroker(dealer);

                }
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    public void removeBroker(String brokerString) {
        synchronized (dealers) {
            ZMQ.Socket dealer = dealersCache.get(brokerString);
            if (dealer != null)
                dealersCache.remove(brokerString);
            dealers.remove(dealer);
            try {
                dealer.close();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    private void initBroker(ZMQ.Socket dealer) {

        try {
            dealer.sendMore(identity);
            dealer.send(msgpack.write(new HandShake(HandShake.HANDHAKE_TYPE.REGISTER, "Connection request from publisher ")));
        } catch (IOException e) {
            dealer.send(e.toString().getBytes());
            logger.error(e.getMessage(), e);
        }
    }


    void publish(byte[] topic, byte[] msg) {

    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] clientIdentity = router.recv();
            if (clientIdentity != null) {
                try {
                    //router.recv()

                } catch (Exception e) {
                    logger.warn(e);
                }
            }

        }
        try {
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
            logger.warn(e);
        }
        router.close();
    }

    public static void main(String[] args) {
        String hostname = args[0];
        int msgSize = Integer.parseInt(args[1]);
        int msgCount = Integer.parseInt(args[2]);

        if (args.length > 3) {
            for (int i = 3; i < args.length; i++) {

            }
        }
    }


}
