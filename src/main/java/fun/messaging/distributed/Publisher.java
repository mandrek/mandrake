package fun.messaging.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.MessagePack;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.*;
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
    private final IdentityResolver<ZMQ.Socket> identityResolver;
    private final ExpressionCache<byte[]> lastValueCache;


    public Publisher(List<String> brokerStrings, String routerString) {
        this(brokerStrings, routerString, new IdentityInvoker(), ExpressionCache.DEFAULT);
    }


    public Publisher(List<String> brokerStrings, String routerString, IdentityResolver<ZMQ.Socket> identityResolver, ExpressionCache<byte[]> lastValueCache) {
        this.lastValueCache = lastValueCache;
        this.identity = getIdentity(DEFAULT_PID, routerString);
        if (brokerStrings != null)
            for (String broker : brokerStrings)
                addBroker(broker);

        this.routerString = routerString;
        this.identityResolver = identityResolver;
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
                byte[] msg = router.recv(ZMQ.DONTWAIT);
                try {
                    if (msg == null)
                        continue;

                    HandShake hs = msgpack.read(msg, HandShake.class);
                    switch (hs.getHandShakeType()) {
                        case REGISTER:
                            logger.info("Client registered: {}", new String(clientIdentity));
                            break;
                        case SUBSCRIBE_AND_QUERY:
                            logger.info("New Client subrcibription: {}", hs.getHandShakeMsg());
                            ZMQ.Socket dealer = dealersCache.get(clientIdentity);
                            if (dealer == null) {
                                //init dealer
                            }
                            //publish initial value
                            break;
                    }

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

        List<String> brokers = null;
        if (args.length > 3) {
            brokers = new ArrayList<>();
            for (int i = 3; i < args.length; i++) {
                brokers.add(args[i]);
            }
        }
        Publisher publisher = new Publisher(brokers, hostname);
        byte[] msg = new byte[msgCount];
        Arrays.fill(msg, (byte) 9);
        //Wait for clients to startup
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        for (int i = 0; i < msgCount; i++) {
            publisher.publish("ABCDEFGH".getBytes(), msg);
        }

    }


}
