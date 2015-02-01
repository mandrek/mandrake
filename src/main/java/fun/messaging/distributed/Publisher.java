package fun.messaging.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.msgpack.MessagePack;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by kuldeep
 */
public class Publisher extends AbstractZMQPubSub implements Runnable {

    private static final Logger logger = LogManager.getLogger(Publisher.class);

    private static final ZContext context = new ZContext();
    private final List<ZMQ.Socket> dealers = new ArrayList<ZMQ.Socket>();
    private final ZMQ.Socket router;

    private final String routerString;
    private final Map<String, ZMQ.Socket> dealersCache = new HashMap<String, ZMQ.Socket>();
    private final byte[] identity;
    private final MessagePack msgpack = new MessagePack();
    private final IdentityResolver<byte[]> identityResolver;
    private final ExpressionCache<byte[]> lastValueCache;
    private final ConcurrentLinkedQueue<byte[]> msgQ = new ConcurrentLinkedQueue<byte[]>();
    private final ReentrantLock pubLock = new ReentrantLock();
    private final boolean isLastValueCache;

    public Publisher(List<String> brokerStrings, String routerString) {
        this(brokerStrings, routerString, null, ExpressionCache.DEFAULT, true);
    }


    public Publisher(List<String> brokerStrings, String routerString, IdentityResolver<byte[]> identityResolver, ExpressionCache<byte[]> lastValueCache, boolean isLastValueCache) {
        this.lastValueCache = lastValueCache;
        this.isLastValueCache = isLastValueCache;
        this.identity = getIdentity(DEFAULT_PID, routerString);
        if (brokerStrings != null)
            for (String broker : brokerStrings)
                addBroker(broker);

        this.routerString = routerString;

        router = SocketFactory.getInstance().getRouterSocket(context, routerString);
        if (identityResolver != null)
            this.identityResolver = identityResolver;
        else
            this.identityResolver = new IdentityInvoker(router);
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
        pubLock.lock();
        try {
            identityResolver.invokeIdentitiesFor(topic, msg);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        pubLock.unlock();

        if (isLastValueCache) {
            lastValueCache.put(topic, msg);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            if(!pubLock.tryLock()) {
                try {
                    Thread.sleep(1);
                    continue;
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }
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
                            identityResolver.put(hs.getExpression(), identity);
                            List<byte[]> intialValues = new ArrayList<>();
                            this.lastValueCache.matchKeyValue(hs.getExpression());
                            
                            /*
                            ZMQ.Socket dealer = dealersCache.get(clientIdentity);
                            if (dealer == null) {
                                //init dealer
                            }
                            //publish initial value
                            */
                            break;
                        case QUERY:
                            break;
                    }

                    /*
                    byte[] topic = msgQ.poll();

                    if(topic != null) {
                        while ((msg = msgQ.poll()) != null) ;
                        try {
                            identityResolver.invokeIdentitiesFor(topic, msg);
                        }catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                    } */

                } catch (Exception e) {
                    logger.warn(e);
                }
            }

            pubLock.unlock();
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
        /*
        for (int i = 0; i < msgCount; i++) {
            publisher.publish("ABCDEFGH".getBytes(), msg);
        } */
        
        

    }
}
