package fun.messaging.distributed;

import org.zeromq.ZContext;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public abstract class AbstractZMQPubSub {
    static final String DEFAULT_PID = "" + System.nanoTime();
    private static final AtomicLong identityCounter = new AtomicLong();

    static final byte[] getIdentity(String pid, String hostname) {
        return (pid + "." + hostname + "." + identityCounter.incrementAndGet()).getBytes();
    }
}
