package fun.messaging.distributed;

import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kuldeep on 1/25/15.
 */
public class IdentityInvoker implements IdentityResolver<ZMQ.Socket> {
    private Map<ByteArrayKey, ZMQ.Socket> map = new HashMap<>();
    private final ByteArrayKey bkey = new ByteArrayKey(new byte[]{});

    @Override
    public void put(byte[] topic, ZMQ.Socket msg) {
        map.put(new ByteArrayKey(topic), msg);
    }


    @Override
    public ZMQ.Socket get(byte[] topic) {
        bkey.setKey(topic);
        return map.get(bkey);
    }

    @Override
    public void remove(byte[] topic) {
        bkey.setKey(topic);
        map.remove(bkey);
    }

    @Override
    public List<ZMQ.Socket> match(byte[] expression) {
        List<ZMQ.Socket> msgs = new ArrayList<>();
        ZMQ.Socket msg = map.get(expression);
        if (msg != null)
            msgs.add(msg);
        return msgs;
    }

    @Override
    public List<byte[][]> matchKeyValue(byte[] expression) {
        return null;
    }

    @Override
    public void matchAndCollect(byte[] expression, List<ZMQ.Socket> msgs) {
        msgs.addAll(match(expression));
    }

    @Override
    public void invokeIdentitiesFor(byte[] topic, byte[] msg) {
        List<ZMQ.Socket> sockets = match(topic);
        if (sockets != null) {
            for (ZMQ.Socket socket : sockets) {
                socket.sendMore(topic);
                socket.send(msg);
            }
        }

    }

    @Override
    public void invokeIdentitiesFor(byte[] topic, ExpressionCache<byte[]> lastValueCache) {
        List pairs = lastValueCache.matchKeyValue(topic);
        for (Object pair : pairs) {
            byte[][] kvp = (byte[][]) pair;
            invokeIdentitiesFor(kvp[0], kvp[1]);
        }


    }

}
