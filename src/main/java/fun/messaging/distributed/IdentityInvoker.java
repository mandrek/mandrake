package fun.messaging.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kuldeep on 1/25/15.
 */
public class IdentityInvoker implements IdentityResolver<byte[]> {
    private Map<ByteArrayKey, byte[]> map = new HashMap<>();
    private final ByteArrayKey bkey = new ByteArrayKey(new byte[]{});
    final ZMQ.Socket router;
    private static final Logger logger = LogManager.getLogger(IdentityResolver.class);

    public IdentityInvoker(ZMQ.Socket router) {
        this.router = router;
    }

    @Override
    public void put(byte[] topic, byte[] msg) {
        map.put(new ByteArrayKey(topic), msg);
    }


    @Override
    public byte[] get(byte[] topic) {
        bkey.setKey(topic);
        return map.get(bkey);
    }

    @Override
    public void remove(byte[] topic) {
        bkey.setKey(topic);
        map.remove(bkey);
    }

    @Override
    public List<byte[]> match(byte[] expression) {
        List<byte[]> sockets = new ArrayList<>();
        byte[] socket = map.get(expression);
        if (socket != null)
            sockets.add(socket);
        return sockets;
    }

    @Override
    public List<byte[][]> matchKeyValue(byte[] expression) {
        return null;
    }

    @Override
    public void matchAndCollect(byte[] expression, List<byte[]> msgs) {
        msgs.addAll(match(expression));
    }

    @Override
    public void invokeIdentitiesFor(byte[] topic, byte[] msg) {
        List<byte[]> indentities = match(topic);
        if (indentities != null) {
            for (byte[] identity : indentities) {
                try {
                    router.sendMore(identity);
                    router.send(msg);
                } catch (Exception e) {
                    logger.warn(e.getMessage(), e);
                }
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
