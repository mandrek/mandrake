package fun.messaging.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kuldeep on 1/24/15.
 */
public interface ExpressionCache<T> {

    void put(byte[] topic, T msg);

    T get(byte[] topic);

    void remove(byte[] topic);

    List<T> match(byte[] expression);

    List matchKeyValue(byte[] expression);

    void matchAndCollect(byte[] expression, List<T> msgs);


    ExpressionCache<byte[]> DEFAULT = new ExpressionCache<byte[]>() {
        private Map<ByteArrayKey, byte[]> map = new HashMap<>();
        private final ByteArrayKey bkey = new ByteArrayKey(new byte[]{});

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
            List<byte[]> msgs = new ArrayList<>();
            byte[] msg = map.get(expression);
            if (msg != null)
                msgs.add(msg);
            return msgs;
        }

        @Override
        public List matchKeyValue(byte[] expression) {
            List<byte[]> msgs = match(expression);
            List<byte[][]> pairs = new ArrayList<>();
            for (int i = 0; i < msgs.size(); i++) {
                byte[][] pair = new byte[2][];
                pair[0] = msgs.get(i);//Topic
                pair[1] = msgs.get(i);//Message
            }
            return pairs;

        }

        @Override
        public void matchAndCollect(byte[] expression, List<byte[]> msgs) {
            msgs.addAll(match(expression));
        }
    };

}
