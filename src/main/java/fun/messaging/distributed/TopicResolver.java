package fun.messaging.distributed;

import java.util.List;

/**
 * Created by kuldeep on 1/24/15.
 */
public interface TopicResolver<T> {
    void registerCallback(byte[] expression, T callback);

    List<T> getCallbacks(byte[] topic);

    void invokeCallbacks(byte[] topic);

}
