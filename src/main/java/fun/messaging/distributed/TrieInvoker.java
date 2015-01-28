package fun.messaging.distributed;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public interface TrieInvoker<T> {
    void invoke(byte[] key, T trieValue, Object invokeValue);
}
