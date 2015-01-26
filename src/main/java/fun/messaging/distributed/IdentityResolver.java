package fun.messaging.distributed;

/**
 * Typically classes which implement this., involve lookups based on Trie
 * Created by kuldeep on 1/24/15.
 */
public interface IdentityResolver<T> extends ExpressionCache<T> {
    void invokeIdentitiesFor(byte[] topic, byte[] msg);

    void invokeIdentitiesFor(byte[] topic, ExpressionCache<byte[]> lastValueCache);
}
