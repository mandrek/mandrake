package fun.messaging.distributed;

import java.util.List;

/**
 * Typically classes which implement this., involve lookups based on Trie
 * Created by kuldeep on 1/24/15.
 */
public interface IdentityResolver {

    void putIdentity(byte[] identity);

    /**
     * For a given topic get a
     *
     * @param topic
     * @return
     */
    List<byte[]> getIdentity(byte[] topic);

    /**
     * Ideally just pass in the topic and the datastructure should be automatically resolve the
     *
     * @param topic
     */
    void invokeIdentitiesFor(byte[] topic);
}
