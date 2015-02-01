package fun.messaging.distributed;

import org.zeromq.ZMQ;

/**
 * Created by Kuldeep on 1/27/2015.
 */
public class RouterTrieInvoker implements TrieInvoker<byte[] > {
    final ZMQ.Socket router;

    RouterTrieInvoker(ZMQ.Socket router) {
        this.router = router;
    }

    @Override
    public void invoke(byte[] key, byte[] trieValue, Object invokeValue) {
        //This method is called for every Identity
        this.router.sendMore(trieValue);
    }
}
