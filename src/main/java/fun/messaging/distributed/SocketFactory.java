package fun.messaging.distributed;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * Created by kuldeep on 1/24/15.
 */
public class SocketFactory {

    private static final SocketFactory instance = new SocketFactory();

    private SocketFactory() {
    }

    static final SocketFactory getInstance() {
        return instance;
    }

    ZMQ.Socket getDealerSocket(ZContext ctx, String connectonString, byte[] identity) {
        ZMQ.Socket soc = ctx.createSocket(ZMQ.ROUTER);
        soc.setRouterMandatory(true); //Throws exceptions in case the subscriber is down and it can then unregister a socket
        //soc.setHWM();
        soc.setIdentity(identity);
        soc.connect(connectonString);
        return soc;
    }


    ZMQ.Socket getRouterSocket(ZContext ctx, String connectonString) {
        ZMQ.Socket soc = ctx.createSocket(ZMQ.ROUTER);
        soc.setRouterMandatory(true); //Throws exceptions in case the subscriber is down and it can then unregister a socket
        //soc.setHWM();
        soc.setLinger(-1);
        soc.bind(connectonString);
        return soc;
    }
}
