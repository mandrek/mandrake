import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kuldeep on 1/24/15.
 */
public class PubSubBroker implements Runnable {

    static final AtomicInteger ctr = new AtomicInteger();
    static final int totalWorkers = 50;
    static final AtomicInteger recvd = new AtomicInteger();

    public static void main(String[] args) {
        byte[] large = new byte[5000000];
        Arrays.fill(large, (byte) 1);

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        /*
        for(int i=0; i < 5; i++) {
            Thread t = new Thread(new PubSubBroker());

            t.start();
//            try {
//                Thread.sleep(300);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

        }*//*
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
*/
        int totalWorksers = 15;
        ZContext ctx = new ZContext(1);
        ZMQ.Socket socket = ctx.createSocket(ZMQ.ROUTER);
        socket.setRouterMandatory(true);
        socket.setHWM(1);
        socket.setLinger(-1);
        socket.bind("tcp://127.0.0.1:6660");

        for (int i = 0; i < totalWorksers; i++) {
            Thread t = new Thread(new PubSubBroker());

            t.start();


        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int counter = 0;

        long t1 = 0;
        long totalBytes = 0;
        while (true) {
            byte[] identity = socket.recv();
            if (identity != null) {
                //System.out.println("Received identity: " + new String(identity));


                byte[] msg = socket.recv();
                if (counter == 0)
                    t1 = System.currentTimeMillis();

                // System.out.println("Received on Server: " + new String(msg));
               /* System.out.println(socket.sendMore(identity));
                System.out.println(socket.sendMore(msg));
                System.out.println(socket.send(large));*/
                System.out.println(socket.sendMore(identity));
                System.out.println(socket.sendMore(msg));
                System.out.println(socket.send(large));
                System.out.println("Send backlog: " + socket.getBacklog());
                counter++;
                totalBytes += ((identity.length + msg.length + large.length) / (1000 * 1000));
                if (counter == totalWorksers) {
                    long t2 = System.currentTimeMillis();
                    socket.close();

                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Published " + totalBytes + "(MB) in: " + (t2 - t1));
                    break;
                }


            }
        }
        /*
        byte[] identity = null;
        String msg = new String(identity) + ":" + msgCounter;
        socket.send(msg.getBytes());*/

    }

    static final AtomicInteger msgCounter = new AtomicInteger();
    static final AtomicLong t1 = new AtomicLong();

    @Override
    public void run() {
        ZContext ctx = new ZContext(1);
        ZMQ.Socket socket = ctx.createSocket(ZMQ.DEALER);

        byte[] identity = ("" + ctr.incrementAndGet()).getBytes();
        socket.setIdentity(identity);
        socket.setBacklog(100000);
        socket.setHWM(1);
        socket.connect("tcp://127.0.0.1:6660");

        //System.out.println("Starting socket: "+ new String(identity));

        String msg = new String(identity) + ":" + "HELLO";
        System.out.println(socket.send(msg.getBytes()));
        /*socket.close();
        socket = ctx.createSocket(ZMQ.DEALER);
        socket.setIdentity(identity);
        socket.connect("tcp://127.0.0.1:6660");
*/
        while (true) {

            int event = socket.getEvents();
            //System.out.println("REcv backlog: "+socket.getBacklog());
            byte[] msg1 = socket.recv(ZMQ.DONTWAIT);
            if (msg1 != null) {
                byte[] msg2 = socket.recv();
                System.out.println("Received on Client " + new String(identity) + "-> " + ":" + new String(msg1) + ":" + msg2.length);
                //System.out.println("Events: " + event);
                int total = recvd.addAndGet(msg2.length / (1000 * 1000));
                int ctr = msgCounter.incrementAndGet();
                if (ctr == 1)
                    t1.compareAndSet(0, System.currentTimeMillis());

                System.out.println("Recvd " + total + "(MB)in: " + t1.get());

                if (ctr == totalWorkers) {
                    t1.compareAndSet(t1.get(), System.currentTimeMillis() - t1.get());
                    System.out.println("Recvd " + total + "(MB)in: " + t1.get());
                }

            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
