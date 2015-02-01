package fun.messaging.distributed;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


/**
 * Kuldeep Marathe
 */
public class MonteCarloPiCalculator {
    static final ZContext context = new ZContext();
    final int terminateCount = 100;
    final int workers = 5;
    final long until = System.currentTimeMillis() + 90000;
    final long taskPollTimeOut = 1000;//10 seconds
    final int batchSize = 5000;
    final int retryLimit = 3;
    final int hwm = 10;
    final AtomicLong processed = new AtomicLong();
    final Runnable piSubmitter = new Runnable() {

        final ZMQ.Socket socket = context.createSocket(ZMQ.PUSH);
        final ZMQ.Socket collector = context.createSocket(ZMQ.PULL);

        @Override
        public void run() {
            //socket.setHWM(hwm);
            //socket.setSendBufferSize(10);
            socket.bind("tcp://127.0.0.1:5000");
            collector.bind("tcp://127.0.0.1:5001");

            final Map<Long, TaskStatus> taskMap = new HashMap<>(); //<taskId,SentTimeStamp
            int totalCount = 0;
            int received = 0;
            long t1 = System.currentTimeMillis();
            long tasks = 0;
            List<Long> retryExceededList = new ArrayList<>();

            while (!(System.currentTimeMillis() > until || (taskMap.size() == 0 &&
                    (received + (retryExceededList.size() * batchSize)) >= (terminateCount * batchSize)))) {
                //Total timeout for the run

                if (tasks < terminateCount && taskMap.size() < 10) {
                    socket.sendMore("" + (++tasks));
                    final boolean isSent = socket.send("" + batchSize, ZMQ.DONTWAIT);
                    if (!isSent)
                        System.out.println("Socket is full+ " + tasks);
                    else {
                        taskMap.put(tasks, new TaskStatus(null));
                    }
                }
                String taskStr = collector.recvStr(ZMQ.DONTWAIT);
                if (taskStr != null) {
                    //int event = collector.getEvents();
                    Long taskId = Long.parseLong(taskStr);
                    String worker = collector.recvStr();
                    String count = collector.recvStr();
                    try {
                        if (taskMap.containsKey(taskId)) {
                            System.out.println("Received from [Worker|taskId|count]:  [" + worker + "|" + taskId +
                                    "|" + count + "]");
                            totalCount += Integer.parseInt(count);
                            received += batchSize;
                            taskMap.remove(taskId);
                        } else {
                            System.out.println("Discarding from [Worker|taskId|count]:  [" + worker + "|" +
                                    taskId + "|" + count + "]");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                long t2 = System.currentTimeMillis();
                if ((t2 - t1) >= taskPollTimeOut) {
                    Iterator<Map.Entry<Long, TaskStatus>> it = taskMap.entrySet().iterator();
                    while (it.hasNext()) {
                        //Resend timedOut worker. Can block Indefinitely
                        Map.Entry<Long, TaskStatus> task = it.next();
                        if ((t2 - task.getValue().lastUpdated) > taskPollTimeOut) {
                            if (task.getValue().retryCount > retryLimit) {
                                System.out.println("Retries exceeded for [taskId|lastUpdate]: " + task.getKey() +
                                        "|" + task.getValue().lastUpdated + "]");
                                retryExceededList.add(task.getKey());
                                it.remove();
                                continue;
                            }

                            if (taskMap.size() < hwm) {
                                socket.sendMore("" + task.getKey().intValue());
                                socket.send("" + batchSize);
                                task.getValue().lastUpdated = System.currentTimeMillis();
                                task.getValue().retryCount++;
                            }
                        }
                    }
                    t1 = t2;
                }

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            double pi = 4 * ((double) totalCount / ((double) (terminateCount * batchSize)));
            System.out.println("Value of pi is: " + pi);
            System.out.println("Incomplete tasks: taskId ->[lastUpdated|retryCount]  ");
            System.out.println("Received vs configured  " + received + " vs " + (terminateCount * batchSize) + " vs " +
                    (tasks * batchSize) + " vs " + processed.addAndGet(0));
            System.out.println("Retry Exceeded Count: " + (retryExceededList.size() * batchSize));
            taskMap.forEach((taskId, taskStatus) -> {
                System.out.println(taskId + "->" + taskStatus);
            });
            socket.close();
            collector.close();
            context.close();
            //context.destroy();

            System.exit(1);
        }
    };
    final Function<Integer, Runnable[]> rogueGenerator = new Function<Integer, Runnable[]>() {


        @Override
        public Runnable[] apply(Integer count) {
            Runnable[] rogues = new Runnable[count];
            for (int i = 0; i < count; i++) {
                final int wId = i;
                rogues[i] = new Runnable() {

                    final ZMQ.Socket socket = context.createSocket(ZMQ.PULL);
                    int workerId = wId;

                    @Override
                    public void run() {
                        socket.connect("tcp://127.0.0.1:5000");
                        while (true) {
                            try {
                                if (socket.getEvents() == -1)
                                    break;

                                String taskId = new String(socket.recv());
                                if (taskId == null)
                                    continue;
                                int batchSize = -1;
                                if (socket.hasReceiveMore())
                                    batchSize = Integer.parseInt(socket.recvStr());
                                System.out.println("Rogue Worker " + wId + " working on [taskId|batchSize]: [" +
                                        taskId + "|" + batchSize + "]");
                                Thread.sleep(9 * taskPollTimeOut);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                };

            }
            return rogues;
        }
    };
    final Function<Integer, Runnable[]> obedientGenerator = new Function<Integer, Runnable[]>() {


        @Override
        public Runnable[] apply(Integer count) {
            Runnable[] obedients = new Runnable[count];

            for (int i = 0; i < count; i++) {
                final int wid = i;
                obedients[i] = new Runnable() {
                    final ZMQ.Socket socket = context.createSocket(ZMQ.PULL);
                    final ZMQ.Socket collector = context.createSocket(ZMQ.PUSH);
                    final SecureRandom randX = new SecureRandom();
                    final SecureRandom randY = new SecureRandom();
                    final SecureRandom signX = new SecureRandom();
                    final SecureRandom signY = new SecureRandom();

                    @Override
                    public void run() {
                        try {
                            Thread.sleep(new SecureRandom().nextInt(1));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        //socket.setHWM(hwm);
                        //collector.setHWM(hwm);
                        socket.connect("tcp://127.0.0.1:5000");
                        collector.connect("tcp://127.0.0.1:5001");
                        while (true) {
                            try {
                                if (socket.getEvents() == -1)
                                    break;

                                String taskId = new String(socket.recv());
                                if (taskId == null)
                                    continue;
                                int batchSize = -1;
                                if (socket.hasReceiveMore())
                                    batchSize = Integer.parseInt(socket.recvStr());
                                int inCircleCounter = 0;

                                for (int i = 0; i < batchSize; i++) {
                                    double x = randX.nextDouble() * (signX.nextInt() % 2 == 0 ? -1.D : 1.D);
                                    double y = randY.nextDouble() * (signY.nextInt() % 2 == 0 ? -1.D : 1.D);
                                    inCircleCounter += ((x * x) + (y * y)) <= 1.D ? 1 : 0;
                                }
                                processed.addAndGet(batchSize);
                                collector.sendMore(taskId);
                                collector.sendMore("Obedient WOrker " + wid);
                                collector.send("" + inCircleCounter);

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                };

            }
            return obedients;
        }
    };
    final Runnable[] rogueWorkers = rogueGenerator.apply(workers);
    final Runnable[] obedientWorkers = obedientGenerator.apply(workers);

    static final void startThreads(Runnable[] runnables) {
        for (Runnable r : runnables) {
            Thread t = new Thread(r);
            t.start();
        }
    }

    public static void main(String[] args) {
        MonteCarloPiCalculator piCalc = new MonteCarloPiCalculator();
        Thread publisher = new Thread(piCalc.piSubmitter);
        publisher.start();
        startThreads(piCalc.obedientWorkers);
        //startThreads(piCalc.rogueWorkers);


        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class TaskStatus<T> {
        final T task;
        long lastUpdated;
        int retryCount;

        TaskStatus(T task) {
            this.task = task;
            lastUpdated = System.currentTimeMillis();
            retryCount = 0;
        }

        @Override
        public String toString() {
            return "[" + lastUpdated + "|" + retryCount + "]";
        }
    }

}
