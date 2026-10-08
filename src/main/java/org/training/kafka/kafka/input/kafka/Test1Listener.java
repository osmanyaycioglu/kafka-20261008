package org.training.kafka.kafka.input.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

@Component
public class Test1Listener {

    private static final Logger logger = LoggerFactory.getLogger(Test1Listener.class);

    private static BlockingQueue<String> strings = new ArrayBlockingQueue<>(5_000);

    @Autowired
    @Qualifier("createExecuter")
    private Executor executor;

    // @PostConstruct
    public void init() {
        for (int i = 0; i < 10; i++) {
            WorkerThread workerThreadLoc = new WorkerThread();
            workerThreadLoc.start();
        }
    }

    @KafkaListener(id = "test1-client",
            topics = "test1",
            groupId = "sgroup1",
            concurrency = "3",
            clientIdPrefix = "myClient",
            ackMode = "MANUAL_IMMEDIATE")
    public void listenTest1Message(String value,
                                   Acknowledgment acknowledgmentParam) {
        System.out.println("Received Message test1 : " + value + " Thread : " + Thread.currentThread()
                                                                                      .getName());
        acknowledgmentParam.acknowledge();
    }

    //  @KafkaListener(topics = "test2", groupId = "sgroup1",concurrency = "3")
    public void listenTest2Message(String value) {
        System.out.println("Received Message test2 : " + value);
        strings.add(value);
    }

    // @KafkaListener(topics = "test3", groupId = "sgroup1",concurrency = "3")
    public void listenTest3Message(String value) {
        System.out.println("Received Message test3 : " + value);

        executor.execute(() -> {
            System.out.println("Message processed : " + value);
        });
    }


    public class WorkerThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    String message = strings.take();
                    System.out.println("Message processed : " + message);
                } catch (Exception eParam) {
                    logger.error("[WorkerThread][run]-> *Error* : " + eParam.getMessage(),
                                 eParam);
                }
            }
        }
    }

}
