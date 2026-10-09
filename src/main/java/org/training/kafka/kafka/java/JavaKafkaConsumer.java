package org.training.kafka.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class JavaKafkaConsumer {

    private static AtomicLong count = new AtomicLong();
    private static BlockingQueue<ConsumerRecord<Integer, String>> records = new ArrayBlockingQueue<>(1_000_000);

    public static void main(String[] args) {
        Properties propertiesLoc = new Properties();
        propertiesLoc.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                          "127.0.0.1:29092");
        propertiesLoc.put(ConsumerConfig.GROUP_ID_CONFIG,
                          "java-group-2");
        propertiesLoc.put(ConsumerConfig.CLIENT_ID_CONFIG,
                          "client-1");
        propertiesLoc.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                          UUID.randomUUID()
                              .toString());
        propertiesLoc.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                          IntegerDeserializer.class.getName());
        propertiesLoc.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        propertiesLoc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                          "earliest");

        propertiesLoc.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                          "true");

        propertiesLoc.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                          20_000);
        propertiesLoc.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                          (100 * 1024 * 1024 ));


//        try (KafkaConsumer<Integer, String> kafkaConsumerLoc = new KafkaConsumer<>(propertiesLoc)) {
//            kafkaConsumerLoc.subscribe(Arrays.asList("test1"));
//            while (true) {
//                ConsumerRecords<Integer, String> pollLoc = kafkaConsumerLoc.poll(Duration.ofMillis(1_000));
//                for (ConsumerRecord<Integer, String> recordLoc : pollLoc) {
//                    System.out.println("java read --- : " + recordLoc);
//                }
//                // kafkaConsumerLoc.commitSync();
//                try {
//                    Thread.sleep(1);
//                } catch (Exception ignored) {
//                }
//
//            }
//        }

        try (KafkaConsumer<Integer, String> kafkaConsumerLoc = new KafkaConsumer<>(propertiesLoc)) {
            kafkaConsumerLoc.subscribe(Arrays.asList("test1"));
            for (int i = 0; i < 24; i++) {
                KafkaWorkerThread threadLoc = new KafkaWorkerThread(kafkaConsumerLoc);
                threadLoc.setName("Kafka-TH-" + i);
                threadLoc.start();
            }
            KafkaTPSThread kafkaTPSThreadLoc = new KafkaTPSThread();
            kafkaTPSThreadLoc.start();

            while (true) {
                ConsumerRecords<Integer, String> pollLoc = kafkaConsumerLoc.poll(Duration.ofMillis(1_000));
                for (ConsumerRecord<Integer, String> recordLoc : pollLoc) {
                    records.add(recordLoc);
                }
                kafkaConsumerLoc.commitSync();
            }
        }
    }


    public static class KafkaWorkerThread extends Thread {
        private final KafkaConsumer<Integer, String> kafkaConsumerLoc;

        public KafkaWorkerThread(final KafkaConsumer<Integer, String> kafkaConsumerLocParam) {
            kafkaConsumerLoc = kafkaConsumerLocParam;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ConsumerRecord<Integer, String> takeLoc = records.take();
//                    System.out.println("Received message : " + takeLoc + " Thread : " + Thread.currentThread()
//                                                                                              .getName());

//                    Map<TopicPartition, OffsetAndMetadata> mapLoc = new HashMap<>();
//                    mapLoc.put(new TopicPartition(takeLoc.topic(),
//                                                  takeLoc.partition()),
//                               new OffsetAndMetadata(takeLoc.offset()));
//                    kafkaConsumerLoc.commitSync(mapLoc);
                    count.incrementAndGet();
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
        }
    }

    public static class KafkaTPSThread extends Thread {


        @Override
        public void run() {
            long preCount = count.get();
            while (true) {
                try {
                    Thread.sleep(1_000);
                    long lLoc = count.get();
                    System.out.println(" Count : "  + (lLoc - preCount));
                    preCount = lLoc;
                } catch (Exception exp) {
                    exp.printStackTrace();
                }
            }
        }
    }

}

