package org.training.kafka.kafka.java;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class JavaKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(JavaKafkaProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                  "Client1");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  "127.0.0.1:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                  RoundRobinPartitioner.class.getName());


        KafkaProducer<Integer, String> producer   = new KafkaProducer<>(props);
        List<Future<RecordMetadata>>   futuresLoc = new ArrayList<>(1_100);
        for (int i = 1; i <= 10_000; i++) {
            Future<RecordMetadata> sendLoc = producer.send(new ProducerRecord<>("first1",
                                                                                i,
                                                                                "Simple Message-" + i));
            futuresLoc.add(sendLoc);

        }

        for (Future<RecordMetadata> futureLoc : futuresLoc) {
            try {
                final RecordMetadata recordMetadataLoc = futureLoc.get(1000, TimeUnit.MILLISECONDS);
                writeDebugLog(() -> "WD Record send  : " + recordMetadataLoc);
                System.out.println("Record send  : " + recordMetadataLoc);
                if (logger.isDebugEnabled()) {
                    logger.debug("Record send  : " + recordMetadataLoc);
                }
            } catch (Exception eParam) {
                logger.error("[JavaKafkaProducer][main]-> *Error* : " + eParam.getMessage(),eParam);
            }

        }

        System.out.println("Finished - Closing Kafka Producer.");
        producer.close();

    }

    public static void writeDebugLog(final Supplier<String> supplierParam) {
        if (logger.isDebugEnabled()) {
            logger.debug(supplierParam.get());
        }
    }


}
