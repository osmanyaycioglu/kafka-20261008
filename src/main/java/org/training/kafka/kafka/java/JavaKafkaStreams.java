package org.training.kafka.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import tools.jackson.databind.ObjectMapper;

import java.util.Properties;

public class JavaKafkaStreams {

    public class KafkaStreamReadJava {
        public static void main(String[] args) {
            Properties propertiesLoc = new Properties();

            propertiesLoc.put(StreamsConfig.APPLICATION_ID_CONFIG,
                              "app-1");
            propertiesLoc.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                              "127.0.0.1:29092");
            propertiesLoc.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                              Serdes.Integer()
                                    .getClass());
            propertiesLoc.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                              Serdes.String()
                                    .getClass());
            propertiesLoc.put(StreamsConfig.CLIENT_ID_CONFIG,
                              "client1");
            propertiesLoc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                              "earliest");
            propertiesLoc.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                              10);


            StreamsBuilder           streamsBuilderLoc = new StreamsBuilder();
            KStream<Integer, String> streamLoc         = streamsBuilderLoc.stream("test1");
            streamLoc.peek(((k, v) -> System.out.println("key : "
                                                         + k
                                                         + " value : "
                                                         + v
                                                         + " C1 Thread : "
                                                         + Thread.currentThread()
                                                                 .getName())))
                     .mapValues(v -> v + " değiştirdim")
                     .to("to-test-topic",
                         Produced.with(Serdes.Integer(),
                                       Serdes.String()));

            ObjectMapper objectMapperLoc = new ObjectMapper();
            streamLoc.peek(((k, v) -> System.out.println("key : "
                                                         + k
                                                         + " value : "
                                                         + v
                                                         + " C2 Thread : "
                                                         + Thread.currentThread()
                                                                 .getName())))
                     .mapValues(v -> new Customer(v.substring(1,
                                                              10),
                                                  v.substring(11,
                                                              20),
                                                  v.substring(21,
                                                              30))
                     )
                     .mapValues(c -> {
                         try {
                             return objectMapperLoc.writer()
                                                   .writeValueAsString(c);
                         } catch (Exception eParam) {
                             throw new RuntimeException(eParam);
                         }
                     })
                     .to("to-customer-topic",
                         Produced.with(Serdes.Integer(),
                                       Serdes.String()));

            Topology topologyLoc = streamsBuilderLoc.build();
            KafkaStreams kafkaStreamsLoc = new KafkaStreams(topologyLoc,
                                                            propertiesLoc);
            System.out.println("starting stream");
            kafkaStreamsLoc.start();

            Runtime.getRuntime()
                   .addShutdownHook(new Thread(() -> {
                       kafkaStreamsLoc.close();
                   }));

        }
    }

}
