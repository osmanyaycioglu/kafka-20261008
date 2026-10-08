package org.training.kafka.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@SpringBootApplication
@EnableKafka
public class Kafka20261008Application {

    @Bean
    public Executor createExecuter(){
        return Executors.newFixedThreadPool(10);
    }

    public static void main(String[] args) {
        SpringApplication.run(Kafka20261008Application.class,
                              args);
    }

}
