package com.xh.kafka.example;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

public class KafkaProducerDemo {
    public static void main(String[] args) {

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.INFO);


        boolean isAsync = false;
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
        producerThread.start();
    }
}
