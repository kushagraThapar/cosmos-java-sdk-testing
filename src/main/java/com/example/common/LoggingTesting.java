package com.example.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingTesting {

    private static final Logger logger = LoggerFactory.getLogger(LoggingTesting.class);

    public static void main(String[] args) {
        logger.info("This is an info log message.");
        logger.error("This is an error log message.");
        logger.warn("This is a warn log message.");

        String formatMessage = "formatted";

        logger.info("This is a {} message", formatMessage);

        RuntimeException exception = new RuntimeException("Cosmos DB timeout error");
        logger.error("This is an error log with {} message and exception: ", formatMessage, exception);
        logger.warn("This is a warn log with {} message and exception: ", formatMessage, exception);
        logger.info("This is an info log with {} message and exception: ", formatMessage, exception);
    }
}
