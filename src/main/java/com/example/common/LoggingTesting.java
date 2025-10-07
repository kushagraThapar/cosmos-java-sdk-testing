package com.example.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingTesting {

    private static final Logger APACHE_LOGGER = LogManager.getLogger(LoggingTesting.class.getName());
    private static final org.slf4j.Logger SLF4J_LOGGER = LoggerFactory.getLogger(LoggingTesting.class);

    public static void main(String[] args) {
        APACHE_LOGGER.info("apache - This is an info log message.");
        SLF4J_LOGGER.info("slf4j - This is an info log message.");
        APACHE_LOGGER.error("apache - This is an error log message.");
        SLF4J_LOGGER.error("slf4j - This is an error log message.");
        APACHE_LOGGER.warn("apache - This is a warn log message.");

        String formatMessage = "formatted";

        APACHE_LOGGER.info("apache - This is a {} message", formatMessage);
        SLF4J_LOGGER.info("slf4j - This is a {} message", formatMessage);

        RuntimeException exception = new RuntimeException("Cosmos DB timeout error");
        APACHE_LOGGER.error("apache - This is an error log with {} message and exception: ", formatMessage, exception);
        SLF4J_LOGGER.error("slf4j - This is an error log with {} message and exception: ", formatMessage, exception);
        APACHE_LOGGER.warn("apache - This is a warn log with {} message and exception: ", formatMessage, exception);
        SLF4J_LOGGER.warn("slf4j - This is a warn log with {} message and exception: ", formatMessage, exception);
        APACHE_LOGGER.info("apache - This is an info log with {} message and exception: ", formatMessage, exception);
        SLF4J_LOGGER.info("slf4j - This is an info log with {} message and exception: ", formatMessage, exception);
    }
}
