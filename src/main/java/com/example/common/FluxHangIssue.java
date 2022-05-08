package com.example.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Iterator;

public class FluxHangIssue {

    private final static Logger logger = LoggerFactory.getLogger(FluxHangIssue.class);

    public static void main(String[] args) throws InterruptedException {

        Flux<Integer> integerFlux = Flux.range(0, 7).map(number -> {
            logger.info("Number is : {}", number);
            if (number > 5) {
                throw new OutOfMemoryError("Custom GC Failure");
            }
            return number;
        }).timeout(Duration.ofSeconds(2)).doOnError(ex -> {
            logger.error("Completed exceptionally", ex);
        }).doOnNext(next -> {
            logger.info("Next is : {}", next);
        }).doOnComplete(() -> {
            logger.info("Completed successfully");
        }).doFinally(signalType -> {
            logger.info("Finally signal is : {}", signalType);
        }).onErrorMap(throwable -> {
            logger.info("On error map", throwable);
            return throwable;
        }).onErrorContinue((throwable, object) -> {
            logger.error("on error continue : {}", object, throwable);
        }).onErrorStop().onErrorReturn(6).onErrorResume(throwable -> {
            logger.info("on error resume", throwable);
            return Mono.error(throwable);
        }).subscribeOn(Schedulers.boundedElastic());

        Iterator<Integer> integers = integerFlux.toIterable().iterator();
        while(integers.hasNext()) {
            logger.info("Next value is : {}", integers.next());
        }

        logger.info("Going to sleep now");

        Thread.sleep(5000);

        logger.info("I woke up");
    }
}
