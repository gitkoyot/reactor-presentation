package com.example.reactive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ReactiveServerApplication {

    public static void main(String[] args) {
        // Limit Netty event-loop threads to 4 for demo purposes
        // (default = Runtime.availableProcessors(), which can be 16-32 on modern CPUs)
        System.setProperty("reactor.netty.ioWorkerCount", "4");

        // Limit Reactor parallel scheduler to 4 threads
        // (used by delayElement(), publishOn(Schedulers.parallel()), etc.)
        System.setProperty("reactor.schedulers.defaultPoolSize", "4");

        SpringApplication.run(ReactiveServerApplication.class, args);
    }
}
