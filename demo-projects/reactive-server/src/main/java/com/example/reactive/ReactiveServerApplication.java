package com.example.reactive;

import io.netty.channel.ChannelOption;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
public class ReactiveServerApplication {

    public static void main(String[] args) {
        // Production-safe enhanced stack traces (bytecode instrumentation, negligible overhead)
        ReactorDebugAgent.init();

        // Limit Netty event-loop threads to 4 for demo purposes
        // (default = max(Runtime.availableProcessors(), 4), which can be 16-32 on modern CPUs)
        System.setProperty("reactor.netty.ioWorkerCount", "4");

        // Dedicated acceptor (selector) loop, separate from the 4 workers.
        // Without it, the busy workers also handle accept() — under a 10,000-connection
        // storm the accept queue overflows and Windows actively refuses (RST) new
        // connections. Tomcat survives the same storm because it has a dedicated acceptor.
        System.setProperty("reactor.netty.ioSelectCount", "1");

        // Limit Reactor parallel scheduler to 4 threads
        // (used by delayElement(), publishOn(Schedulers.parallel()), etc.)
        System.setProperty("reactor.schedulers.defaultPoolSize", "4");

        SpringApplication.run(ReactiveServerApplication.class, args);
    }

    /**
     * Raise the TCP accept backlog (Netty default ~1000). The 10,000-request step opens
     * thousands of connections at once — without this, the backlog overflows and
     * connections are refused/reset before the event loop ever sees them.
     * (Tomcat side is tuned the same way via max-connections/accept-count in YAML.)
     */
    @Bean
    WebServerFactoryCustomizer<NettyReactiveWebServerFactory> nettyBacklogCustomizer() {
        return factory -> factory.addServerCustomizers(
                http -> http.option(ChannelOption.SO_BACKLOG, 8192));
    }
}
