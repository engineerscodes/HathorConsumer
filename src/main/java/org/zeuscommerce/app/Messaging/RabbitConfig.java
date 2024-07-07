package org.zeuscommerce.app.Messaging;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;


@Slf4j
@Configuration
public class RabbitConfig {


    @Bean
    public Receiver receiver() {
        log.info("[Start] : Creating RabbitMQ Receiver");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        ReceiverOptions options = new ReceiverOptions()
                .connectionFactory(connectionFactory)
                .connectionSupplier(cf -> {
                    try {
                        return connectionFactory.newConnection("hathor-rabbit");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        log.info("[END] : Creating RabbitMQ Receiver");

        return RabbitFlux.createReceiver(options);
    }

    @Bean
    Mono<Connection> connectionMono() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.useNio();
        return Mono.fromCallable(() -> connectionFactory.newConnection("zeus-rabbit")).cache();
    }

    @Bean
    public SenderOptions senderOptions(Mono<Connection> connectionMono) {
        return new SenderOptions()
                .connectionMono(connectionMono)
                .resourceManagementScheduler(Schedulers.boundedElastic());
    }

    @Bean
    public Sender sender(SenderOptions senderOptions) {
        return RabbitFlux.createSender(senderOptions);
    }


}
