package org.zeuscommerce.app.Listenser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.zeuscommerce.app.Entity.Order;
import org.zeuscommerce.app.Repo.OrderRepo;
import org.zeuscommerce.app.Service.OrderService;
import org.zeuscommerce.app.Util.OrderMsg;
import org.zeuscommerce.app.Util.OrderStatus;
import org.zeuscommerce.app.Util.Util;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;


@Slf4j
@Service
public class LotConsumer {

    @Autowired
    Receiver receiver;

    @Autowired
    OrderService orderService;

    @PostConstruct
    public void startListening() {
        ConsumeOptions consumeOptions = new ConsumeOptions().qos(1);
        receiver.consumeManualAck("zeus-queue-1",consumeOptions)
                .subscribe(delivery -> {
                    String message = new String(delivery.getBody());
                    try {
                        System.out.println(message);
                        orderService.consume(Util.Json2Order(message)).subscribe();
                        delivery.ack();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    delivery.ack(); // ack even if Error
                });
    }
}
