package org.zeuscommerce.app.Listenser;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zeuscommerce.app.Service.OrderService;
import org.zeuscommerce.app.Util.Util;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;

@Slf4j
@Service
public class LessConsumer {

    @Autowired
    Receiver receiver;

    @Autowired
    OrderService orderService;

    @PostConstruct
    public void startListening() {
        ConsumeOptions consumeOptions = new ConsumeOptions().qos(1);
        receiver.consumeManualAck("zeus-queue-3",consumeOptions)
                .subscribe(delivery -> {
                    String message = new String(delivery.getBody());
                    try {
                        orderService.consume(Util.Json2Order(message)).subscribe();
                        delivery.ack();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    delivery.ack(); // ack even if Error
                });
    }


}
