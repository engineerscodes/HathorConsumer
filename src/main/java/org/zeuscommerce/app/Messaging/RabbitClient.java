package org.zeuscommerce.app.Messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.zeuscommerce.app.Entity.Order;
import org.zeuscommerce.app.Entity.Product;
import org.zeuscommerce.app.Repo.ProductRepo;
import org.zeuscommerce.app.Util.Address;
import org.zeuscommerce.app.Util.DeliveryStatus;
import org.zeuscommerce.app.Util.OrderMsg;
import org.zeuscommerce.app.Util.PlacedProduct;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.rabbitmq.*;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class RabbitClient {


    public static final String EXCHANGE_NAME = "zeus-retry-order";
    public static final String QUEUE_NAME_1 = "zeus-order-retry-queue";
    public static final String ROUTING_KEY_1 = "zeus.order.retry";

    public static final String EXCHANGE_NAME_2 = "zeus-delivery-order";
    public static final String QUEUE_NAME_2 = "zeus-delivery-order";
    public static final String ROUTING_KEY_2 = "zeus-delivery-order";

    @Autowired
    Sender RabbitSender;

    @Autowired
    ProductRepo productsRepo;


    @PostConstruct
    public void setup() {
        RabbitSender.declareExchange(ExchangeSpecification.exchange(EXCHANGE_NAME).durable(true))
                .then(RabbitSender.declareQueue(QueueSpecification.queue(QUEUE_NAME_1).durable(true)))
                .then(RabbitSender.bind(BindingSpecification.binding(EXCHANGE_NAME, ROUTING_KEY_1, QUEUE_NAME_1)))
                .subscribe();
        RabbitSender.declareQueue(QueueSpecification.queue(QUEUE_NAME_2).durable(true))
                .then(RabbitSender.declareExchange(ExchangeSpecification.exchange(EXCHANGE_NAME_2).durable(true)))
                .then(RabbitSender.bind(BindingSpecification.binding(EXCHANGE_NAME_2, ROUTING_KEY_2, QUEUE_NAME_2)))
                .subscribe();
    }

    public void sendMessage(Order savedOrder){
        log.info("Sending message to exchange");
        ObjectMapper mapper = new ObjectMapper();
        productsRepo.findAllById(savedOrder.getPlacedProducts().stream().map(PlacedProduct::getProductId).toList()).collectList().flatMapMany(products -> {
            Map<String, Map<String, Long>> idVersionQuantityMap = products.stream()
                    .collect(Collectors.toMap(
                                    Product::getId,
                                    product -> Map.of("productVersion",product.getVersion(),"quantity",product.getQuantity().longValue())
                            )
                    );
            OrderMsg msg = OrderMsg.builder().id(savedOrder.getId()).OrderVersion(savedOrder.getVersion()).productVersion(idVersionQuantityMap).build();
            try {
                byte[] jsonBytes = mapper.writeValueAsBytes(msg);
                Flux<OutboundMessage> outboundMessageFlux =
                        Flux.just(new OutboundMessage(EXCHANGE_NAME,ROUTING_KEY_1,jsonBytes));
                RabbitSender.sendWithPublishConfirms(outboundMessageFlux).doOnError(e->log.error("Error sending data to queue")).subscribe();
            } catch (JsonProcessingException e) {
                return Mono.error(new RuntimeException("Error sending message to exchange"));
            }
            return Mono.create(MonoSink::success);
        }).subscribe();
    }


    public void sendMessageDelivery(Order savedOrder){
        log.info("Sending message to exchange to Delivery Queue");
        ObjectMapper mapper = new ObjectMapper();
        Address address = (Address) savedOrder.getOrderDetails().get("address");
        Assert.isTrue(address!=null,"Address can't be Null");
        DeliveryStatus msg = DeliveryStatus.builder().orderId(savedOrder.getId()).destinationLatitude(address.getLatitude()).destinationLongitude(address.getLongitude())
                .sourceLatitude(40).sourceLongitude(40).build();
        try {
            byte[] jsonBytes = mapper.writeValueAsBytes(msg);
            Flux<OutboundMessage> outboundMessageFlux =
                    Flux.just(new OutboundMessage(EXCHANGE_NAME_2,ROUTING_KEY_2,jsonBytes));
            RabbitSender.sendWithPublishConfirms(outboundMessageFlux)
                    .doOnError(e->log.error("Error sending data to queue")).subscribe();
        } catch (JsonProcessingException e) {
            log.error("ERROR {}",e.getMessage());
        }

    }

}
