package org.zeuscommerce.app.Service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.zeuscommerce.app.Entity.Order;
import org.zeuscommerce.app.Entity.Product;
import org.zeuscommerce.app.Messaging.RabbitClient;
import org.zeuscommerce.app.Repo.OrderRepo;
import org.zeuscommerce.app.Repo.ProductRepo;
import org.zeuscommerce.app.Util.*;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OrderService {

    @Autowired
    OrderRepo orderRepo;

    @Autowired
    ProductRepo productRepo;

    @Autowired
    ProductService productService;

    @Autowired
    RabbitClient rabbitClient;

    @Autowired
    private TransactionalOperator transactionalOperator;


    public Mono<Order> consume(OrderMsg order){
        return orderRepo.findById(order.getId()).flatMap(dbOrder->{
            log.info("[START] ORDER FROM DB {}",dbOrder.getOrderDetails());
            Set<String > products = dbOrder.getPlacedProducts().stream().map(PlacedProduct::getProductId).collect(Collectors.toSet());
            Map<String,Map<String,Long>> productVersionMap = order.getProductVersion();
            Mono<List<Product>> productMono = productService.findAllByID(new ArrayList<>(products)).collectList();
            return Mono.zip(Mono.just(dbOrder),productMono).flatMap(tuple->{
                Order _order = tuple.getT1();
                List<Product> _products = tuple.getT2();
                BigDecimal total = BigDecimal.ZERO;
                for (Product product:_products){
                    Long versionInQueue = productVersionMap.get(product.getId()).get("productVersion");
                    Long versionInDB =  product.getVersion();
                    Long orderedQuantity = productVersionMap.get(product.getId()).get("quantity");
                    Long availableQuantity = product.getQuantity().longValue();
                    log.info("Order data : versionInDB {}/ VersionAtTimeOfOrdered {} AvailableQuantity {} / orderedQuantity {}",
                            versionInDB,versionInQueue,availableQuantity,orderedQuantity);
                    if(product.getStatus().equals(ProductStatus.UNSOLD) &&orderedQuantity<=availableQuantity && !Objects.equals(versionInDB, versionInQueue)){
                        // PUT IT BACK IN RABBITMQ RETRY FOR THE ONCE
                        rabbitClient.sendMessage(_order); // it be in retry queue till it find same version or get sold
                        throw  new OptimisticLockingFailureException("PRODUCT WAS UPDATED BY OTHER USER : UNSOLD");
                    } else if (product.getStatus().equals(ProductStatus.SOLD)|| product.getStatus().equals(ProductStatus.RETURNED)
                            || orderedQuantity>availableQuantity) {
                        _order.setStatus(OrderStatus.Cancel);
                        String msg = String.format("PRODUCT IS SOLD/RETURNED of Product : %s",product.getId());
                        Map<String,Object> orderData = _order.getOrderDetails();
                        orderData.put("cancel_reason",msg);
                        _order.setOrderDetails(orderData);
                        orderRepo.save(_order).subscribe();
                        throw  new RuntimeException(msg);
                    }else{
                        _order.setStatus(OrderStatus.Confirmed);
                        long newQuantity =availableQuantity-orderedQuantity;
                        Assert.isTrue(newQuantity>=0,"Not possible to place order,Product not available");
                        product.setQuantity((int) newQuantity);
                        if(newQuantity==0) product.setStatus(ProductStatus.SOLD);
                        productRepo.save(product).subscribe();
                        total=total.add(BigDecimal.valueOf(product.getPrice()*orderedQuantity));
                    }
                };
                _order.setStatus(OrderStatus.Confirmed);
                _order.setAdditionalFees(500);
                Map<String,Object> orderDetails = new HashMap<>(_order.getOrderDetails() != null ? _order.getOrderDetails() : Map.of());
                orderDetails.put("DeliveryDateTime", Util.formatDateTime(LocalDateTime.now().plusHours(2)));
                orderDetails.put("DeliveryPerson", "Naveen"); // usually to assign delivery we need another MS but due to Time ,just assign here assuming unlimited driver available
                _order.setOrderDetails(orderDetails);
                _order.setOrderProductSum(total);
                return orderRepo.save(_order).doOnSuccess(o->rabbitClient.sendMessageDelivery(o)); //throw OptimisticLockingFailureException  if order is update
            }).as(transactionalOperator::transactional);// -> Needs MongoDB replicate set -> custer is needed
        });
    }



}
