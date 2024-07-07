package org.zeuscommerce.app.Service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.zeuscommerce.app.Entity.Product;
import org.zeuscommerce.app.Repo.ProductRepo;
import org.zeuscommerce.app.Util.ProductStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class ProductService {

    @Autowired
    ProductRepo productRepo;

    public Mono<Product> findProduct(String productId){
        return productRepo.findById(productId).
                switchIfEmpty(Mono.error(new RuntimeException("Product Not found")));
    }

    public Flux<Product> findAllByID(ArrayList<String> productId){
        return productRepo.findAllById(productId)
                .switchIfEmpty(Flux.error(new RuntimeException("No Product with given Id's found")));
    }

}
