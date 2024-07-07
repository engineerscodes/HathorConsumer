package org.zeuscommerce.app.Repo;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import org.zeuscommerce.app.Entity.Order;
@Repository
public interface OrderRepo extends ReactiveMongoRepository<Order,String> {
}
