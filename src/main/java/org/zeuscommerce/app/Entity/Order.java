package org.zeuscommerce.app.Entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.zeuscommerce.app.Util.OrderStatus;
import org.zeuscommerce.app.Util.PlacedProduct;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;


@Document
@Builder
@Data //take of time like order place order confirmed and order Delivered
public class Order {

    @Id
    private String id;


    private Map<String, Object> orderDetails; // store user data & location

    Set<PlacedProduct> placedProducts;


    @JsonIgnore
    @Version
    private Long version;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @DecimalMin(value = "0",message = "Price can't be negative")
    private BigDecimal orderProductSum;

    @DecimalMin(value = "0",message = "Price can't be negative")
    private double additionalFees;

    private boolean rushOrder;


}
