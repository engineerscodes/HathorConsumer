package org.zeuscommerce.app.Entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.validation.constraints.*;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;
import org.zeuscommerce.app.Util.ProductStatus;

import java.util.Map;


@Document
@Builder
@Data
public class Product {

    @Id
    private String id;

    @NotBlank
    private String name;

    @NotBlank
    private String description;

    @DecimalMin(value = "0",message = "Price can't be negative")
    private double price;

    @DecimalMin(value = "0",message = "Price can't be negative")
    private double cost;


    private Map<String, Object> metaData ; //will have sold to ,sold date and etc ,can't have sold details is it unsold


    @DecimalMin(value = "-90.0", message = "Latitude must be between -90 and 90")
    @DecimalMax(value = "90.0", message = "Latitude must be between -90 and 90")
    private double latitude;

    @DecimalMin(value = "-180.0", message = "Longitude must be between -180 and 180")
    @DecimalMax(value = "180.0", message = "Longitude must be between -180 and 180")
    private double longitude;

    @JsonIgnore
    @Version
    private Long version;

    @Enumerated(EnumType.STRING)
    private ProductStatus status;

    @Min(value = 0,message = "Can't be negative")
    @Max(value = 100,message = "Max 100 only possible")
    private Integer quantity;

}
