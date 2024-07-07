package org.zeuscommerce.app.Util;

import lombok.*;

import java.util.Objects;
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PlacedProduct {
    String productId;
    Integer quantity;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlacedProduct that = (PlacedProduct) o;
        return Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId);
    }

}
