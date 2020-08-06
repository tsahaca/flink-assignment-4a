package org.apache.flink.training.assignments.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Order extends IncomingEvent {

    private static final long serialVersionUID = 1L;

    private String orderId;

    private String cusip;
    private String assetType;
    private BuySell buySell;
    private BigDecimal bidOffer;
    private String currency;
    private int quantity;
    private long orderTime;

    private List<Allocation> allocations;


    @Override
    public byte[] key() {
        return orderId.getBytes();
    }
}


