package org.apache.flink.training.assignments.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PositionByCusip extends IncomingEvent {
    private static final long serialVersionUID = 7946678872780554209L;
    private String cusip;
    private int quantity;
    //private String orderId;

    @Override
    public byte[] key() {
        return cusip.getBytes();
    }
}

