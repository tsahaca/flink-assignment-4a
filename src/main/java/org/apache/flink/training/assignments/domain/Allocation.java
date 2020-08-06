package org.apache.flink.training.assignments.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class Allocation {

    private static final long serialVersionUID = 1L;

    private String orderId;
    private String account;
    private String subAccount;
    private int quantity;
    private int orderSize;

    public Allocation(final String acct,
                      final String subAct,
                      final int qty){
        this.account=acct;
        this.subAccount=subAct;
        this.quantity=qty;
    }

}

