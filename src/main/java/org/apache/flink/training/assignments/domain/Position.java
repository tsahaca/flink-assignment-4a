package org.apache.flink.training.assignments.domain;

import jdk.jfr.DataAmount;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
//@Builder
@NoArgsConstructor
@AllArgsConstructor
//public class Position implements Serializable {
public class Position extends IncomingEvent {

    private static final long serialVersionUID = -2499451017707868513L;
    private String account;
    private String subAccount;
    private String cusip;
    public int quantity;

    @Override
    public byte[] key() {
        return account.getBytes();
    }
}
