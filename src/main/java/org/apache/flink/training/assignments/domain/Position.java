package org.apache.flink.training.assignments.domain;

import jdk.jfr.DataAmount;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Position implements Serializable {
    private static final long serialVersionUID = -2499451017707868513L;
    private String account;
    private String subAccount;
    private String cusip;
    private int quantity;
}
