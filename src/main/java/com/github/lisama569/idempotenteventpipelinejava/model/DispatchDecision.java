package com.github.lisama569.idempotenteventpipelinejava.model;

import com.github.lisama569.idempotenteventpipelinejava.domain.DeliveryResult;
import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;

/** 一次投递处理后的判定。 */
public class DispatchDecision {
    public DeliveryResult result;
    public RejectReason reason;
    public String detail;
}
