package com.github.lisama569.idempotenteventpipelinejava.model;

import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;

/** 接收判定结论。 */
public class ReceiveOutcome {
    public boolean duplicate;
    public String eventId;
    public String partition;
    public long seqInPartition;
    public RejectReason reason;
    public String detail;
}
