package com.github.lisama569.idempotenteventpipelinejava.web;

/** REST 出参 DTO。 */
public class ApiDtos {

    public static class StatusView {
        public String eventId;
        public String partition;
        public long seqInPartition;
        public String status;
        public int attemptCount;
        public String rejectReason;
        public String rejectDetail;
        public int observedSideEffects;
    }
}
