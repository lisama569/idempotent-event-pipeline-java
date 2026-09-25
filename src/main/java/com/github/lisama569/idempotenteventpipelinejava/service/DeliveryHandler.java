package com.github.lisama569.idempotenteventpipelinejava.service;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;

/** 下游投递抽象（本地可注入失败模式）。 */
public interface DeliveryHandler {

    com.github.lisama569.idempotenteventpipelinejava.model.DispatchDecision handle(EventRecord event);
}
