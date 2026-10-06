package com.github.lisama569.idempotenteventpipelinejava.pipeline;

/** 接收结论：提交事件时接收侧给出的即时结果（投递在后台推进）。 */
public enum ReceiveOutcome {
    /** 已受理，后台会继续投递 */
    ACCEPTED,
    /** 重复提交：该事件已成功投递（幂等，不产生新副作用） */
    DELIVERED,
    /** 校验/乱序拒绝，或重复提交一个已被拒绝/已进死信的事件 */
    REJECTED
}
