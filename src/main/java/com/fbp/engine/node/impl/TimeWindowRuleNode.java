package com.fbp.engine.node.impl;

import com.fbp.engine.message.Message;
import com.fbp.engine.node.AbstractNode;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Predicate;

/**
 *  input port : in
 *  output port : alert, pass
 */
public class TimeWindowRuleNode extends AbstractNode {
    private final Predicate<Message> condition;
    private final long windowMs;
    private final int threshold;
    private Queue<Long> events;

    public TimeWindowRuleNode(String id, Predicate<Message> condition, long windowMs, int threshold) {
        super(id);
        this.condition = condition;
        this.windowMs = windowMs;
        this.threshold = threshold;
        this.events = new LinkedList<>();

        addInputPort("in");
        addOutputPort("alert");
        addOutputPort("pass");
    }

    @Override
    protected void onProcess(Message message) {
        long now = System.currentTimeMillis();

        if(condition.test(message)){
            events.offer(now);
        }

        while(!events.isEmpty() && (now - events.peek() > windowMs)){
            events.poll();
        }

        if(events.size() >= threshold){
            send("alert", message);
        }else{
            send("pass", message);
        }
    }
}
