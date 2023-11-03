package com.foresee.mqdemo3.controller;

import com.foresee.mqdemo3.mq.RocketMQTemplateProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * @author: yuanrui
 */

@RequestMapping("/api")
@RestController
public class HelloController {

    @Autowired
    private RocketMQTemplateProducer rocketMQTemplateProducer;

    @Value("${rocketmq.testTopic}")
    private String topic;

    @GetMapping("/hello")
    public String hello() {
        return "hello";
    }

    @PostMapping("/send/{id}")
    public String send(@PathVariable String id) {
        String msg = "message" + id;
        String taskId = UUID.randomUUID().toString();
        Message<String> message = MessageBuilder
                .withPayload(msg)
                .setHeader(RocketMQHeaders.KEYS, taskId)
                .build();
        SendResult sendResult = rocketMQTemplateProducer.syncSendMessage(topic, message);
        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
            return "success";
        } else {
            return "error";
        }
    }
}
