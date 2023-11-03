package com.foresee.mqdemo3.consumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author: yuanrui
 */
@RocketMQMessageListener(topic = "${rocketmq.testTopic}",
        consumerGroup = "${rocketmq.consumer.group.abc}")
@Component
@Slf4j
public class SomeTopicConsumer implements RocketMQListener<MessageExt>, RocketMQPushConsumerLifecycleListener {

    @Override
    public void prepareStart(DefaultMQPushConsumer defaultMQPushConsumer) {
        // set consumer consume message from now
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        defaultMQPushConsumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()));
    }

    @SneakyThrows
    @Override
    public void onMessage(MessageExt message) {
        String taskId = message.getKeys();
        byte[] body = message.getBody();
        String value = new String(body, StandardCharsets.UTF_8);

        //log.info("Received message, taskId : {}, value : {}", taskId, value);
        log.info("Received message, message : {}", message);
        if (true) {
            throw new Exception("人工异常，不服憋着！");
        }
    }
}
