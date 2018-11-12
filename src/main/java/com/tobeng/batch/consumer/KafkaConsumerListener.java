package com.tobeng.batch.consumer;

import com.tobeng.batch.thread.StatictisThrad;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * @author yaorui
 * @package com.tobeng.batch.consumer
 * @date 2018/11/12
 */
@Slf4j
//@Component
public class KafkaConsumerListener {

    private static final String TPOIC = "records-11";

    private ExecutorService pool = newCachedThreadPool();

    @KafkaListener(topicPartitions = { @TopicPartition(topic = TPOIC, partitions = { "0" }) })
    public void listenPartition0(List<ConsumerRecord<?, ?>> records) {
        pool.execute(new StatictisThrad(records));
    }
//
    @KafkaListener(topicPartitions = { @TopicPartition(topic = TPOIC, partitions = { "1" }) })
    public void listenPartition1(List<ConsumerRecord<?, ?>> records) {
        pool.execute(new StatictisThrad(records));
    }

    @KafkaListener(topicPartitions = { @TopicPartition(topic = TPOIC, partitions = { "2" }) })
    public void listenPartition2(List<ConsumerRecord<?, ?>> records) {
        pool.execute(new StatictisThrad(records));
    }

    @KafkaListener(topicPartitions = { @TopicPartition(topic = TPOIC, partitions = { "3" }) })
    public void listenPartition3(List<ConsumerRecord<?, ?>> records) {
        pool.execute(new StatictisThrad(records));
    }

}
