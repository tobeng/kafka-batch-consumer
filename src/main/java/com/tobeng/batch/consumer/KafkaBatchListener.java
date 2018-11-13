package com.tobeng.batch.consumer;

import com.tobeng.batch.thread.StatictisThrad;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
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
@Component
public class KafkaBatchListener {

    private static int i = 0;
    private ExecutorService pool = newCachedThreadPool();

    @KafkaListener(topics = "${spring.kafka.consumer.topic}", containerFactory = "batchFactory")
    public void listenPartition1(List<ConsumerRecord<?, ?>> records) {
        i++;
        pool.execute(new StatictisThrad(records));

    }

}
