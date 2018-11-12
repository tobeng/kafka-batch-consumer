package com.tobeng.batch.thread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * @author yaorui
 * @package com.tobeng.batch.thread
 * @date 2018/11/12
 */
@Slf4j
public class StatictisThrad implements Runnable {

    private List<ConsumerRecord<?, ?>> records;

    public  StatictisThrad(List<ConsumerRecord<?, ?>> records){
        this.records = records;
    }

    @Override
    public void run() {
        for (ConsumerRecord<?, ?> record : records) {
            log.info("Received: " + record.value().toString());
        }
    }
}
