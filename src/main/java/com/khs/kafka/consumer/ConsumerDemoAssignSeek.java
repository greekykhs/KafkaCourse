package com.khs.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String BOOT_STRAP_SERVER="127.0.0.1:9092";
		String TOPIC_NAME="first_topic";		
       
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_NAME, 0);
        
        long offsetToReadFrom = 15L;//start reading from offset 15
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key:"+record.key()+"\n"+
						"Value:"+record.value()+"\n"+
						"Partition:"+record.partition()+"\n"+
						"Offsets:"+record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false; // to exit the while loop
                    break; // to exit the for loop
                }
            }
        }

        logger.info("Exiting the application");
    }
	public static void testAssign(String topic, KafkaConsumer<String, String> consumer,
			Collection<TopicPartition> partitions) {
		
		//We start by asking the cluster for the partitions available in the topic. 
		//In case if we want to consume a specific partition, we can skip this part.
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		if (partitionInfos != null) {
			for (PartitionInfo partition : partitionInfos)
				partitions.add(new TopicPartition(partition.topic(), partition.partition()));
			
			//Once we know which partitions we want, we call assign() with the list.
			consumer.assign(partitions);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);

				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Key:" + record.key() + "\n" + 
										"Value:" + record.value() + "\n" + 
										"Partition:"+ record.partition() + "\n" + 
										"Offsets:" + record.offset());
				}
				consumer.commitSync();
			}
		}
	}
}
