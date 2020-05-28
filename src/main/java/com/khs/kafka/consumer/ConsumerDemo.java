package com.khs.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	public static void main(String[] args) {
		Logger logger=LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String BOOT_STRAP_SERVER="127.0.0.1:9092";
		String groupId="my-fourth-application";
		String TOPIC_NAME="first_topic";		
		
		//Create consumer configs
		Properties properties=new Properties();		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
				BOOT_STRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
				StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
				StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, 
				groupId);
//		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
//				"earliest");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
				"latest");
//		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
//				"none");
		
		//create consumer
		KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);
		
		//subscribe consumer to our topics
		//consumer.subscribe(Collections.singleton(TOPIC_NAME));
		consumer.subscribe(Arrays.asList(TOPIC_NAME));
		
		ConsumerRecords<String, String> records;
		
		//poll for new data
		while(true) {
			//new in Kafka 2.0.0
			//consumer.poll(100);
			
			records=consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record:records) {
				logger.info("Key:"+record.key()+"\n"+
						"Value:"+record.value()+"\n"+
						"Partition:"+record.partition()+"\n"+
						"Offsets:"+record.offset());
			}
		}		
	}
}
