package com.khs.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallbackDemo {
	static Logger logger=LoggerFactory.getLogger(ProducerDemo.class);
	
	public static void main(String[] args) {
		String BOOT_STRAP_SERVER="127.0.0.1:9092";
		String TOPIC_NAME="first_topic";
		String MESSAGE="Sending message withh callback!";
		
		// 1. Create Producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// 2. Create the producer
		KafkaProducer<String, String> producer=new KafkaProducer<>(properties);
		
		// 3. Create a producer record and Send data
		ProducerRecord<String, String> record=new ProducerRecord<String, String>(TOPIC_NAME, MESSAGE);
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				if (e == null) {
					logger.info("***ProducerCallbackDemo: Successfully received the details as: \n" + "Topic:" + recordMetadata.topic() + "\n"
							+ "Partition:" + recordMetadata.partition() + "\n" + "Offset" + recordMetadata.offset()
							+ "\n" + "Timestamp" + recordMetadata.timestamp());
				}
				else 
					logger.error("*****ProducerCallbackDemo: Error occurred while sending the data.", e);
			}
		});		
		//flush data
		producer.flush();
		//flush and close producer
		producer.close();
	}
}
