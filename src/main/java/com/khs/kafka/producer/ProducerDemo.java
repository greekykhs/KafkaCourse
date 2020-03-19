package com.khs.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	public static void main(String[] args) {
		String BOOT_STRAP_SERVER="127.0.0.1:9092";
		String TOPIC_NAME="first_topic";
		String MESSAGE="Hello World!";
		
		// 1. Create Producer properties
		Properties properties=new Properties();
		/*
		properties.setProperty("bootstrap.servers", BOOT_STRAP_SERVER);
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		*/
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// 2. Create the producer
		KafkaProducer<String, String> producer=new KafkaProducer<>(properties);
		
		// 3. Send data
		ProducerRecord<String, String> record=new ProducerRecord<String, String>(TOPIC_NAME, MESSAGE);
		producer.send(record);
		
		//flush data
		producer.flush();
		//flush and close producer
		producer.close();
	}
}
