package com.khs.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	static Logger logger=LoggerFactory.getLogger(ProducerDemo.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String BOOT_STRAP_SERVER="127.0.0.1:9092";
		String TOPIC_NAME="my_topic";
		String MESSAGE="Sending message withh callback!";
		String KEY_1="key_1", KEY_2="key_2", KEY_3="key_3";
		String key="";
		
		// 1. Create Producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// 2. Create the producer
		KafkaProducer<String, String> producer=new KafkaProducer<>(properties);
		
		// 3. Create a producer record and Send data
		ProducerRecord<String, String> record=null;
		for (int i=1; i<10;i++)
		{
			if(i<=3)
				key=KEY_1;
			else if(i>3 && i<=6)
				key=KEY_2;
			else
				key=KEY_3;
			
			String tempKey=key;
			record=new ProducerRecord<String, String>(TOPIC_NAME,key, MESSAGE+":"+i);
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if (e == null) {
						logger.info("***ProducerDemoKey: Successfully received the details as: \n" + "Topic:" + recordMetadata.topic() + "\n"
								+ "Partition:" + recordMetadata.partition()
								+ "\n" + "Key:" + tempKey);
					}
					else 
						logger.error("*****ProducerDemoKey: Error occurred while sending the data.", e);
				}
			}).get();		
		}
		
		//flush data
		producer.flush();
		//flush and close producer
		producer.close();
	}
}
