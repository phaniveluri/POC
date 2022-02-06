package com.amazonaws.kafka.samples;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleProducer {

	KafkaProducer<String, String> producer = null;
	private static final Logger logger = LogManager.getLogger(HandlerMSK.class);
	

	public SimpleProducer() {
		// TODO Auto-generated constructor stub
		logger.info("in Constructor..");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-1.eligibilitycluster.0ty3pm.c19.kafka.us-east-1.amazonaws.com:9092,b-2.eligibilitycluster.0ty3pm.c19.kafka.us-east-1.amazonaws.com:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
		logger.info("Complete Constructor..");
	}

	public void publish(String topic, String strData) throws InterruptedException {
		ProducerRecord<String, String> data;
		data = new ProducerRecord<String, String>(topic, strData);
		logger.info("Before Send..");
		producer.send(data);
		logger.info("After Send..");
		
	}
}