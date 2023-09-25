package com.prutech.beam.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaEventProducer {
	private KafkaProducer<String, String> producer;
	private String kafkaTopic;

	public KafkaEventProducer(String bootstrapServers, String topic) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.producer = new KafkaProducer<>(properties);
		this.kafkaTopic = topic;
	}

	public void sendEvent(String event) {
		ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, event);
		producer.send(record);
	}

	public void close() {
		producer.close();
	}
}
