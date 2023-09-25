package com.prutech.beam.kafka;

import java.util.Properties;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaEventSender extends DoFn<String, Void> {
	private KafkaProducer<String, String> producer;
	private String kafkaTopic;

	public KafkaEventSender(String bootstrapServers, String topic) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.producer = new KafkaProducer<>(properties);
		this.kafkaTopic = topic;
	}

	@ProcessElement
	public void processElement(ProcessContext context) {
		String event = context.element();
		ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, event);
		producer.send(record);
	}

	@Teardown
	public void teardown() {
		producer.close();
	}
}
