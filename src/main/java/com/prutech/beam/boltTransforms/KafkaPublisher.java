package com.prutech.beam.boltTransforms;

import java.util.Properties;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class KafkaPublisher extends DoFn<KV<String, String>, Void> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = Logger.getLogger(KafkaPublisher.class);

	// Replace with your Kafka broker(s) address
	private static final String KAFKA_BROKER = "localhost:9092";

	// Configure Kafka producer properties
	private static Properties kafkaProperties = new Properties();

	static {
		kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
		kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	}

	@ProcessElement
	public void processElement(@Element KV<String, String> kafkaMessage) {
		String kafkaTopic = kafkaMessage.getKey();
		String kafkaMessageValue = kafkaMessage.getValue();

		// Create a Kafka producer instance
		try (Producer<String, String> producer = new KafkaProducer<>(kafkaProperties)) {
			// Create a Kafka record with the topic and message
			ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, kafkaMessageValue);

			// Send the record to Kafka
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						LOG.error("Error publishing to Kafka topic: " + kafkaTopic, exception);
					} else {
						LOG.info("Published to Kafka topic: " + kafkaTopic + ", Partition: " + metadata.partition()
								+ ", Offset: " + metadata.offset());
					}
				}
			});

			// Flush and close the producer
			producer.flush();
		} catch (Exception e) {
			LOG.error("Error creating or using Kafka producer", e);
		}
	}
}
