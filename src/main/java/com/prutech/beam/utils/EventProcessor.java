package com.prutech.beam.utils;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prutech.beam.entity.PredictionEvent;

public class EventProcessor {
	private static final Logger LOG = Logger.getLogger(EventProcessor.class);

	public static String processEvent(String input) {
		try {
			// Parse the input JSON event
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(input);

			// Extract fields from the input event
			String prediction = rootNode.path("prediction").asText();
			String driverName = rootNode.path("driverName").asText();
			String routeName = rootNode.path("routeName").asText();
			int driverId = rootNode.path("driverId").asInt();
			int truckId = rootNode.path("truckId").asInt();
			String eventTime = rootNode.path("timeStamp").asText();
			double longitude = rootNode.path("longitude").asDouble();
			double latitude = rootNode.path("latitude").asDouble();
			String certified = rootNode.path("certified").asText();
			String wagePlan = rootNode.path("wagePlan").asText();
			int hoursLogged = rootNode.path("hours_logged").asInt();
			int milesLogged = rootNode.path("miles_logged").asInt();
			String isFoggy = rootNode.path("isFoggy").asText();
			String isRainy = rootNode.path("isRainy").asText();
			String isWindy = rootNode.path("isWindy").asText();

			String event = constructEvent(prediction, driverName, routeName, driverId, truckId, eventTime, longitude,
					latitude, certified, wagePlan, hoursLogged, milesLogged, isFoggy, isRainy, isWindy);

			// ((ObjectNode) rootNode).put("processed", true);

			// Convert the modified JSON back to a string
			String processedEvent = mapper.writeValueAsString(rootNode);

			return processedEvent;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String constructEvent(String prediction, String driverName, String routeName, int driverId,
			int truckId, String timeStamp, double longitude, double latitude, String certified, String wagePlan,
			int hours_logged, int miles_logged, String isFoggy, String isRainy, String isWindy) {

		PredictionEvent pEvent = new PredictionEvent(prediction, driverName, routeName, driverId, truckId, timeStamp,
				longitude, latitude, certified, wagePlan, hours_logged, miles_logged, isFoggy, isRainy, isWindy);

		ObjectMapper mapper = new ObjectMapper();
		String event = null;
		try {
			event = mapper.writeValueAsString(pEvent);
		} catch (Exception e) {
			LOG.error("Error converting Prediction Event to JSON");
		}
		return event;
	}

	/*
	 * Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
	 * 
	 * String inputKafkaTopic = "input-kafka-topic"; // Replace with your input
	 * Kafka topic name String outputKafkaTopic = "output-kafka-topic"; // Replace
	 * with your output Kafka topic name
	 * 
	 * pipeline.apply(KafkaIO.<String,
	 * String>read().withBootstrapServers("your-bootstrap-servers")
	 * .withTopics(Collections.singletonList(inputKafkaTopic)).withKeyDeserializer(
	 * StringDeserializer.class) .withValueDeserializer(StringDeserializer.class))
	 * .apply(MapElements.into(TypeDescriptor.of(String.class)).via(new
	 * KafkaRecordToString())).apply(ParDo.of(new
	 * KafkaEventSender("your-bootstrap-servers", outputKafkaTopic))); // Replace
	 * with
	 * 
	 * pipeline.run();
	 */

}
