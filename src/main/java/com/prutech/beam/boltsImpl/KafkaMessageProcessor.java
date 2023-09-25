package com.prutech.beam.boltsImpl;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.prutech.beam.entity.PredictionEvent;

public class KafkaMessageProcessor extends DoFn<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = Logger.getLogger(KafkaMessageProcessor.class);

	@ProcessElement
	public void processElement(ProcessContext c) {
		String kafkaMessage = c.element();

		try {
			// Parse the Kafka message as a JSON object
			JsonObject json = new JsonParser().parse(kafkaMessage).getAsJsonObject();

			// Extract fields from the JSON object
			String prediction = json.get("prediction").getAsString();
			String driverName = json.get("driverName").getAsString();
			String routeName = json.get("routeName").getAsString();
			int driverId = json.get("driverId").getAsInt();
			int truckId = json.get("truckId").getAsInt();
			String eventTime = json.get("timeStamp").getAsString();
			double longitude = json.get("longitude").getAsDouble();
			double latitude = json.get("latitude").getAsDouble();
			String certified = json.get("certified").getAsString();
			String wagePlan = json.get("wagePlan").getAsString();
			int hoursLogged = json.get("hours_logged").getAsInt();
			int milesLogged = json.get("miles_logged").getAsInt();
			String isFoggy = json.get("isFoggy").getAsString();
			String isRainy = json.get("isRainy").getAsString();
			String isWindy = json.get("isWindy").getAsString();

			String event = constructEvents(prediction, driverName, routeName, driverId, truckId, eventTime, longitude,
					latitude, certified, wagePlan, hoursLogged, milesLogged, isFoggy, isRainy, isWindy);

			c.output(event);
		} catch (JsonParseException e) {
			System.err.println("Error parsing Kafka message: " + e.getMessage());
		}
	}

	public String constructEvents(String prediction, String driverName, String routeName, int driverId, int truckId,
			String timeStamp, double longitude, double latitude, String certified, String wagePlan, int hours_logged,
			int miles_logged, String isFoggy, String isRainy, String isWindy) {

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
}
