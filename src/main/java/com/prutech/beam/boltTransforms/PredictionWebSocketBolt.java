package com.prutech.beam.boltTransforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.prutech.beam.entity.PredictionEvent;

public class PredictionWebSocketBolt extends DoFn<String, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(PredictionWebSocketBolt.class);

	@ProcessElement
	public void processElement(@Element String line, OutputReceiver<KV<String, String>> out) {
		String[] eventParts = line.split(",");
		if (eventParts.length == 9) {
			try {

				String prediction = eventParts[0].trim();
				String driverName = eventParts[1].trim();
				String routeName = eventParts[2].trim();
				int driverId = Integer.parseInt(eventParts[3].trim());
				int truckId = Integer.parseInt(eventParts[4].trim());
				String eventTime = eventParts[5].trim();
				double longitude = Double.parseDouble(eventParts[6].trim());
				double latitude = Double.parseDouble(eventParts[7].trim());
				String certified = eventParts[8].trim();
				String wagePlan = eventParts[9].trim();
				int hours_logged = Integer.parseInt(eventParts[10].trim());
				int miles_logged = Integer.parseInt(eventParts[11].trim());
				String isFoggy = eventParts[12].trim();
				String isRainy = eventParts[13].trim();
				String isWindy = eventParts[14].trim();

				String event = constructEvent(prediction, driverName, routeName, driverId, truckId, eventTime,
						longitude, latitude, certified, wagePlan, hours_logged, miles_logged, isFoggy, isRainy,
						isWindy);
				// Emit a KV pair with a null key and the constructed value
				out.output(KV.of(driverName, event));
			} catch (NumberFormatException e) {
				LOG.error("Error parsing driverId or truckId as integers. Skipping line: " + line, e);
			}
		}
	}

	public String constructEvent(String prediction, String driverName, String routeName, int driverId, int truckId,
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

	/*
	 * PipelineOptions options = PipelineOptionsFactory.create(); Pipeline pipeline
	 * = Pipeline.create(options); pipeline.apply("ReadFromTextFile",
	 * TextIO.read().from("src/main/resources/input.txt"))
	 * .apply("TransformToBeamEvents", ParDo.of(new DroolsWebSocketBolt()))
	 * .apply(KafkaIO.<String, String>write().withBootstrapServers("localhost:9092")
	 * .withTopic("driver_alerts_topic").withKeySerializer(StringSerializer.class)
	 * .withValueSerializer(StringSerializer.class));
	 * 
	 * pipeline.run();
	 */

}
