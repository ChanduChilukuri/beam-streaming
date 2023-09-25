package com.prutech.beam.boltTransforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.prutech.beam.entity.TruckEvent;

public class DroolsWebSocketBolt extends DoFn<String, KV<String, String>> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(DroolsWebSocketBolt.class);

	@ProcessElement
	public void processElement(@Element String line, OutputReceiver<KV<String, String>> out) {
		String[] eventParts = line.split(",");
		if (eventParts.length == 9) {
			try {
				int driverId = Integer.parseInt(eventParts[0].trim());
				String driverName = eventParts[1].trim();
				String routeName = eventParts[2].trim();
				int truckId = Integer.parseInt(eventParts[3].trim());
				String eventTime = eventParts[4].trim(); // Keep as string
				String eventType = eventParts[5].trim();
				double longitude = Double.parseDouble(eventParts[6].trim());
				double latitude = Double.parseDouble(eventParts[7].trim());

				String value = constructEvent(eventType, driverName, routeName, driverId, truckId, eventTime, longitude,
						latitude, false);

				// Emit a KV pair with a null key and the constructed value
				out.output(KV.of(driverName, value));
			} catch (NumberFormatException e) {
				LOG.error("Error parsing driverId or truckId as integers. Skipping line: " + line, e);
			}
		}
	}

	public String constructEvent(String eventType, String driverName, String routeName, int driverId, int truckId,
			String timeStamp, double longitude, double latitude, boolean raiseAlert) {
		TruckEvent eventobj = new TruckEvent(driverId, driverName, routeName, truckId, timeStamp, eventType, longitude,
				latitude, raiseAlert);

		ObjectMapper mapper = new ObjectMapper();
		String event = null;
		try {
			event = mapper.writeValueAsString(eventobj);
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
