package com.prutech.beam.boltTransforms;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.prutech.beam.entity.TruckDriverViolationEvent;

public class WebSocketBolt extends DoFn<String, KV<String, String>> {

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
				String eventTime = eventParts[4].trim();

				String eventType = eventParts[5].trim();
				double longitude = Double.parseDouble(eventParts[6].trim());
				double latitude = Double.parseDouble(eventParts[7].trim());

				long numberOfInfractions = Long.parseLong(eventParts[8].trim());
				int routeId = Integer.parseInt(eventParts[9].trim());

				java.sql.Timestamp eventTimes = convert(eventTime);
				long eventTimeLong = eventTimes.getTime();

				String value = constructEvents(driverId, truckId, eventTimeLong, eventTime, eventType, longitude,
						latitude, numberOfInfractions, driverName, routeId, routeName);

				String kafkaTopic;
				if ("Normal".equals(eventType)) {
					kafkaTopic = "truck_event";
				} else if ("Speeding".equals(eventType)) {
					kafkaTopic = "driver_alerts_topic";
				} else {
					kafkaTopic = "other_events_topic";
				}

				// Emit a KV pair with the determined Kafka topic as the key and the value
				out.output(KV.of(kafkaTopic, value));

			} catch (NumberFormatException e) {
				LOG.error("Error parsing driverId or truckId as integers. Skipping line: " + line, e);
			}
		}
	}

	public java.sql.Timestamp convert(String time) {
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
			Date parsedDate = dateFormat.parse(time);
			java.sql.Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
			return timestamp;
		} catch (Exception e) {
			LOG.error("error converting" + time);
		}
		return null;
	}

	public String constructEvents(int driverId, int truckId, long eventTimeLong, String timeStampString,
			String eventType, double longitude, double latitude, long numberOfInfractions, String driverName,
			int routeId, String routeName) {

		String truckDriverEventKey = driverId + "|" + truckId;
		TruckDriverViolationEvent driverInfraction = new TruckDriverViolationEvent(truckDriverEventKey, driverId,
				truckId, eventTimeLong, timeStampString, longitude, latitude, eventType, numberOfInfractions,
				driverName, routeId, routeName);
		ObjectMapper mapper = new ObjectMapper();
		String event = null;
		try {
			event = mapper.writeValueAsString(driverInfraction);
		} catch (Exception e) {
			LOG.error("Error converting TruckDriverViolationEvent to JSON");
		}
		return event;
	}

	/*
	 * PipelineOptions options = PipelineOptionsFactory.create(); Pipeline pipeline
	 * = Pipeline.create(options);
	 * 
	 * pipeline.apply("ReadFromTextFile",
	 * TextIO.read().from("src/main/resources/infraction.txt"))
	 * .apply("TransformToBeamEvents", ParDo.of(new WebSocketBolt()))
	 * .apply("kafka Producer", ParDo.of(new KafkaPublisher())); pipeline.run();
	 */
}
