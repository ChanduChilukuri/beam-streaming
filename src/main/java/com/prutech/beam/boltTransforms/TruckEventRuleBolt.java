package com.prutech.beam.boltTransforms;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

import com.prutech.beam.boltsImpl.TruckEventRuleEngine;

public class TruckEventRuleBolt extends DoFn<String, String> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(TruckEventRuleBolt.class);

	private TruckEventRuleEngine ruleEngine;

	public TruckEventRuleBolt(Properties kafkaConfig) {
		this.ruleEngine = new TruckEventRuleEngine(kafkaConfig);
	}

	@ProcessElement
	public void processElement(@Element String line, ProcessContext c) {
		String[] eventParts = line.split(",");
		if (eventParts.length == 10) {
			try {
				int driverId = Integer.parseInt(eventParts[0].trim());
				String driverName = eventParts[1].trim();
				int routeId = Integer.parseInt(eventParts[2].trim());
				String routeName = eventParts[3].trim();
				int truckId = Integer.parseInt(eventParts[4].trim());
				String eventTime = eventParts[5].trim();
				java.sql.Timestamp eventTimes = convert(eventTime);
				String eventType = eventParts[6].trim();
				double longitude = Double.parseDouble(eventParts[7].trim());
				double latitude = Double.parseDouble(eventParts[8].trim());
				long correlationId = Long.parseLong(eventParts[9].trim());

				LOG.info("Processing truck event[" + eventType + "]  for driverId[" + driverId + "], truck[" + truckId
						+ "], " + "route[" + routeName + "], correlationId[" + correlationId + "]");
				ruleEngine.processEvent(driverId, driverName, routeId, truckId, eventTimes, eventType, longitude,
						latitude, correlationId, routeName);
				c.output("rule engine executed");
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

}
