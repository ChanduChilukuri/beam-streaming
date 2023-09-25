package com.prutech.beam.boltTransforms;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.beam.sdk.transforms.DoFn;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.prutech.beam.entity.TruckEvent;

public class DroolsBolt extends DoFn<String, String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient KieSession kSession;

	@Setup
	public void setup() {
		// Load Drools rules from "ViolationsRules.drl" during setup
		KieServices ks = KieServices.Factory.get();
		KieContainer kContainer = ks.getKieClasspathContainer();
		kSession = kContainer.newKieSession();
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		// Extract event data (assuming a comma-separated format)
		String eventString = c.element();
		String[] eventParts = eventString.split(",");

		if (eventParts.length == 8) {
			// Assuming there are 8 fields, parse the fields from eventParts
			String eventType = eventParts[5].trim();
			String driverName = eventParts[1].trim();
			String routeName = eventParts[2].trim();
			int truckId = Integer.parseInt(eventParts[3].trim());
			String eventTime = eventParts[4].trim();
			double longitude = Double.parseDouble(eventParts[6].trim());
			double latitude = Double.parseDouble(eventParts[7].trim());
			int driverId = Integer.parseInt(eventParts[0].trim());

			SimpleDateFormat sdf = new SimpleDateFormat();

			TruckEvent truckEvent = new TruckEvent(driverId, driverName, routeName, truckId, sdf.format(new Date()),
					eventType, longitude, latitude, false);
			// Applying Drools rules
			kSession.insert(truckEvent);
			kSession.fireAllRules();
			// Emit the processed event
			c.output(truckEvent.toString());
		} else {
			System.out.println("null");
		}
	}

	@Teardown
	public void teardown() {
		if (kSession != null) {
			kSession.dispose();
		}
	}

	/*
	 * PipelineOptions options = PipelineOptionsFactory.create(); Pipeline pipeline
	 * = Pipeline.create(options);
	 * pipeline.apply(TextIO.read().from("src/main/resources/input.txt")).apply(
	 * ParDo.of(new DroolsBolt()))
	 * .apply(TextIO.write().to("output.txt").withoutSharding()); pipeline.run();
	 */
}
