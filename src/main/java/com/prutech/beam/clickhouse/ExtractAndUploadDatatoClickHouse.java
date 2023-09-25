package com.prutech.beam.clickhouse;

import org.apache.beam.sdk.transforms.DoFn;

import com.prutech.beam.entity.DriverAlertNotification;

/**
 * 
 * @author chandu.ch
 *
 */
public class ExtractAndUploadDatatoClickHouse extends DoFn<String, DriverAlertNotification> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 956399112230692668L;

	@ProcessElement
	public void processElement(@Element String row, OutputReceiver<DriverAlertNotification> receiver) {
		String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

		// Skips over the header row
		if (!fields[0].equals("driverId")) {
			receiver.output(new DriverAlertNotification());
		}
	}
}
