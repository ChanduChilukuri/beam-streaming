package com.prutech.beam.boltTransforms;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.log4j.Logger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.prutech.beam.utils.CassandraConnector;

/**
 * 
 * @author chandu.ch
 *
 */
public class TruckHBaseBolt extends DoFn<String, Void> {
	//
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(TruckHBaseBolt.class);

	/**
	 * 
	 * @param line
	 * @param c
	 */
	@ProcessElement
	public void processElement(@Element String line, ProcessContext c) {
		String[] eventParts = line.split(",");
		if (eventParts.length == 9) {
			try {

				int driverId = Integer.parseInt(eventParts[0].trim());
				int truckId = Integer.parseInt(eventParts[1].trim());
				String eventTimes = eventParts[2].trim();
				String eventType = eventParts[3].trim();
				double longitude = Double.parseDouble(eventParts[4].trim());
				double latitude = Double.parseDouble(eventParts[5].trim());
				String driverName = eventParts[6].trim();
				int routeId = Integer.parseInt(eventParts[7].trim());
				String routeName = eventParts[8].trim();
				/*
				 * System.out.println(driverId + ":" + truckId + ":" + eventTimes + ":" +
				 * eventType + ":" + latitude + ":" + longitude + ":" + driverName + ":" +
				 * routeId + ":" + routeName);
				 */
				CqlSession cassandraSession = CassandraConnector.connect();

				if (shouldInsertIntoDangerousEvents(eventType)) {
					insertDangerousEvent(cassandraSession, driverId, truckId, eventTimes, eventType, longitude,
							latitude, driverName, routeId, routeName);
					// System.out.println("inserted data into dangerous_events_table");
					// need to get the number of violations done by the driver from violation table
					updateEventCount(cassandraSession, driverId, 1);
					// System.out.println("updated driver_dangerous_events_count table");
				}
				if (persistAllEvents()) {
					insertAllEvent(cassandraSession, driverId, truckId, eventTimes, eventType, longitude, latitude,
							driverName, routeId, routeName);
					// System.out.println("inserted in to driver_events table");
				}

			} catch (NumberFormatException e) {
				LOG.error("Error parsing driverId or truckId as integers. Skipping line: " + line, e);
			}
		}
	}

	private boolean shouldInsertIntoDangerousEvents(String eventtype) {
		// System.out.println(eventtype);
		return !eventtype.equals("Normal");
	}

	// need to get it from config file. for now setting to static
	private boolean persistAllEvents() {
		return true;
	}

	/**
	 * 
	 * @param cassandraSession
	 * @param driverId
	 * @param incidentRunningTotal
	 */
	private void updateEventCount(CqlSession cassandraSession, int driverId, long incidentRunningTotal) {

		String updateQuery = "UPDATE driver_violation_count SET incidentRunningTotal = incidentRunningTotal + ? WHERE driverId = ?;";
		PreparedStatement preparedStatement = cassandraSession.prepare(updateQuery);
		BoundStatement boundStatement = preparedStatement.bind(incidentRunningTotal, driverId);

		cassandraSession.execute(boundStatement);
	}

	/**
	 * 
	 * @param cassandraSession
	 * @param driverId
	 * @param truckId
	 * @param eventTime
	 * @param eventType
	 * @param latitude
	 * @param longitude
	 * @param driverName
	 * @param routeId
	 * @param routeName
	 */
	private void insertAllEvent(CqlSession cassandraSession, int driverId, int truckId, String eventTime,
			String eventType, double latitude, double longitude, String driverName, int routeId, String routeName) {
		String insertQuery = "INSERT INTO all_driver_events(id,driverId, truckId, eventTime, eventType, latitude, longitude, driverName, routeId, routeName) "
				+ "VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?);";

		UUID id = UUID.randomUUID();
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		LocalDateTime localDateTime = LocalDateTime.parse(eventTime, dateTimeFormatter);

		// Convert LocalDateTime to Instant
		Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
		PreparedStatement preparedStatement = cassandraSession.prepare(insertQuery);
		BoundStatement boundStatement = preparedStatement.bind(id, driverId, truckId, instant, eventType, latitude,
				longitude, driverName, routeId, routeName);

		cassandraSession.execute(boundStatement);
	}

	/**
	 * 
	 * @param cassandraSession
	 * @param driverId
	 * @param truckId
	 * @param eventTime
	 * @param eventType
	 * @param latitude
	 * @param longitude
	 * @param driverName
	 * @param routeId
	 * @param routeName
	 */
	private void insertDangerousEvent(CqlSession cassandraSession, int driverId, int truckId, String eventTime,
			String eventType, double latitude, double longitude, String driverName, int routeId, String routeName) {
		String insertQuery = "INSERT INTO violation_Events(id,driverId, truckId, eventTime, eventType, latitude, longitude, driverName, routeId, routeName) "
				+ "VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?);";
		UUID id = UUID.randomUUID();
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		LocalDateTime localDateTime = LocalDateTime.parse(eventTime, dateTimeFormatter);

		// Convert LocalDateTime to Instant
		Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

		PreparedStatement preparedStatement = cassandraSession.prepare(insertQuery);
		BoundStatement boundStatement = preparedStatement.bind(id, driverId, truckId, instant, eventType, latitude,
				longitude, driverName, routeId, routeName);

		cassandraSession.execute(boundStatement);
	}

}
