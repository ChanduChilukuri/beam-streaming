package com.prutech.beam.boltsImpl;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.prutech.beam.entity.DriverAlertNotification;
import com.prutech.beam.utils.EventMailer;

public class TruckEventRuleEngine implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2181751780608478206L;

	public static final int MAX_UNSAFE_EVENTS = 5;
	private static final Logger LOG = Logger.getLogger(TruckEventRuleEngine.class);
	private Map<Integer, LinkedList<String>> driverEvents = new HashMap<>();

	private long lastCorrelationId;

	private String email;
	private String subject;
	private EventMailer eventMailer;
	private boolean sendAlertToEmail;
	private boolean sendAlertToTopic;

	private String kafkaBootstrapServers; // Kafka broker addresses
	private String topicName; // Kafka topic name

	public TruckEventRuleEngine(Properties config) {
		this.sendAlertToEmail = Boolean.valueOf(config.getProperty("notification.email")).booleanValue();
		if (sendAlertToEmail) {
			LOG.info("TruckEventRuleEngine configured to send email on alert");
			configureEmail(config);
		} else {
			LOG.info("TruckEventRuleEngine configured to NOT send alerts");
		}

		this.sendAlertToTopic = Boolean.valueOf(config.getProperty("notification.topic")).booleanValue();

		if (sendAlertToTopic) {
			this.kafkaBootstrapServers = config.getProperty("notification.topic.bootstrap.servers");
			this.topicName = config.getProperty("notification.topic.alerts.name");
		} else {
			LOG.info("TruckEventRuleEngine configured to send alerts to Topic");
		}
	}

	public void processEvent(int driverId, String driverName, int routeId, int truckId, Timestamp eventTime,
			String event, double longitude, double latitude, long currentCorrelationId, String routeName) {
		if (lastCorrelationId != currentCorrelationId) {
			lastCorrelationId = currentCorrelationId;
			driverEvents.clear();
		}
		if (!driverEvents.containsKey(driverId)) {
			driverEvents.put(driverId, new LinkedList<>());
		}

		if (!event.equals("Normal")) {
			LinkedList<String> events = driverEvents.get(driverId);
			if (events.size() < MAX_UNSAFE_EVENTS) {
				events.add(eventTime + " " + event);
				LOG.info("Driver[" + driverId + "] " + driverName + " unsafe events: " + events.size());
			} else {
				LOG.info("Driver[" + driverId + "] has exceeded max events...");
				handleExcessiveEvents(driverId, driverName, events, truckId, eventTime.getTime(), routeId, routeName);
				events.clear();
			}
		}
	}

	private void configureEmail(Properties config) {
		try {
			eventMailer = new EventMailer();
			email = config.getProperty("notification.email.address");
			subject = config.getProperty("notification.email.subject");
			LOG.info("Initializing rule engine with email: " + email + " subject: " + subject);
		} catch (Exception e) {
			LOG.error("Error configuring email: " + e.getMessage());
		}
	}

	private void handleExcessiveEvents(int driverId, String driverName, LinkedList<String> events, int truckId,
			long timeStamp, int routeId, String routeName) {
		try {
			StringBuilder eventsMessage = new StringBuilder();
			for (String unsafeEvent : events) {
				eventsMessage.append(unsafeEvent).append("\n");
			}
			if (sendAlertToEmail) {
				sendAlertEmail(driverName, driverId, eventsMessage);
			}
			if (sendAlertToTopic) {
				sendAlertToTopic(driverName, driverId, eventsMessage, truckId, timeStamp, routeId, routeName);
			}
		} catch (Exception e) {
			LOG.error("Error handling excessive events: " + e.getMessage());
		}
	}

	private void sendAlertEmail(String driverName, int driverId, StringBuilder eventsMessage) {
		eventMailer.sendEmail(email, email, subject, "We've identified 5 unsafe driving events for Driver " + driverName
				+ " with Driver Identification Number: " + driverId + "\n\n" + eventsMessage.toString());
	}

	private void sendAlertToTopic(String driverName, int driverId, StringBuilder events, int truckId, long timeStamp,
			int routeId, String routeName) {
		String truckDriverEventKey = driverId + "|" + truckId;
		SimpleDateFormat sdf = new SimpleDateFormat();
		String timeStampString = sdf.format(timeStamp);

		String alertMessage = "5 unsafe driving events have been identified for Driver " + driverName + " with Driver "
				+ "Identification Number: " + driverId + " for Route[" + routeName + "] " + events.toString();

		DriverAlertNotification alert = new DriverAlertNotification();
		alert.setTruckDriverEventKey(truckDriverEventKey);
		alert.setDriverId(driverId);
		alert.setTruckId(truckId);
		alert.setTimeStamp(timeStamp);
		alert.setTimeStampString(timeStampString);
		alert.setAlertNotification(alertMessage);
		alert.setDriverName(driverName);
		alert.setRouteId(routeId);
		alert.setRouteName(routeName);

		String jsonAlert;
		try {
			ObjectMapper mapper = new ObjectMapper();
			jsonAlert = mapper.writeValueAsString(alert);
		} catch (Exception e) {
			LOG.error("Error converting DriverAlertNotification to JSON", e);
			return;
		}

		sendAlert(jsonAlert);
	}

	private void sendAlert(String event) {
		try (KafkaProducer<String, String> producer = createKafkaProducer()) {
			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, event);
			producer.send(record);
		} catch (Exception e) {
			LOG.error("Error sending alert to Kafka: " + e.getMessage());
		}
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaBootstrapServers);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<>(props);
	}

	public void cleanUpResources() {
		// Cleanup logic
	}

}
