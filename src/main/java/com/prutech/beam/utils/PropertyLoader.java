package com.prutech.beam.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyLoader implements PropertyProvider {

	private final Properties properties;

	public PropertyLoader() {
		properties = new Properties();
		try {
			FileInputStream inputStream = new FileInputStream(
					"C:\\Users\\chandu.ch\\Desktop\\scylla examples\\beam-streaming\\src\\main\\resources\\config.properties");
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
