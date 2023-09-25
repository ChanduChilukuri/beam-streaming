package com.prutech.beam.utils;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCSAuthenticator {

	private final String jsonKeyFilePath;
	private final String projectId;

	private final PropertyProvider propertyProvider;

	public GCSAuthenticator(PropertyProvider propertyProvider) {
		this.propertyProvider = propertyProvider;
		this.jsonKeyFilePath = propertyProvider.getProperty("jsonKeyFilePath");
		this.projectId = propertyProvider.getProperty("projectId");
	}

	public Storage authenticate() throws IOException {
		Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyFilePath))
				.createScoped("https://www.googleapis.com/auth/cloud-platform");
		return StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
	}
}
