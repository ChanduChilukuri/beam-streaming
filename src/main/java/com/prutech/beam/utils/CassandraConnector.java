package com.prutech.beam.utils;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * 
 * @author chandu.ch
 *
 */
public class CassandraConnector {

	/**
	 * session creation to astra db
	 * 
	 * @return
	 */
	public static CqlSession connect() {
		Path path = Paths.get("C:/Users/chandu.ch/Downloads/secure-connect-astra-database.zip");
		CqlSession session = CqlSession.builder().withCloudSecureConnectBundle(path).withAuthCredentials(
				"KNQZCvuchWHDyEHfyyDbQvSd",
				"Qnqq.,jC3_+RmpTxAjRKK_XH9.TT6-XrP.+GxmCqAHXK.0e747gyDfaZ7BfNes-Awpfc.g3kX3tYL4IGAkm+iDlD8Zo,.lD1j66cgDQmOlPU-E9xhGM,HhJ46X846F5s")
				.withKeyspace("astra_keyspace").build();
		return session;
	}
}
