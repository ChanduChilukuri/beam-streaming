package com.prutech.beam.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;

import com.prutech.beam.boltTransforms.TruckHBaseBolt;

public class TruckHBaseBoltPipeline {

	public static void buildPipeline() {
		Pipeline pipeline = Pipeline.create();

		/*
		 * CqlSession cassandraSession = CassandraConnector.connect();
		 * CassandraTableInitializer.createTablesIfNotExist(cassandraSession);
		 */
		pipeline.apply(TextIO.read().from("src/main/resources/truckhbasebolt.txt")).apply("ParseEvents",
				ParDo.of(new TruckHBaseBolt()));
		pipeline.run();

	}
}
