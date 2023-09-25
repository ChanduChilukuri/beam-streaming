package com.prutech.beam.pipelines;

import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;

import com.prutech.beam.boltTransforms.TruckEventRuleBolt;
import com.prutech.beam.utils.PropertyProvider;

public class TruckEventRuleBoltPipeline {

	public static void buildPipeline(PropertyProvider propertyProvider) {
		Pipeline pipeline = Pipeline.create();

		pipeline.apply(TextIO.read().from("src/main/resources/truckeventrulebolt.txt")).apply("ParseEvents",
				ParDo.of(new TruckEventRuleBolt(new Properties())));
		pipeline.run();

	}
}
