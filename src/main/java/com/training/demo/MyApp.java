package com.training.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class MyApp {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		System.out.println("Apache Beam app");
		PCollection<String> output = pipeline.apply(TextIO.read().from("./src/main/resources/source/input.csv"));

		output.apply(TextIO.write().to("./src/main/resources/source/output.csv").withNumShards(1).withSuffix(".csv"));

		pipeline.run();
	}

}
