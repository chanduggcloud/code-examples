package com.training.demo.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class SqlTransformTemplate {

	public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> movieTags = pipeline
                .apply("ReadMovieTags", TextIO.read().from("src/main/resources/source/data/windowing/tags_sample.csv"));
		

        
	}

}
