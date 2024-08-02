package com.training.demo.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.api.services.bigquery.model.TableRow;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(JUnit4.class)
public class BigQueryToPubsubTest {
	
	@Rule
    public final transient TestPipeline tpipeline = TestPipeline.create();
	
	PipelineOptions options = PipelineOptionsFactory.create();
	Pipeline pipeline = Pipeline.create(options);
	
	List<TableRow> inputRecords = Arrays.asList(
            new TableRow().set("message", "message4").set("replayTopic", "replayTopic4"),
            new TableRow().set("message", "message3").set("replayTopic", "replayTopic3")
    );

    @Test
    public void testBigQueryToGcs() {
    	log.info("### testBigQueryToGcs(): START");
    	PCollection<TableRow> rows = pipeline.apply(Create.of(inputRecords)).setCoder(TableRowJsonCoder.of());
    	
    	log.info("### transformToPubsubMessage(): START");
        PCollection<PubsubMessage> output = BigQueryToPubsub.transformPubsubMessage(rows);
        
        // Assert the output
        //PCollection<String> successOutput = output.get(BigQueryToGcs.SUCCESS_TAG);
        //PCollection<String> failureOutput = output.get(BigQueryToGcs.FAILURE_TAG);

        PAssert.that(output).containsInAnyOrder(prepareExpectedResult());

        pipeline.run().waitUntilFinish();
        log.info("### testBigQueryToGcs(): END");
    }
    
    private PubsubMessage[] prepareExpectedResult() {
    	
    	
    	PubsubMessage pubsubMessage1 = new PubsubMessage(
    					Objects.requireNonNull("message4").toString().getBytes(StandardCharsets.UTF_8),
    					Collections.emptyMap())
    					.withTopic(Objects.requireNonNull("replayTopic4"));
    	PubsubMessage pubsubMessage2 = new PubsubMessage(
    							Objects.requireNonNull("message3").toString().getBytes(StandardCharsets.UTF_8),
    							Collections.emptyMap())
    							.withTopic(Objects.requireNonNull("replayTopic3"));
    	
        PubsubMessage[] expectedMessages = {pubsubMessage1, pubsubMessage2};
        
    	return expectedMessages;
    }
    
}