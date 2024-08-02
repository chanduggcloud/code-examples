package com.training.demo.functions;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableRow;

public class TransformPubsubMessage extends DoFn<TableRow, PubsubMessage> implements Serializable {


	private static final long serialVersionUID = 1L;

	@ProcessElement
    public void processElement(ProcessContext c) {
		TableRow row = c.element();
		
		System.out.println("### -> message : " + row.get("message") + " ; ---> replayTopic : " + row.get("replayTopic").toString());
		
		PubsubMessage message = new PubsubMessage(
				Objects.requireNonNull(row.get("message")).toString().getBytes(StandardCharsets.UTF_8),
				Collections.emptyMap())
				.withTopic(Objects.requireNonNull(row.get("replayTopic").toString()));
    	
    	c.output(message);
    }
}
