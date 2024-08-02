package com.training.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.*;

public class PaymentTypeProcessing {

    public static void main(String[] args) {

        final List<String> LINES = Arrays.asList(
                "1/5/09 5:39,Shoes,1200,Amex,Netherlands",
                "1/2/09 9:16,Jacket,1200,Mastercard,United States",
                "1/5/09 10:08,Phone,3600,Visa,United States",
                "1/2/09 14:18,Shoes,1200,Visa,United States",
                "1/4/09 1:05,Phone,3600,Diners,Ireland",
                "1/5/09 11:37,Books,1200,Visa,Canada");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(ParDo.of(new PrintToConsoleFn()))
                .apply(ParDo.of(new ExtractPaymentTypeFn()))
                .apply(ParDo.of(new PrintToConsoleFn()));

        pipeline.run();

        System.out.println("Pipeline execution complete!");
    }

    public static class ExtractPaymentTypeFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] tokens = c.element().split(",");
            if (tokens.length >= 4) {
                c.output(tokens[3]);
            }
        }
    }

    public static class PrintToConsoleFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());

            c.output(c.element());
        }
    }
}

