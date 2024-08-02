package com.training.demo.coGroupByKey;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Joining_v1 {

    private static final String CSV_INFO_HEADER = "CustomerID,Gender,Age,Annual_Income";
    private static final String CSV_SCORE_HEADER = "CustomerID,Spending Score";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Integer>> customersIncome = pipeline
                .apply(TextIO.read().from("src/main/resources/source/mall_customers_info.csv"))
                .apply("FilterInfoHeader", ParDo.of(new FilterHeaderFn(CSV_INFO_HEADER)))
                .apply("IdIncomeKV", ParDo.of(new IdIncomeKVFn()));

        customersIncome.apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("**Income " + c.element().getKey() + ": " + c.element().getValue());
            }
        }));

        PCollection<KV<String, Integer>> customersScore = pipeline
                .apply(TextIO.read().from("src/main/resources/source/mall_customers_score.csv"))
                .apply("FilterScoreHeader", ParDo.of(new FilterHeaderFn(CSV_SCORE_HEADER)))
                .apply("IdScoreKV", ParDo.of(new IdScoreKVFn()));

        customersScore.apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println("--Score " + c.element().getKey() + ": " + c.element().getValue());
            }
        }));

        pipeline.run().waitUntilFinish();
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

    private static class IdIncomeKVFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, Integer>> out) {
            String[] fields = element.split(",");

            String id = fields[0];
            int income = Integer.parseInt(fields[3]);

            out.output(KV.of(id, income));
        }
    }

    private static class IdScoreKVFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(
                @Element String element,
                OutputReceiver<KV<String, Integer>> out) {
            String[] fields = element.split(",");

            String id = fields[0];
            int score = Integer.parseInt(fields[1]);

            out.output(KV.of(id, score));
        }
    }

}
