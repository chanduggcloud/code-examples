package com.training.demo.compositeTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

public class Composite_v1 {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("ReadAds", TextIO.read().from("src/main/resources/source/car_ads*.csv"))
            .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
            .apply("MakePriceKVFn", ParDo.of(new MakePriceKVFn()))
            .apply("AveragePrice", Mean.perKey())
            .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    System.out.println(c.element().getKey() + ": " + c.element().getValue());
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

    private static class MakePriceKVFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            Double price = Double.parseDouble(fields[1]);

            c.output(KV.of(make, price));
        }
    }
}
