package com.training.demo.pardoDoFn;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.util.Calendar;

public class Computations {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("src/main/resources/source/car_ads*.csv"))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("MakeAgeKVFn", ParDo.of(new MakeAgeKVFn()))
                .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
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

    private static class MakeAgeKVFn extends DoFn<String, KV<String, Integer>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            int currentYear = Calendar.getInstance().get(Calendar.YEAR);

            int age = currentYear - Integer.parseInt(fields[7]);

            c.output(KV.of(make, age));
        }
    }
}
