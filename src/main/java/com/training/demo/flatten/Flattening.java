package com.training.demo.flatten;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Flattening {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pCollection1 = pipeline.apply("ReadAds",
                TextIO.read().from("src/main/resources/source/car_ads_1.csv"));
        PCollection<String> pCollection2 = pipeline.apply("ReadAds",
                TextIO.read().from("src/main/resources/source/car_ads_2.csv"));
        PCollection<String> pCollection3 = pipeline.apply("ReadAds",
                TextIO.read().from("src/main/resources/source/car_ads_3.csv"));

        PCollectionList<String> pCollectionList = PCollectionList.of(pCollection1)
                .and(pCollection2).and(pCollection3);

        PCollection<String> flattenedCollection = pCollectionList.apply(Flatten.pCollections());

        flattenedCollection
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("MakeModelKVFn", ParDo.of(new MakeModelKVFn()))
                .apply("CountModels", Count.perKey())
                .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Long>, Void>() {
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

    private static class MakeModelKVFn extends DoFn<String, KV<String, String>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            String model = fields[8];

            c.output(KV.of(make, model));
        }
    }
}
