package com.training.demo.schema.inference;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class SchemaDefinitionAndProcessing_v2 {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<SalesRecord> salesRecords = readSalesData(pipeline);

        System.out.println("Has schema? " + salesRecords.hasSchema());
        System.out.println("Schema: " + salesRecords.getSchema());

        salesRecords.apply(MapElements.via(new SimpleFunction<SalesRecord, Void>() {
            @Override
            public Void apply (SalesRecord input){
                System.out.println(input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }

    private static PCollection<SalesRecord> readSalesData(Pipeline pipeline) {

        SalesRecord record1 = new SalesRecord(
                "1/5/09 5:39", "Shoes",
                120, "Amex", "Netherlands");
        SalesRecord record2 = new SalesRecord(
                "2/2/09 9:16", "Jeans",
                110, "Mastercard", "United States");
        SalesRecord record3 = new SalesRecord(
                "3/5/09 10:08", "Pens",
                10, "Visa", "United States");
        SalesRecord record4 = new SalesRecord(
                "4/2/09 14:18", "Shoes",
                303, "Visa", "United States");
        SalesRecord record5 = new SalesRecord(
                "5/4/09 1:05", "iPhone",
                1240, "Diners", "Ireland");
        SalesRecord record6 = new SalesRecord(
                "6/5/09 11:37", "TV",
                1503, "Visa", "Canada");

        return pipeline.apply(Create.of(record1, record2, record3, record4, record5, record6));
    }
}
