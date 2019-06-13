package fr.marouni.apache.beam.io;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class MyDoFnIO {
    static class Read extends PTransform<PBegin, PCollection<String>> {

        @Override
        public PCollection<String> expand(PBegin input) {
            return null;
            //input.apply(ParDo.of(new StringSource()));
        }

        public static class StringSource extends DoFn<PBegin, String> {

            @ProcessElement
            public void processElement(ProcessContext c) {
                c.output("XXXXX");
            }
        }
    }
}
