package fr.marouni.apache.beam.io;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public class RandomStringGeneratorIO {

    /**
     * The entry point to my IO. Here my Read implements a PTransform that takes a PBegin and returns
     * a PCollection of strings.
     *
     * My IO will generate random strings, the number of strings generated is determined by the size parameter
     * passed to my IO.
     */
    static class Read extends PTransform<PBegin, PCollection<String>> {

        /**
         * number of random strings generated
         */
        private int size;

        /**
         * Init my Read IO with a size parameter
         *
         */
        Read(int size) {
            this.size = size;
        }


        public int getSize(){
            return this.size;
        }

        /**
         * The main entry point to my IO
         * @param input The pipeline PBegin
         * @return A PCollection of strings
         */
        @Override
        public PCollection<String> expand(PBegin input) {
            return input.apply("", org.apache.beam.sdk.io.Read.from(new RandomStringGeneratorSource(this, size)));
        }
    }
}
