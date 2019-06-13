package fr.marouni.apache.beam.io;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The bounded source that generates random strings.
 */
public class RandomStringGeneratorSource extends BoundedSource<String> {

    final private RandomStringGeneratorIO.Read spec;
    final private int sourceSize;
    private final int UUID_SIZE = 36;

    /**
     * Init my source
     * @param spec The Read specifications, including the size
     */
    RandomStringGeneratorSource(RandomStringGeneratorIO.Read spec, int sourceSize) {
        this.spec = spec;
        this.sourceSize = sourceSize;
    }

    @Override
    public Coder<String> getOutputCoder() {
        return SerializableCoder.of(String.class);
    }

    /**
     * Here we're splitting into 2 sources regardless of the estimated size
     */
    @Override
    public List<? extends BoundedSource<String>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        int reader1 = sourceSize/2;
        int reader2 = sourceSize/2;
        if(sourceSize % 2 == 0) reader2 = reader2 + 1;
        return Arrays.asList(new RandomStringGeneratorSource(spec, reader1),
                new RandomStringGeneratorSource(spec, reader2));
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return spec.getSize() * UUID_SIZE;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
        return new RandomStringReader(this, sourceSize);
    }
}
