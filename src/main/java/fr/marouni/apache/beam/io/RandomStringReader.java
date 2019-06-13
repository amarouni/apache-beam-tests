package fr.marouni.apache.beam.io;

import org.apache.beam.sdk.io.BoundedSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Random string generator source
 */
class RandomStringReader extends BoundedSource.BoundedReader<String> {

    private final RandomStringGeneratorSource source;
    private String current;
    private RandomStringIterator randomStringIterator;

    RandomStringReader(RandomStringGeneratorSource randomStringGeneratorSource, int sourceSize){
        this.source = randomStringGeneratorSource;
        this.randomStringIterator = new RandomStringIterator(sourceSize);
    }

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean advance() {
        if(randomStringIterator.hasNext()) {
            current = randomStringIterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
        if (current == null) advance();
        return current;

    }

    @Override
    public void close() {
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
        return this.source;
    }

    /**
     * The real string source that generates random UUIDs
     */
    static class RandomStringIterator implements Iterator<String> {

        int[] range;
        List<String> internalStringSource;
        int index;
        int sourceSize;

        RandomStringIterator(int sourceSize){
            this.sourceSize = sourceSize;
            this.internalStringSource = new ArrayList<>();
            for(int i = 0; i< sourceSize; i++){
                this.internalStringSource.add(UUID.randomUUID().toString());
            }
            this.index = sourceSize;
        }

        @Override
        public boolean hasNext() {
            return index > 0;
        }

        @Override
        public String next() {
            String string = internalStringSource.get(index - 1);
            index--;
            return string;
        }
    }
}