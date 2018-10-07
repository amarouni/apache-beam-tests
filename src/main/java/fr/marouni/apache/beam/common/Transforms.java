package fr.marouni.apache.beam.common;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.firstNonNull;

public class Transforms {
    public static class CountWords extends PTransform<PCollection<String>,
        PCollection<KV<String, Long>>> {
      @Override
      public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

        // Convert lines of text into individual words.
        PCollection<String> words = lines.apply(
            ParDo.of(new DoFns.ExtractWordsFn()));

        // Count the number of times each word occurs.
        PCollection<KV<String, Long>> wordCounts =
            words.apply(Count.<String>perElement());

        return wordCounts;
      }
    }

    /**
     * A {@link DoFn} that writes elements to files with names deterministically derived from the lower
     * and upper bounds of their key (an {@link IntervalWindow}).
     *
     * <p>This is test utility code, not for end-users, so examples can be focused on their primary
     * lessons.
     */
    public static class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {
      private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
      private String filenamePrefix;
      @Nullable
      private Integer numShards;

      public WriteOneFilePerWindow(String filenamePrefix, Integer numShards) {
        this.filenamePrefix = filenamePrefix;
        this.numShards = numShards;
      }

      @Override
      public PDone expand(PCollection<String> input) {
        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
        TextIO.Write write =
            TextIO.write()
                .to(new PerWindowFiles(resource))
                .withTempDirectory(resource.getCurrentDirectory())
                .withWindowedWrites();
        if (numShards != null) {
          write = write.withNumShards(numShards);
        }
        return input.apply(write);
      }

      /**
       * A {@link FileBasedSink.FilenamePolicy} produces a base file name for a write based on metadata about the data
       * being written. This always includes the shard number and the total number of shards. For
       * windowed writes, it also includes the window and pane index (a sequence number assigned to each
       * trigger firing).
       */
      public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

        private final ResourceId baseFilename;

        public PerWindowFiles(ResourceId baseFilename) {
          this.baseFilename = baseFilename;
        }

        public String filenamePrefixForWindow(IntervalWindow window) {
          String prefix =
              baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
          return String.format("%s-%s-%s",
              prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
        }

        @Override
        public ResourceId windowedFilename(int shardNumber,
                                           int numShards,
                                           BoundedWindow window,
                                           PaneInfo paneInfo,
                                           FileBasedSink.OutputFileHints outputFileHints) {
          IntervalWindow intervalWindow = (IntervalWindow) window;
          String filename =
              String.format(
                  "%s-%s-of-%s%s",
                  filenamePrefixForWindow(intervalWindow),
                  shardNumber,
                  numShards,
                  outputFileHints.getSuggestedFilenameSuffix());
          return baseFilename
              .getCurrentDirectory()
              .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(
            int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
          throw new UnsupportedOperationException("Unsupported.");
        }
      }
    }

    public static class KeyAppender extends PTransform<PCollection<String>, PCollection<KV<Integer, String>>> {

        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFns.GetLineLengthFromString()));
        }
    }
}
