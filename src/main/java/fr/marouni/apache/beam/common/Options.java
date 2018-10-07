package fr.marouni.apache.beam.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public class Options {
    /**
     * Options that can be used to configure BigQuery tables in Beam examples.
     * The project defaults to the project being used to run the example.
     */
    public static interface ExampleBigQueryTableOptions extends GcpOptions {
      @Description("BigQuery dataset name")
      @Default.String("beam_examples")
      String getBigQueryDataset();
      void setBigQueryDataset(String dataset);

      @Description("BigQuery table name")
      @Default.InstanceFactory(BigQueryTableFactory.class)
      String getBigQueryTable();
      void setBigQueryTable(String table);

      @Description("BigQuery table schema")
      TableSchema getBigQuerySchema();
      void setBigQuerySchema(TableSchema schema);

      /**
       * Returns the job name as the default BigQuery table name.
       */
      class BigQueryTableFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
          return options.getJobName().replace('-', '_');
        }
      }
    }

    /**
     * Options that can be used to configure the Beam examples.
     */
    public static interface ExampleOptions extends PipelineOptions {
      @Description("Whether to keep jobs running after local process exit")
      @Default.Boolean(false)
      boolean getKeepJobsRunning();
      void setKeepJobsRunning(boolean keepJobsRunning);

      @Description("Number of workers to use when executing the injector pipeline")
      @Default.Integer(1)
      int getInjectorNumWorkers();
      void setInjectorNumWorkers(int numWorkers);
    }

    /**
     * Options that can be used to configure Pub/Sub topic/subscription in Beam examples.
     */
    public static interface ExamplePubsubTopicAndSubscriptionOptions extends ExamplePubsubTopicOptions {
      @Description("Pub/Sub subscription")
      @Default.InstanceFactory(PubsubSubscriptionFactory.class)
      String getPubsubSubscription();
      void setPubsubSubscription(String subscription);

      /**
       * Returns a default Pub/Sub subscription based on the project and the job names.
       */
      class PubsubSubscriptionFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
          return "projects/" + options.as(GcpOptions.class).getProject()
              + "/subscriptions/" + options.getJobName();
        }
      }
    }

    /**
     * Options that can be used to configure Pub/Sub topic in Beam examples.
     */
    public static interface ExamplePubsubTopicOptions extends GcpOptions {
      @Description("Pub/Sub topic")
      @Default.InstanceFactory(PubsubTopicFactory.class)
      String getPubsubTopic();
      void setPubsubTopic(String topic);

      /**
       * Returns a default Pub/Sub topic based on the project and the job names.
       */
      class PubsubTopicFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
          return "projects/" + options.as(GcpOptions.class).getProject()
              + "/topics/" + options.getJobName();
        }
      }
    }
}
