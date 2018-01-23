
import readers.EnhancedResourcesItemReader;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DOMHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import java.util.UUID;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;

/**
 * YourJobConfig.java - a Spring Batch configuration template that demonstrates
 * ingesting data into MarkLogic. This job specification uses the
 * MarkLogicBatchConfiguration that utilizes a MarkLogic implementation of a
 * JobRepository.
 *
 * @author Scott Stafford
 * @version 1.4.0
 * @see EnableBatchProcessing
 * @see com.marklogic.spring.batch.config.MarkLogicBatchConfiguration
 * @see com.marklogic.spring.batch.config.MarkLogicConfiguration
 * @see com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor
 * @see com.marklogic.spring.batch.item.writer.MarkLogicItemWriter
 */
@EnableBatchProcessing
@Import(value = {
    com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class,
    com.marklogic.spring.batch.config.MarkLogicConfiguration.class})
@PropertySource("classpath:job.properties")
public class MyImportDocsExtractTextJob {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // This is the bean label for the name of your Job.  Pass this label into the job_id parameter
    // when using the CommandLineJobRunner
    private final String JOB_NAME = "mydirectoryJob";

    /**
     * The JobBuilderFactory and Step parameters are injected via the
     * EnableBatchProcessing annotation.
     *
     * @param jobBuilderFactory injected from the @EnableBatchProcessing
     * annotation
     * @param step injected from the step method in this class
     * @return Job
     */
    @Bean(name = JOB_NAME)
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        JobExecutionListener listener = new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("BEFORE JOB");
                jobExecution.getExecutionContext().putString("random", "yourJob123");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                logger.info("AFTER JOB");
            }
        };

        return jobBuilderFactory.get(JOB_NAME)
                .start(step)
                .listener(listener)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    /**
     * The StepBuilderFactory and DatabaseClientProvider parameters are injected
     * via Spring. Custom parameters must be annotated with @Value.
     *
     * @param stepBuilderFactory injected from the @EnableBatchProcessing
     * annotation
     * @param databaseClientProvider injected from the BasicConfig class
     * @param collections This is an example of how user parameters could be
     * injected via command line or a properties file
     * @return Step
     * @see DatabaseClientProvider
     * @see ItemReader
     * @see ItemProcessor
     * @see DocumentWriteOperation
     * @see MarkLogicItemProcessor
     * @see MarkLogicItemWriter
     */
    @Bean
    @JobScope
    public Step step(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider,
            @Value("#{jobParameters['input_file_path']}") String inputFilePath,
            @Value("#{jobParameters['input_file_pattern']}") String inputFilePattern) {

        ItemProcessor<Resource, DocumentWriteOperation> processor = new ItemProcessor<Resource, DocumentWriteOperation>() {
            @Override
            public DocumentWriteOperation process(Resource item) throws Exception {
                ContentHandler handler = new ToXMLContentHandler();

                AutoDetectParser parser = new AutoDetectParser();
                Metadata metadata = new Metadata();

                logger.info("processing: " + item.getFilename());

                InputStream stream = item.getInputStream();

                parser.parse(stream, handler, metadata);

                logger.info("content found: " + handler.toString());

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();

                Document document = builder.parse(new InputSource(new StringReader(handler.toString())));
                document.setDocumentURI(item.getFilename());
                DOMHandle handle = new DOMHandle(document);
                handle.setFormat(Format.XML);
                return new DocumentWriteOperationImpl(
                        DocumentWriteOperation.OperationType.DOCUMENT_WRITE,
                        item.getFilename() + ".xml",
                        new DocumentMetadataHandle().withCollections("binary"),
                        handle);
            }
        };

        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();

        MarkLogicItemWriter writer = new MarkLogicItemWriter(databaseClient);
        writer.setBatchSize(10);

        return stepBuilderFactory.get("step1")
                .<Resource, DocumentWriteOperation>chunk(10)
                .reader(new EnhancedResourcesItemReader(inputFilePath, inputFilePattern))
                .processor(processor)
                .writer(writer)
                .build();
    }

    public static String streamToString(final InputStream inputStream) throws Exception {
        // buffering optional
        try (
                final BufferedReader br
                = new BufferedReader(new InputStreamReader(inputStream))) {
            // parallel optional
            return br.lines().parallel().collect(Collectors.joining("\n"));
        } catch (final IOException e) {
            throw new RuntimeException(e);
            // whatever.
        }
    }

}
