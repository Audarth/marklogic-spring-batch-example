
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import com.marklogic.spring.batch.item.file.EnhancedResourcesItemReader;
import com.marklogic.spring.batch.item.file.TikaParserItemProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;

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
    public Job job(
            JobBuilderFactory jobBuilderFactory,
            @Qualifier("loadDocumentsFromDirectoryJobStep1") Step step) {

        JobExecutionListener listener = new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("BEFORE JOB");
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

    @Bean
    @JobScope
    public Step loadDocumentsFromDirectoryJobStep1(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider,
            @Value("#{jobParameters['input_file_path']}") String inputFilePath,
            @Value("#{jobParameters['input_file_pattern']}") String inputFilePattern) {

        EnhancedResourcesItemReader itemReader = new EnhancedResourcesItemReader(inputFilePath, inputFilePattern);

        TikaParserItemProcessor itemProcessor = new TikaParserItemProcessor();
        itemProcessor.setCollections(new String[]{"directoryJob"});

        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();

        MarkLogicItemWriter writer = new MarkLogicItemWriter(databaseClient);
        writer.setBatchSize(10);

        return stepBuilderFactory.get("step1")
                .<Resource, DocumentWriteOperation>chunk(10)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(writer)
                .build();
    }

}
