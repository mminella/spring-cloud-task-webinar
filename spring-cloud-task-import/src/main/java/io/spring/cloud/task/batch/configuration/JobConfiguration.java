/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.cloud.task.batch.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import io.spring.cloud.task.batch.domain.Customer;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.deployer.resource.maven.MavenResource;
import org.springframework.cloud.deployer.spi.local.LocalDeployerProperties;
import org.springframework.cloud.deployer.spi.local.LocalTaskLauncher;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * @author Michael Minella
 */
@Configuration
public class JobConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public JobRepository jobRepository;

	@Autowired
	private ConfigurableApplicationContext context;

	@Bean
	@Profile("master")
	public CommandLineRunner jobLauncherCommandLineRunner() {
		return new JobLaunchingCommandLineRunner();
	}

	@Bean
	public JobExplorerFactoryBean jobExplorer() {
		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();

		jobExplorerFactoryBean.setDataSource(this.dataSource);

		return jobExplorerFactoryBean;
	}

	@Bean
	public TaskLauncher taskLauncher() {
		LocalDeployerProperties localDeployerProperties = new LocalDeployerProperties();

		localDeployerProperties.setDeleteFilesOnExit(false);

		return new LocalTaskLauncher(localDeployerProperties);
	}

	@Bean
	public DeployerPartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer) throws Exception {
		MavenResource resource = MavenResource.parse("io.spring.cloud.task:import-job:1.0.0.BUILD-SNAPSHOT");

		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "importStep");

		Map<String, String> environmentProperties = new HashMap<>();
		environmentProperties.put("spring.profiles.active", "worker");

		partitionHandler.setEnvironmentProperties(environmentProperties);
		partitionHandler.setMaxWorkers(2);

		return partitionHandler;
	}

	@Bean
	@StepScope
	public Partitioner partitioner() throws IOException {
		MultiResourcePartitioner multiResourcePartitioner = new MultiResourcePartitioner();

		ClassLoader cl = this.getClass().getClassLoader();
		PathMatchingResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver(cl);
		Resource[] resources = resourcePatternResolver.getResources("file:/tmp/inbound/*.csv");

		for (Resource resource : resources) {
			System.out.println(">> CurResource = " + resource.getFile().getAbsolutePath());
		}

		multiResourcePartitioner.setResources(resources);

		return multiResourcePartitioner;
	}

	@Bean
	@Profile("worker")
	public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
		return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
	}

	@Bean
	@StepScope
	public Tasklet unzipTasklet(@Value("#{jobParameters['fileName']}") String fileName) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
				final InputStream is = new FileInputStream(new UrlResource(fileName).getFile());
				ArchiveInputStream ais = new
						ArchiveStreamFactory().createArchiveInputStream(ArchiveStreamFactory.ZIP,
						is);

				ZipArchiveEntry entry = (ZipArchiveEntry) ais.getNextEntry();
				String parent = new UrlResource(fileName).getFile().getParent();

				while (entry != null) {
					if(!entry.getName().startsWith("__")) {
						File outputFile = new File(parent, entry.getName());
						OutputStream os = new FileOutputStream(outputFile);

						IOUtils.copy(ais, os);
						os.close();
					}

					entry = (ZipArchiveEntry) ais.getNextEntry();
				}

				ais.close();
				is.close();

				return RepeatStatus.FINISHED;
			}
		};

	}

	@Bean
	public Step unzipStep() throws Exception {
		return stepBuilderFactory.get("unzipStep")
				.tasklet(unzipTasklet(null))
				.build();
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Customer> reader(@Value("#{stepExecutionContext['fileName']}") String fileName) throws Exception {
		FlatFileItemReader<Customer> customerReader = new FlatFileItemReader<>();
		customerReader.setLinesToSkip(1);
		customerReader.setResource(new UrlResource(fileName));

		BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(Customer.class);
		fieldSetMapper.afterPropertiesSet();

		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(new String[] {"firstName", "lastName", "address", "city", "zip", "phone"});

		DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		lineMapper.afterPropertiesSet();

		customerReader.setLineMapper(lineMapper);

		return customerReader;
	}

	@Bean
	public JdbcBatchItemWriter<Customer> writer() {
		JdbcBatchItemWriter<Customer> customerWriter = new JdbcBatchItemWriter<>();

		customerWriter.setDataSource(this.dataSource);
		customerWriter.setSql("INSERT INTO CUSTOMER (firstName, lastName, address, city, zip, phone) VALUES (:firstName, :lastName, :address, :city, :zip, :phone)");
		customerWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		customerWriter.afterPropertiesSet();

		return customerWriter;
	}

	@Bean
	public Step importStep() throws Exception {
		return stepBuilderFactory.get("import")
				.<Customer, Customer>chunk(400)
				.reader(reader(null))
				.writer(writer())
				.build();
	}

	@Bean
	public Step partitionedStep(PartitionHandler partitionHandler) throws Exception {
		return stepBuilderFactory.get("partitionedStep")
				.partitioner(importStep().getName(), partitioner())
				.step(importStep())
				.partitionHandler(partitionHandler)
				.build();
	}

	@Bean
	@Profile("master")
	public Job job(PartitionHandler partitionHandler) throws Exception {
		return jobBuilderFactory.get("job")
				.start(unzipStep())
				.next(partitionedStep(partitionHandler))
				.build();
	}
}
