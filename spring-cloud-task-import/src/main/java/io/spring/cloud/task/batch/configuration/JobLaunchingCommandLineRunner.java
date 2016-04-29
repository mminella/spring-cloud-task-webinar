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

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;

/**
 * @author Michael Minella
 */
public class JobLaunchingCommandLineRunner implements CommandLineRunner {

	@Autowired
	private Job importJob;

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Environment environment;

	@Override
	public void run(String... args) throws Exception {
		String resourcesString = environment.getProperty("import_job_resources");
		System.out.println(">> resourcesString = " + resourcesString);

		JobParameters jobParameters = new JobParametersBuilder().addString("fileName", resourcesString).toJobParameters();

		this.jobLauncher.run(this.importJob, jobParameters);
	}
}
