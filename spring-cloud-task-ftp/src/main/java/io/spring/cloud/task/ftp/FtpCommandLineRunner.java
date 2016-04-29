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
package io.spring.cloud.task.ftp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.integration.file.remote.synchronizer.InboundFileSynchronizer;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

/**
 * @author Michael Minella
 */
@Component
public class FtpCommandLineRunner implements CommandLineRunner {

	@Autowired
	private InboundFileSynchronizer fileSynchronizer;

	@Autowired
	private Source source;

	@Override
	public void run(String... args) throws Exception {
		fileSynchronizer.synchronizeToLocalDirectory(new File("/tmp/inbound"));

		ClassLoader cl = this.getClass().getClassLoader();
		PathMatchingResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver(cl);
		Resource[] resources = resourcePatternResolver.getResources("file:/tmp/inbound/*.zip");

		if(resources.length > 0) {
			launchImport(resources);
		}
	}

	private void launchImport(Resource[] resources) throws IOException {
		StringBuilder builder = new StringBuilder();
		for (Resource resource : resources) {
			builder.append(resource.getURL() + ",");
		}

		Map<String, String> properties = new HashMap<>();

		String resource = builder.substring(0, builder.length() - 1);
		properties.put("import_job_resources", resource);
		properties.put("spring_profiles_active", "master");

		TaskLaunchRequest request = new TaskLaunchRequest("import-job",
				"io.spring.cloud.task", "1.0.0.BUILD-SNAPSHOT", "jar",
				null, properties);

		source.output().send(new GenericMessage<>(request));
	}
}
