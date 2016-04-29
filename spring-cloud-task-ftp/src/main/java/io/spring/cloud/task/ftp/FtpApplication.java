package io.spring.cloud.task.ftp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.file.remote.synchronizer.InboundFileSynchronizer;
import org.springframework.integration.ftp.filters.FtpSimplePatternFileListFilter;
import org.springframework.integration.ftp.inbound.FtpInboundFileSynchronizer;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.PeriodicTrigger;

@SpringBootApplication
@EnableTask
@EnableBinding(Source.class)
public class FtpApplication {

	public static void main(String[] args) {
		SpringApplication.run(FtpApplication.class, args);
	}

	@Autowired
	@Bindings(FtpApplication.class)
	Source source;

	@Bean
	public InboundFileSynchronizer fileSynchronizer(DefaultFtpSessionFactory sessionFactory) {
		FtpInboundFileSynchronizer ftpInboundFileSynchronizer =
				new FtpInboundFileSynchronizer(sessionFactory);
		ftpInboundFileSynchronizer.setFilter(new FtpSimplePatternFileListFilter("*.zip"));
		ftpInboundFileSynchronizer.setRemoteDirectory("/");

		return ftpInboundFileSynchronizer;
	}
}
