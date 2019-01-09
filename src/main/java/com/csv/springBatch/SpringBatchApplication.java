package com.csv.springBatch;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.csv.springBatch.Service.CsvReaderService;

@SpringBootApplication
public class SpringBatchApplication {
	@Autowired
	CsvReaderService service;

	public static void main(String[] args) throws FileNotFoundException, IOException {

		SpringApplication.run(SpringBatchApplication.class, args);
	}

	@Bean
	ApplicationRunner init() {
		return arg -> {
			service.readFile();
		};
	}

}
