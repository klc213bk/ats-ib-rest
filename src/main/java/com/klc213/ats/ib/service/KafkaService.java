package com.klc213.ats.ib.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ib.controller.ApiController.IAccountHandler;
import com.ib.controller.Position;
import com.klc213.ats.common.AccountInfo;
import com.klc213.ats.common.AccountValue;
import com.klc213.ats.common.Contract;
import com.klc213.ats.common.Portfolio;
import com.klc213.ats.common.TopicEnum;
import com.klc213.ats.common.util.HttpUtils;
import com.klc213.ats.common.util.KafkaUtils;

@Service
public class KafkaService {
	private final Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Value("${kafka.home}")
	private String kafkaHome;
	
	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	public List<String> listTopic() throws Exception {
		ProcessBuilder builder = new ProcessBuilder();
		builder.command("sh", "-c", "./bin/kafka-topics.sh --list --bootstrap-server " + kafkaBootstrapServer);
		builder.directory(new File(kafkaHome));
		Process process = builder.start();
		process.getInputStream();
		List<String> topicList = new ArrayList<>();
		new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach( 
				e -> topicList.add(e)
				);
		
		int exitCode = process.waitFor();
		System.out.println(">>>createTopic exitCode:" + exitCode);
		
		return topicList;
	}
	public int deleteTopic(String topic) throws Exception {
		ProcessBuilder builder = new ProcessBuilder();
		builder.command("sh", "-c", "./bin/kafka-topics.sh --delete --topic " + topic + " --bootstrap-server " + kafkaBootstrapServer);
		builder.directory(new File(kafkaHome));
		Process process = builder.start();
		process.getInputStream();
		
		int exitCode = process.waitFor();
		
		return exitCode;
	}
	public int createTopic(String topic) throws Exception {
		ProcessBuilder builder = new ProcessBuilder();
		builder.command("sh", "-c", "./bin/kafka-topics.sh --create --topic " + topic + " --bootstrap-server " + kafkaBootstrapServer);
		builder.directory(new File(kafkaHome));
		Process process = builder.start();
		process.getInputStream();
		
		int exitCode = process.waitFor();
	
		return exitCode;
	
	}
}
