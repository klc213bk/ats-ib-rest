package com.klc213.ats.ib.service;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
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
public class AtsService {
	private final Logger logger = LoggerFactory.getLogger(AtsService.class);

	@Value("${kafka.client.id}")
	private String kafkaClientId;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	@Value("${ats.kafka.rest.url}")
	private String atsKafkaRestUrl;

	public void createAtsTopics() throws Exception {
		logger.info(">>>>>>>>>>>> createAtsTopics ");
		try {
			String url = null;
			for (TopicEnum t : TopicEnum.values()) {
				url = atsKafkaRestUrl + "/createTopic/" + t.getTopic() + "/rf/3/np/3";
				HttpUtils.restService(url, "POST");
			}

		} catch (IOException e) {
			logger.error(">>> Error!!!, createAtsTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			logger.error(">>> Error!!!, createAtsTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void deleteAtsTopics() throws Exception {
		logger.info(">>>>>>>>>>>> deleteAtsTopics ");
		try {
			String url = null;
			for (TopicEnum t : TopicEnum.values()) {
				url = atsKafkaRestUrl + "/deleteTopic/" + t.getTopic();
				HttpUtils.restService(url, "POST");
			}

		} catch (IOException e) {
			logger.error(">>> Error!!!, deleteAtsTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			logger.error(">>> Error!!!, deleteAtsTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
}
