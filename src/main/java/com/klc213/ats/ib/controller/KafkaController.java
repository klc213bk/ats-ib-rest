package com.klc213.ats.ib.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.klc213.ats.ib.service.KafkaService;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
	static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

	@Autowired
	private KafkaService kafkaService;
	
	@Autowired
	private ObjectMapper mapper;
	
	
	@PostMapping(path="/createTopic/{topic}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createTopic(@PathVariable("topic") String topic) {
		logger.info(">>>>controller createTopic is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			int exitCode = kafkaService.createTopic(topic);
			objectNode.put("returnCode", "0000");
			objectNode.put("exitCode", exitCode);
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller createTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/deleteTopic/{topic}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> deleteTopic(@PathVariable("topic") String topic) {
		logger.info(">>>>controller deleteTopic is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			int exitCode = kafkaService.deleteTopic(topic);
			objectNode.put("returnCode", "0000");
			objectNode.put("exitCode", exitCode);
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller deleteTopic finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
