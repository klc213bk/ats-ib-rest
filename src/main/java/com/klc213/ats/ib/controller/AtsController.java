package com.klc213.ats.ib.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.klc213.ats.ib.service.AtsService;

@RestController
@RequestMapping("/ats")
public class AtsController {
	static final Logger logger = LoggerFactory.getLogger(AtsController.class);

	@Autowired
	private AtsService atsService;
	
	@Autowired
	private ObjectMapper mapper;
	
	
	@PostMapping(path="/createAtsTopics", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createAtsTopics() {
		logger.info(">>>>controller createAtsTopics is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			atsService.createAtsTopics();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller createAtsTopics finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/deleteAtsTopics", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> deleteAtsTopics() {
		logger.info(">>>>controller deleteAtsTopics is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			atsService.deleteAtsTopics();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller deleteAtsTopics finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
