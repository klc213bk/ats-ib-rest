package com.klc213.ats.ib.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.klc213.ats.ib.service.ConnectionService;

@RestController
@RequestMapping("/connection")
public class ConnectionController {
	private static final Logger LOG = LoggerFactory.getLogger(ConnectionController.class);
	
	@Autowired
	private ConnectionService connectionService;

	@Autowired
	private ObjectMapper mapper;

	@GetMapping(path="/ok")
	@ResponseBody
	public ResponseEntity<String> ok() {
		LOG.info(">>>>controller ok is called");
			
		LOG.info(">>>>controller ok finished ");
		
		return new ResponseEntity<String>("OK", HttpStatus.OK);
	}
	@PostMapping(path="/connect", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> connect() {
		LOG.info(">>>>controller connect is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			connectionService.connect();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		LOG.info(">>>>controller connect finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/disconnect", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> disconnect() {
		LOG.info(">>>>controller disconnect is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			connectionService.disconnect();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		LOG.info(">>>>controller disconnect finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}

}
