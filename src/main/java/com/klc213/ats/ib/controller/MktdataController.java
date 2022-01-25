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
import com.klc213.ats.ib.service.MktdataService;

@RestController
@RequestMapping("/mktdata")
public class MktdataController {
	private static final Logger LOG = LoggerFactory.getLogger(MktdataController.class);

	@Autowired
	private MktdataService mktdataService;

	@Autowired
	private ObjectMapper mapper;
	
	@PostMapping(path="/reqRealTimeBars/{symbol}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> reqRealTimeBars(@PathVariable("symbol") String symbol) {
		LOG.info(">>>>controller reqRealTimeBars is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			mktdataService.reqRealTimeBars(symbol);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		LOG.info(">>>>controller reqRealTimeBars finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/cancelRealTimeBars/{symbol}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> cancelRealTimeBars(@PathVariable("symbol") String symbol) {
		LOG.info(">>>>controller cancelRealTimeBars is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			mktdataService.cancelRealTimeBars(symbol);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		LOG.info(">>>>controller cancelRealTimeBars finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
