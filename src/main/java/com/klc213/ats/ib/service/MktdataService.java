package com.klc213.ats.ib.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ib.client.Contract;
import com.ib.client.Types.SecType;
import com.ib.client.Types.WhatToShow;
import com.ib.controller.ApiController.IRealTimeBarHandler;
import com.ib.controller.Bar;
import com.klc213.ats.common.AtsBar;
import com.klc213.ats.common.BarSizeEnum;
import com.klc213.ats.common.TopicEnum;
import com.klc213.ats.common.util.HttpUtils;
import com.klc213.ats.common.util.KafkaUtils;

@Service
public class MktdataService {
	private final static Logger LOGGER = LoggerFactory.getLogger(MktdataService.class);

	@Value("${kafka.home}")
	private String kafkaHome;
	
	@Value("${kafka.client.id}")
	private String kafkaClientId;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	@Autowired
	private TwsApi twsApi;
	
	@Autowired
	private KafkaService kafkaService;
	
	private Producer<String, String> realTimeBarsProducer;
	
	//private String realTimeBarsTopic;
	
	private Map<String, IRealTimeBarHandler> realTimeBarHandlerMap = new HashMap<>();
	
	private Map<String, Set<String>> realTimeTopicMap= new HashMap<>(); // topic, symbolSet
	
	public void reqRealTimeBars(String symbol) throws Exception {
			
		String topic = KafkaUtils.getTwsMktDataRealtimeTopic(symbol);
		if (!realTimeTopicMap.containsKey(topic) ) {
			KafkaUtils.createTopic(kafkaBootstrapServer, kafkaHome, topic);
			Set<String> symbolSet = new HashSet<>();
			symbolSet.add(symbol);
			
			realTimeTopicMap.put(topic, symbolSet);
		} else {
			Set<String> symbolSet = realTimeTopicMap.get(topic);
			if (!symbolSet.contains(symbol)) {
				symbolSet.add(symbol);
			}
		}
		
		IRealTimeBarHandler handler = new IRealTimeBarHandler() {

			@Override
			public void realtimeBar(Bar b) {
				if (realTimeBarsProducer == null) {
					String clientid = kafkaClientId + "-RealTimeBars";
					realTimeBarsProducer = KafkaUtils.createProducer(kafkaBootstrapServer, clientid);
				}
				try { 
					AtsBar atsBar = new AtsBar(
							symbol, BarSizeEnum.BARSIZE_5_SECONDS, BigDecimal.valueOf(b.open()),
							BigDecimal.valueOf(b.high()), BigDecimal.valueOf(b.low()), BigDecimal.valueOf(b.close()),
							b.volume(), b.count(), b.time() * 1000);
					
					ObjectMapper objectMapper = new ObjectMapper();
					String jsonStr = objectMapper.writeValueAsString(atsBar);

					final ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonStr);

					RecordMetadata meta = realTimeBarsProducer.send(record).get();

				} catch (Exception e) {
					
					e.printStackTrace();
				} finally {
					realTimeBarsProducer.flush();
				}
				
			}
			
		};
		
		Contract contract = new Contract();
		contract.symbol(symbol);
		contract.secType(SecType.STK);
		contract.currency("USD");
		contract.exchange("SMART");
		
		twsApi.controller().reqRealTimeBars(contract, WhatToShow.MIDPOINT, true, handler);
		
		realTimeBarHandlerMap.put(symbol, handler);
	}
	public void cancelRealTimeBars(String symbol) throws Exception {
		IRealTimeBarHandler handler = realTimeBarHandlerMap.get(symbol);

		String topic = KafkaUtils.getTwsMktDataRealtimeTopic(symbol);
		if (realTimeTopicMap.containsKey(topic) ) {
			Set<String> symbolSet = realTimeTopicMap.get(topic);
			if (symbolSet.contains(symbol)) {
				symbolSet.remove(symbol);
				if (symbolSet.isEmpty()) {
					twsApi.controller().cancelRealtimeBars(handler);
					
					realTimeBarsProducer.close();
					
					KafkaUtils.deleteTopic(kafkaBootstrapServer, kafkaHome, topic);
					
					realTimeBarHandlerMap.remove(symbol);
					
				}
			}  else {
				// error, do nothing
			}
		} else {
			// topic does not exists, do nothing
		}
		
	}
}
