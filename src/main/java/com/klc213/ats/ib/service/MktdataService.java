package com.klc213.ats.ib.service;

import java.math.BigDecimal;
import java.util.HashMap;
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

	@Value("${kafka.client.id}")
	private String kafkaClientId;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	@Value("${ats.kafka.rest.url}")
	private String atsKafkaRestUrl;
	
	@Value("${replication.factor}")
	private String replicationFactor;
	
	@Value("${num.partitions}")
	private String numPartitions;
	
	@Autowired
	private TwsApi twsApi;
	
	private Producer<String, String> realTimeBarsProducer;
	private String realTimeBarsTopic;
	
	private Map<String, IRealTimeBarHandler> realTimeBarHandlerMap = new HashMap<>();
	
	public void reqRealTimeBars(String symbol) throws Exception {
		
		realTimeBarsTopic = "TWS.MKTDATA." + symbol;
		Set<String> topicSet = KafkaUtils.listTopics(atsKafkaRestUrl);
		String url = null;
		if (!topicSet.contains(realTimeBarsTopic)) {
			// create Topic
			url = String.format("%s/createTopic/%s/rf/%s/np/%s", atsKafkaRestUrl,realTimeBarsTopic,replicationFactor,numPartitions);
			String response = HttpUtils.restService(url, "POST");
			
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

					final ProducerRecord<String, String> record = new ProducerRecord<>(realTimeBarsTopic, jsonStr);

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
		twsApi.controller().cancelRealtimeBars(handler);
		
		realTimeBarHandlerMap.remove(symbol);
		
		realTimeBarsProducer.close();
		
		Set<String> topicSet = KafkaUtils.listTopics(atsKafkaRestUrl);
		String url = null;
		if (!topicSet.contains(realTimeBarsTopic)) {
			// create Topic
			url = String.format("%s/deleteTopic/%s", atsKafkaRestUrl,realTimeBarsTopic);
			String response = HttpUtils.restService(url, "POST");
			
		}
//		
//		String topic = KafkaUtils.getTwsMktDataTopic(symbol, BarSizeEnum.BARSIZE_5_SECONDS);
//		Set<String> topicSet = KafkaUtils.listTopics(atsKafkaRestUrl);
//		if (topicSet.contains(topic)) {
//			// create Topic
//			String url = atsKafkaRestUrl + "/deleteTopic/"+topic;
//			String response = HttpUtils.restService(url, "POST");
//			
//		}
	}
}
