package com.klc213.ats.ib.service;

import java.util.HashMap;
import java.util.Map;
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
import com.klc213.ats.common.CurrencyEnum;
import com.klc213.ats.common.TopicEnum;
import com.klc213.ats.common.util.KafkaUtils;

@Service
public class MktdataService {
	private final static Logger LOGGER = LoggerFactory.getLogger(MktdataService.class);

	@Value("${kafka.client.id}")
	private String kafkaClientId;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	@Autowired
	private TwsApi twsApi;
	
	private Producer<String, String> producer;
	
	private Map<String, IRealTimeBarHandler> realTimeBarHandlerMap = new HashMap<>();
	
	public void reqRealTimeBars(String symbol) throws Exception {
		
		IRealTimeBarHandler handler = new IRealTimeBarHandler() {

			@Override
			public void realtimeBar(Bar b) {
				if (producer == null) {
					producer = KafkaUtils.createProducer(kafkaBootstrapServer, kafkaClientId);
				}
				try {
					Bar bar = new Bar(0, 0, 0, 0, 0, 0, 0, 0);
					Bar( long time, double high, double low, double open, double close, double wap, long volume, int count);
					accountInfo.setAccountTime(System.currentTimeMillis());
					
					ObjectMapper objectMapper = new ObjectMapper();
					String jsonStr = objectMapper.writeValueAsString(accountInfo);

					final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_ACCOUNT.getTopic(), jsonStr);

					RecordMetadata meta = producer.send(record).get();


				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					producer.flush();
					producer.close();
				}
				
			}
			
		};
		
		Contract contract = new Contract();
		contract.symbol(symbol);
		contract.secType(SecType.STK);
		contract.currency(CurrencyEnum.USD.name());
		contract.exchange("SMART");
		
		twsApi.controller().reqRealTimeBars(contract, WhatToShow.MIDPOINT, true, handler);
		
		realTimeBarHandlerMap.put(symbol, handler);
	}
	public void cancelRealTimeBars(String symbol) throws Exception {
		IRealTimeBarHandler handler = realTimeBarHandlerMap.get(symbol);
		twsApi.controller().cancelRealtimeBars(handler);
		
		realTimeBarHandlerMap.remove(symbol);
	}
}
