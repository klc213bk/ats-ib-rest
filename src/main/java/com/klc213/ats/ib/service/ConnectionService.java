package com.klc213.ats.ib.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
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
import com.klc213.ats.common.util.KafkaUtils;

@Service
public class ConnectionService {
	private final Logger logger = LoggerFactory.getLogger(ConnectionService.class);

	@Autowired
	private TwsApi twsApi;

	@Value("${tws.api.url}")
	private String twsApiUrl;

	@Value("${tws.api.port}")
	private String twsApiPort;

	@Value("${kafka.client.id}")
	private String kafkaClientId;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	private Producer<String, String> producer;

	private String acctNo;

	public boolean connect() {
		logger.info(">>>>> connect connecting .....");

		if (twsApi.isConnected()) {
			logger.info(">>> Is already connected");

			return true;
		} else {

			while (!twsApi.isConnected() || twsApi.accountList().size() == 0) {
				twsApi.controller().connect(twsApiUrl, Integer.valueOf(twsApiPort), 0, null);
				try {
					Thread.sleep(100);
					logger.info("waiting to connect...");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			logger.info("connected !!!");
		} 

		// req account update
		boolean subscribe = true;
		acctNo = twsApi.accountList().get(0);

		AccountInfo accountInfo = new AccountInfo();
		Set<AccountValue> accountValueSet = new HashSet<>();
		Set<Portfolio> portfolioSet = new HashSet<>();

		accountInfo.setAccountCode(acctNo);
		accountInfo.setAccountValueSet(accountValueSet);
		accountInfo.setPortfolioSet(portfolioSet);

		producer = KafkaUtils.createProducer(kafkaBootstrapServer, kafkaClientId);
		
		twsApi.controller().reqAccountUpdates(subscribe, acctNo, new IAccountHandler() {

			private Long acctTime = 0L;
			//			private Map<AtsAccountValueKey,AtsAccountValue> atsAccountValueMap = new HashMap<>();
			//			private Map<AtsContract,AtsPosition> atsPositionMap = new HashMap<>();

			private final Object timeLock = new Object();

			@Override
			public void accountDownloadEnd(String accountNo) {
				logger.info("callback accountDownloadEnd!!! accountNo={}", accountNo);
	
				try {
					
					ObjectMapper objectMapper = new ObjectMapper();
					String jsonStr = objectMapper.writeValueAsString(accountNo);

					final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_ACCOUNT_DOWNLOAD_END.getTopic(), jsonStr);

					RecordMetadata meta = producer.send(record).get();
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					
				}

			}

			@Override
			public void accountTime(String accountTime) {
				logger.info("Got call back account time !!! accountTime={}", accountTime);
				
				try {
					
					ObjectMapper objectMapper = new ObjectMapper();
					String jsonStr = objectMapper.writeValueAsString(accountTime);

					final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_ACCOUNT_TIME.getTopic(), jsonStr);

					RecordMetadata meta = producer.send(record).get();
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					
				}
			}

			@Override
			public void accountValue(String account, String key, String value, String currency) {
				logger.info("call back account value: {}, {}, {}, {}", account, key, value, currency);

				if (StringUtils.equals(account, accountInfo.getAccountCode())) {

					AccountValue av = new AccountValue();
					av.setCurrency(currency);
					av.setKey(key);
					av.setValue(value);
					try {
						
						ObjectMapper objectMapper = new ObjectMapper();
						String jsonStr = objectMapper.writeValueAsString(av);

						final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_ACCOUNT_VALUE.getTopic(), jsonStr);

						RecordMetadata meta = producer.send(record).get();
					
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						
					}

				}
			}

			@Override
			public void updatePortfolio(Position position) {
				logger.info("call back Position= {}", ToStringBuilder.reflectionToString(position));

				Contract contract = new Contract();
				contract.setSymbol(position.contract().symbol());
				contract.setCurrency(position.contract().currency());
				
				Portfolio portfolio = new Portfolio();
				portfolio.setAccountCode(accountInfo.getAccountCode());
				portfolio.setAvgCost(BigDecimal.valueOf(position.averageCost()));
				portfolio.setContract(contract);
				portfolio.setMktPrice(BigDecimal.valueOf(position.marketPrice()));
				portfolio.setMktValue(BigDecimal.valueOf(position.marketValue()));
				portfolio.setPosition(BigDecimal.valueOf(position.position()));
				try {
					
					ObjectMapper objectMapper = new ObjectMapper();
					String jsonStr = objectMapper.writeValueAsString(portfolio);

					final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_PORTFOLIO.getTopic(), jsonStr);

					RecordMetadata meta = producer.send(record).get();
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					
				}
			}
			
		});

		return twsApi.isConnected();

	}
	public boolean disconnect() {
		logger.info(">>>>> is disconnecting .....");
		if (!twsApi.isConnected()) {
			logger.info(">>>>> is already disconnected");
			return true;	
		}
		while (twsApi.isConnected()) {
			twsApi.controller().disconnect();
			try {

				Thread.sleep(100);
				logger.info("waiting to disconnect ...");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (producer != null) {
			producer.close();
		}
		logger.info(">>>>> disconnected !!!");
		return !twsApi.isConnected();
	}


	//	private void createSymbolTopic(String symbol) throws InterruptedException, ExecutionException {
	//
	//		Properties config = new Properties();
	//		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBootstrapServers);
	//		AdminClient admin = AdminClient.create(config);
	//
	//		Set<String> existingTopics = admin.listTopics().names().get();
	//		//listing
	//		admin.listTopics().names().get().forEach(System.out::println);
	//
	//		//creating new topic
	//		String symbol = "twsapi.data." + symbol + ".0";
	//		if (!existingTopics.contains(topic)) {
	//			NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
	//			admin.createTopics(Collections.singleton(newTopic));
	//			logger.info(">>> topic={} created", topic);
	//		}
	//
	//	}
}
