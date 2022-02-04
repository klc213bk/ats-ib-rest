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
					av.setUpdateTime(System.currentTimeMillis());
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
				portfolio.setUpdateTime(System.currentTimeMillis());
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
			//
			//			@Override
			//			public void accountTime(String time) {
			//				String[] timeArr = time.split(":");
			//				LocalDateTime accountTime =  LocalDate.now().atTime(Integer.valueOf(timeArr[0]), Integer.valueOf(timeArr[1]));
			//				logger.info("accountTime:{}", accountTime);
			//
			//				Date accDate = Date.from(accountTime.atZone(ZoneId.systemDefault()).toInstant());
			//
			//				synchronized (timeLock) {
			//					acctTime = accDate.getTime();
			//					atsAccountValueMap.forEach((k, v) -> {
			//						if (acctTime.compareTo(v.getTime()) > 0) {
			//							v.setTime(acctTime);
			//							try {
			//								AtsProducer.sendAccountValueMessage(v);
			//							} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
			//								// TODO Auto-generated catch block
			//								e.printStackTrace();
			//							}
			//						}
			//					} );
			//					atsPositionMap.forEach((k, v) -> {
			//						if (acctTime.compareTo(v.getTime()) > 0) {
			//							v.setTime(acctTime);
			//							try {
			//								AtsProducer.sendPositionMessage(v);
			//							} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
			//								// TODO Auto-generated catch block
			//								e.printStackTrace();
			//							}
			//						}
			//					});
			//					
			//				}
			//			}
			//
			//			@Override
			//			public void accountValue(String account, String key, String value, String currency) {
			//				//	logger.info("set account value: {}, {}, {}, {}", account, key, value, currency);
			//
			//				AccountValueKeyEnum accountValueKeyEnum = IbAccountValueKey.get(key);
			//				if (IbAccountValueKey.get(key) != null) {
			//					AtsAccountValueKey atsAccountValueKey = new AtsAccountValueKey();
			//					atsAccountValueKey.setAccount(account);
			//					atsAccountValueKey.setCurrency(currency);
			//					atsAccountValueKey.setKey(accountValueKeyEnum);
			//
			//					AtsAccountValue atsAccountValue = new AtsAccountValue();
			//					atsAccountValue.setAtsAccountValueKey(atsAccountValueKey);
			//					atsAccountValue.setValue(value);
			//					synchronized (timeLock) {
			//						try {
			//
			//							AtsAccountValue atsAccountValueOld = atsAccountValueMap.get(atsAccountValueKey); // last
			//							if (atsAccountValueOld != null && acctTime.compareTo(atsAccountValueOld.getTime()) > 0) {
			//								atsAccountValue.setTime(acctTime);
			//								atsAccountValueMap.put(atsAccountValueKey, atsAccountValue);
			//								AtsProducer.sendAccountValueMessage(atsAccountValue);
			//								
			//							} else {
			//								atsAccountValueMap.put(atsAccountValueKey, atsAccountValue);
			//							}
			//
			//						} catch (JsonProcessingException e) {
			//							// TODO Auto-generated catch block
			//							e.printStackTrace();
			//						} catch (InterruptedException e) {
			//							// TODO Auto-generated catch block
			//							e.printStackTrace();
			//						} catch (ExecutionException e) {
			//							// TODO Auto-generated catch block
			//							e.printStackTrace();
			//						}
			//					}
			//				}
			//
			//			}
			//
			//			@Override
			//			public void updatePortfolio(Position position) {
			//				logger.info("acct position!!!" + ToStringBuilder.reflectionToString(position));
			//
			//				AtsContract atsContract = new AtsContract(position.contract().symbol());
			//				AtsPosition atsPosition = new AtsPosition();
			//				atsPosition.setAccount(position.account());
			//				atsPosition.setAverageCost(BigDecimal.valueOf(position.averageCost()));
			//				atsPosition.setContract(atsContract);
			//				atsPosition.setMarketPrice(BigDecimal.valueOf(position.marketPrice()));
			//				atsPosition.setMarketValue(BigDecimal.valueOf(position.marketValue()));
			//				atsPosition.setPosition(BigDecimal.valueOf(position.position()));
			//				atsPosition.setRealPnl(BigDecimal.valueOf(position.realPnl()));
			//				atsPosition.setUnrealPnl(BigDecimal.valueOf(position.unrealPnl()));
			//
			//				synchronized (timeLock) {
			//					try {
			//						AtsPosition atsPositionOld = atsPositionMap.get(atsContract); // last
			//						if (atsPositionOld != null && acctTime.compareTo(atsPositionOld.getTime()) > 0) {
			//							atsPosition.setTime(acctTime);
			//							atsPositionMap.put(atsContract, atsPosition);
			//							AtsProducer.sendPositionMessage(atsPosition);
			//						topicTwsAccount	
			//						} else {
			//							atsPositionMap.put(atsContract, atsPosition);
			//						}				
			//
			//					} catch (JsonProcessingException e) {
			//						// TODO Auto-generated catch block
			//						e.printStackTrace();
			//					} catch (InterruptedException e) {
			//						// TODO Auto-generated catch block
			//						e.printStackTrace();
			//					} catch (ExecutionException e) {
			//						// TODO Auto-generated catch block
			//						e.printStackTrace();
			//					}
			//				}
			//			}
			//
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
