package com.klc213.ats.ib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.klc213.ats.common.AccountInfo;
import com.klc213.ats.common.AccountValue;
import com.klc213.ats.common.AtsBar;
import com.klc213.ats.common.BarSizeEnum;
import com.klc213.ats.common.Contract;
import com.klc213.ats.common.CurrencyEnum;
import com.klc213.ats.common.Portfolio;
import com.klc213.ats.common.TopicEnum;
import com.klc213.ats.common.util.HttpUtils;
import com.klc213.ats.common.util.KafkaUtils;

public class Test {

	static String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";
	static String KAFKA_CLIENT_ID = "ats-ib-rest";
	static String ATS_KAFKA_REST_URL = "http://localhost:7101";

	public static void main(String[] args) {

		testAccountTopic();

		testRealTimeBar();
	}
	public static void testRealTimeBar() {
		Producer<String, String> producer = null;
		try { 
			String symbol = "SPY";
			String topic = KafkaUtils.getTwsMktDataTopic(symbol);
			Set<String> topicSet = KafkaUtils.listTopics(ATS_KAFKA_REST_URL);
			if (!topicSet.contains(topic)) {
				// create Topic
				String url = ATS_KAFKA_REST_URL + "/createTopic/" + topic;
				String response = HttpUtils.restService(url, "POST");
				System.out.println("Topic" + topic + " created");
			}
			producer = KafkaUtils.createProducer(KAFKA_BOOTSTRAP_SERVER, KAFKA_CLIENT_ID);
			ObjectMapper objectMapper = new ObjectMapper();
			
			for (int i =0; i < 100; i++) {
				BigDecimal open = BigDecimal.valueOf(101 + i*0.1);
				BigDecimal high = BigDecimal.valueOf(102 + i*0.2);
				BigDecimal low = BigDecimal.valueOf(98 + i*0.3);
				BigDecimal close = BigDecimal.valueOf(100 + i*0.4);
				long volume = (100+i) * 1000;
				int count = i * 10  +1;
				long time = System.currentTimeMillis();
				AtsBar atsBar = new AtsBar(
						symbol, BarSizeEnum.BARSIZE_5_SECONDS,open,
						high, low, close,volume, count, time);
			
				String jsonStr = objectMapper.writeValueAsString(atsBar);

				final ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonStr);

				RecordMetadata meta = producer.send(record).get();
			}
			
			
			String topic1 = KafkaUtils.getTwsMktDataTopic(symbol);
			Set<String> topicSet1 = KafkaUtils.listTopics(ATS_KAFKA_REST_URL);
			if (topicSet1.contains(topic1)) {
				// delete Topic
				String url = ATS_KAFKA_REST_URL + "/deleteTopic/"+topic1;
				String response = HttpUtils.restService(url, "POST");
				System.out.println("Topic" + topic1 + " deleted");
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
	public static void testAccountTopic() {

		Producer<String, String> producer = null;
		try {

			Set<AccountValue> accountValueSet = new HashSet<>();
			AccountValue accountValue = new AccountValue("CURRENCT_VALUE", new BigDecimal("12345.6"), CurrencyEnum.USD);
			AccountValue accountValue2 = new AccountValue("BALANCE", new BigDecimal("12345.3"), CurrencyEnum.JPY);

			accountValueSet.add(accountValue);
			accountValueSet.add(accountValue2);

			Set<Portfolio> portfolioSet = new HashSet<>();

			Portfolio portfolio = new Portfolio();
			Contract contract = new Contract();
			contract.setSymbol("SPY");
			contract.setCurrency(CurrencyEnum.USD);
			portfolio.setAccountCode("U1309876");
			portfolio.setAvgCost(new BigDecimal("143.81"));
			portfolio.setContract(contract);
			portfolio.setMktPrice(new BigDecimal("145.87"));
			portfolio.setMktValue(new BigDecimal("1450.87"));
			portfolio.setPosition(new BigDecimal("1000"));

			Portfolio portfolio2 = new Portfolio();
			Contract contract2 = new Contract();
			contract2.setSymbol("DWI");
			contract2.setCurrency(CurrencyEnum.TWD);
			portfolio2.setAccountCode("U1309875");
			portfolio2.setAvgCost(new BigDecimal("143.82"));
			portfolio2.setContract(contract2);
			portfolio2.setMktPrice(new BigDecimal("145.82"));
			portfolio2.setMktValue(new BigDecimal("1450.82"));
			portfolio2.setPosition(new BigDecimal("1000"));

			portfolioSet.add(portfolio);
			portfolioSet.add(portfolio2);

			AccountInfo accountInfo = new AccountInfo();
			accountInfo.setAccountCode("U1309876");
			accountInfo.setAccountTime(System.currentTimeMillis());
			accountInfo.setAccountValueSet(accountValueSet);
			accountInfo.setPortfolioSet(portfolioSet);

			producer = KafkaUtils.createProducer(KAFKA_BOOTSTRAP_SERVER, KAFKA_CLIENT_ID);

			ObjectMapper objectMapper = new ObjectMapper();
			String jsonStr = objectMapper.writeValueAsString(accountInfo);

			final ProducerRecord<String, String> record = new ProducerRecord<>(TopicEnum.TWS_ACCOUNT.getTopic(),jsonStr);

			RecordMetadata meta = producer.send(record).get();



		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}

}
