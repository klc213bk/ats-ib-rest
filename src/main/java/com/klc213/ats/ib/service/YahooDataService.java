package com.klc213.ats.ib.service;

import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class YahooDataService {
	private final static Logger LOGGER = LoggerFactory.getLogger(YahooDataService.class);
	
	private static String YAHOO_HISTORY_URL = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%d&period2=%d&interval=1d&events=history&includeAdjustedClose=true";
	private static SimpleDateFormat SDF;
	static
	{
		SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	
	@Value("${mktdata.dir.1d}")
	private String mktdataDir1d;
	
	/**
	 * 
	 * @param date1 yyyy-MM-dd
	 * @param date2 yyyy-MM-dd
	 * @throws Exception
	 */
	public long downloadMktDataDaily(String symbol, String fromdate, String todate) throws Exception {
	
        
		Date d1 = SDF.parse(fromdate + " 00:00:00");
		Long d1Sec = d1.getTime() / 1000;
		
		Date d2 = SDF.parse(todate + " 00:00:00");
		Long d2Sec = d2.getTime() / 1000;
		
		String yahooUrl = String.format(YAHOO_HISTORY_URL, symbol,d1Sec,d2Sec);
		
		TrustManager x509TM =  new X509TrustManager() {
		        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
		            return null;
		        }
		        public void checkClientTrusted(
		            java.security.cert.X509Certificate[] certs, String authType) {
		        }
		        public void checkServerTrusted(
		            java.security.cert.X509Certificate[] certs, String authType) {
		        }
		    };
		 
		TrustManager[] trustAllCerts = new TrustManager[]{ x509TM };

			// Activate the new trust manager
			try {
			    SSLContext sc = SSLContext.getInstance("SSL");
			    sc.init(null, trustAllCerts, new java.security.SecureRandom());
			    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			} catch (Exception e) {
			}

			URL url = new URL(yahooUrl);
		//	URLConnection connection = url.openConnection();
		//	InputStream is = connection.getInputStream();
			// .. then download the file
			
			ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
			FileOutputStream fileOutputStream = new FileOutputStream(mktdataDir1d + "/" + symbol + ".csv");
			FileChannel fileChannel = fileOutputStream.getChannel();
			long bytes = fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
			
			fileOutputStream.close();
			
			return bytes;
	}
	/**
	 * 
	 * @param date1 yyyy-MM-dd
	 * @param date2 yyyy-MM-dd
	 * @throws Exception
	 */
	public void downloadMktDataDailySP500(String fromdate, String todate) throws Exception {
	
        List<String> symbolList = new ArrayList<>();
        symbolList.add("SPY");
        symbolList.add("MSFT");
        symbolList.add("GOOGL");
        symbolList.add("TSM");
        
        symbolList.stream().forEach(s -> testSym(s));
        
	//	downloadMktDataDaily("SPY", fromdate, todate);
		
		
	}
	private void testSym(String symbol) {
		System.out.println("symbol:" + symbol);
	}
}
