package com.klc213.ats.ib.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ib.controller.ApiConnection.ILogger;
import com.ib.controller.ApiController;
import com.ib.controller.ApiController.IConnectionHandler;
import com.ib.controller.Formats;

@Component
public class TwsApi implements IConnectionHandler {
	private final Logger logger = LoggerFactory.getLogger(TwsApi.class);
	
	private ApiController m_controller;
	
	private final List<String> m_acctList = new ArrayList<>();
	
	private final IbLogger m_inLogger = new IbLogger();
	private final IbLogger m_outLogger = new IbLogger();
	
	ILogger getInLogger()            { return m_inLogger; }
	ILogger getOutLogger()           { return m_outLogger; }
	
	private boolean isConnected;
	
	public ApiController controller() {
        if ( m_controller == null ) {
            m_controller = new ApiController( this, getInLogger(), getOutLogger() );
        }
        return m_controller;
    }
	
	@Override
	public void accountList(List<String> list) {
		show( "Received account list");
		m_acctList.clear();
		m_acctList.addAll( list);
	}

	@Override
	public void connected() {
		isConnected = true;
		
		show( "connected");
		
		controller().reqCurrentTime(time -> show( "Server date/time is " + Formats.fmtDate(time * 1000) ));
		
		controller().reqBulletins( true, (msgId, newsType, message, exchange) -> {
            String str = String.format( "Received bulletin:  type=%s  exchange=%s", newsType, exchange);
            show( str);
            show( message);
        });
	}

	@Override
	public void disconnected() {
		isConnected = false;
		
		show( "disconnected is called");
	}

	@Override
	public void error(Exception e) {
		show( e.toString() );
		
	}

	@Override
	public void message(int id, int errorCode, String errorMsg) {
		show( id + " " + errorCode + " " + errorMsg);
		
	}

	@Override
	public void show(String msg) {
		logger.info(">>> " + msg);
		
	}
	
	public boolean isConnected() {
		return isConnected;
	}
	public List<String> accountList() 	{ 
		return m_acctList; 
	}
	private static class IbLogger implements ILogger {
		
		@Override public void log(final String str) {
			
		}
	}
}
