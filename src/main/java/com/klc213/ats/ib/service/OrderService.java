package com.klc213.ats.ib.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ib.client.Contract;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.OrderStatus;
import com.ib.client.OrderType;
import com.ib.client.Types.Action;
import com.ib.contracts.StkContract;
import com.ib.controller.ApiController.IOrderHandler;

@Service
public class OrderService {
	private final Logger logger = LoggerFactory.getLogger(OrderService.class);

	@Value("${kafka.client.id}")
	private String kafkaClientId;
	
	@Autowired
	private TwsApi twsApi;
	
	private Map<Order, IOrderHandler> orderHandlerMap = new HashMap<>();
	
	public void placeOrder(String symbol) {
		
		Contract contract = new StkContract("SPY");
		
        Order order = new Order();
        order.action(Action.BUY);
        order.orderType(OrderType.LMT);
        order.totalQuantity(1.0);
        order.lmtPrice(400.00); 

		IOrderHandler handler = new IOrderHandler() {

			@Override
			public void handle(int errorCode, String errorMsg) {
				logger.info(">>> place orde rhandler errorCode={}, errmsg={}", errorCode, errorMsg);
				
			}

			@Override
			public void orderState(OrderState orderState) {
				logger.info(">>> place order orderState={}", orderState.getStatus());
				
			}

			@Override
			public void orderStatus(OrderStatus status, double filled, double remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
				logger.info(">>> place order orderStatus={}, filled={},remainning={},avgFillPrice={},permId={},parentId={},lastFillPrice={},clientId={},whyHeld={}, mktCapPrice={}", 
						status.name(),filled,remaining,avgFillPrice,permId,parentId,lastFillPrice,clientId,whyHeld,mktCapPrice);
			}
			
		};
		twsApi.controller().placeOrModifyOrder(contract, order, handler);
		
		orderHandlerMap.put(order, handler);
		
	}
}
