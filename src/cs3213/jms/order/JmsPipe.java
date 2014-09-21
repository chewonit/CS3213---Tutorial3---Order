package cs3213.jms.order;

import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Matric 1: A0097797Y
 * Name   1: Chew Chin Hao, Darry
 * 
 * Matric 2: A0097964H
 * Name   2: Chew Tee Ming
 *
 * This file implements a pipe that transfer messages using JMS.
 */

public class JmsPipe implements IPipe {
	
	private QueueConnectionFactory qconFactory;
    private QueueConnection qcon;
    private QueueSession qsession;
    private QueueSender qsender;
    private QueueReceiver qreceiver;
    private Queue queue;
    private TextMessage msg;

	public JmsPipe(String factoryName, String queueName) throws NamingException, JMSException {
		
		InitialContext ctx = getInitialContext();
		qconFactory = (QueueConnectionFactory) ctx.lookup(factoryName);
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ctx.lookup(queueName);
        qcon.start();
	}

	@Override
	public void write(Order message) {
		
		if (qsender == null) {
			initQueueSender();
		}
				
		try {			
			msg.setText(message.toString());
			qsender.send(msg);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void initQueueSender() {
		
		try {
			qsender = qsession.createSender(queue);
			msg = qsession.createTextMessage();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Order read() {
		
		if (qreceiver == null) {
			initQueueReceiver();
		}
		
		try {
			TextMessage msg = (TextMessage) qreceiver.receive();
			return Order.fromString( msg.getText() );
		} catch (JMSException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void initQueueReceiver() {
		try {
			qreceiver = qsession.createReceiver(queue);
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			if (qreceiver != null) {
				qreceiver.close();
			}
			if (qsender != null) {
				qsender.close();
			}
			qsession.close();
			qcon.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
	
	private static InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }
    
}
