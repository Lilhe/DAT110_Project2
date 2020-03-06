package no.hvl.dat110.broker;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.messages.CreateTopicMsg;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messagetransport.Connection;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users to the topic
	protected ConcurrentHashMap<String, Set<String>> subscriptions;

	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	protected ConcurrentHashMap<String, ClientSession> clients;
	
	//oppgave E maps from a user to messages
	protected ConcurrentHashMap<String, Set<Message>> messageBuffer;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
		messageBuffer = new ConcurrentHashMap<String, Set<Message>>();
	}
	
	public Set<Message> getBufferMessages(String user) {
		return messageBuffer.get(user);
	}
	
	public void addBufferMessage(String user, Message message) {
		
		Set<Message> sm;
		
		if (!messageBuffer.containsKey(user)) { //if the user is not in the buffermessage list
			sm = new HashSet<Message>();
		} else { //if the user is in the buffermessage list
			sm = messageBuffer.get(user);
		}
		
		sm.add(message);
		messageBuffer.put(user, sm);
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	// get the session object for a given user
	// session object can be used to send a message to the user

	public ClientSession getSession(String user) {

		ClientSession session = clients.get(user);

		return session;
	}

	public Set<String> getSubscribers(String topic) {

		return (subscriptions.get(topic));

	}

	public void addClientSession(String user, Connection connection) {

		// TODO: add corresponding client session to the storage

		ClientSession cs = new ClientSession(user, connection);
		clients.put(user, cs);

	}

	public void removeClientSession(String user) {

		// TODO: remove client session for user from the storage

		clients.remove(user);

	}

	public void createTopic(String topic) {

		// TODO: create topic in the storage

		if(!subscriptions.containsKey(topic)) {
			Set<String> subscribers = ConcurrentHashMap.newKeySet();
			subscriptions.put(topic, subscribers);
		}

	}

	public void deleteTopic(String topic) {

		// TODO: delete topic from the storage

		subscriptions.remove(topic);

	}

	public void addSubscriber(String user, String topic) {

		// TODO: add the user as subscriber to the topic

		subscriptions.get(topic).add(user);

	}

	public void removeSubscriber(String user, String topic) {

		// TODO: remove the user as subscriber to the topic

		subscriptions.get(topic).remove(user);
	}
}
