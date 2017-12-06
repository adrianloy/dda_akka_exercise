package de.hpi.akka_tutorial.remote.actors;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.stop;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.OneForOneStrategy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;
import de.hpi.akka_tutorial.remote.actors.Master.PrimesMessage;
import de.hpi.akka_tutorial.remote.actors.scheduling.PWSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.scheduling.SchedulingStrategy;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;
import scala.concurrent.duration.Duration;

/**
 * The master receives password hashes and ranges of possible passwords to brute force it. This is done by delegation to slaves.
 */
public class PWMaster extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "master";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link PWMaster} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props(final ActorRef listener, PWSchedulingStrategy.PWFactory schedulingStrategyFactory, final int numLocalWorkers) {
		return Props.create(PWMaster.class, () -> new PWMaster(listener, schedulingStrategyFactory, numLocalWorkers));
	}

	/**
	 * Asks the {@link PWMaster} to start the distributed password brute force calculation of the pwhash
	 */
	public static class PWHashMessage implements Serializable {

		private static final long serialVersionUID = 1538940836039448197L;
		
		private String pwhash;
		
		private String username;

		/**
		 * Construct a new {@link RangeMessage} object.
		 * 
		 * @param startNumber first number in the range to be checked as prime (inclusive)
		 * @param endNumber last number in the range to be checked as prime (inclusive)
		 */
		public PWHashMessage(final String pwhash, final String username) {
			//this.startNumber = startNumber;
			//this.endNumber = endNumber;
			this.pwhash = pwhash;
			this.username = username;
		}

		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private PWHashMessage() {
		}

		@Override
		public String toString() {
			return String.format("%s[%,d..%,d]", this.getClass().getSimpleName(), this.username, this.pwhash);
		}
	}
	


	/**
	 * Answer to a {@link PWCrackWorker.PWValidationMessage}. Tells the {@link PWMaster} the password if it was found.
	 */
	public static class PWMessage implements Serializable {

		private static final long serialVersionUID = 4862570515887001983L;

		private int requestId;

		private int password;

		private String user;

		/**
		 * Create a new instance.
		 *
		 * @param requestId  the ID of the query that is being served
		 * @param password   contains the password if found, otherwise -1
		 * @param isComplete atm always true
		 */
		public PWMessage(final int requestId, final int password, final String user) {
			this.requestId = requestId;
			this.password = password;
			this.user = user;
		}
		
		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private PWMessage() {
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) 
				return false;
			final PWMessage that = (PWMessage) o;
			return this.requestId == that.requestId &&
					Objects.equals(this.user, that.user) &&
					Objects.equals(this.password, that.password);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.requestId, this.password, this.user);
		}
	}

	/**
	 * Asks the {@link PWMaster} to schedule work to a new remote actor system.
	 */
	public static class RemoteSystemMessage implements Serializable {

		private static final long serialVersionUID = 2786272840353304769L;

		private Address remoteAddress;

		public RemoteSystemMessage(final Address remoteAddress) {
			this.remoteAddress = remoteAddress;
		}
		
		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private RemoteSystemMessage() {
		}
	}

	// The supervisor strategy for the worker actors created by this master actor
	private static SupervisorStrategy strategy =
			new OneForOneStrategy(0, Duration.create(1, TimeUnit.SECONDS), DeciderBuilder
					.match(Exception.class, e -> stop())
					.matchAny(o -> escalate())
					.build());
	
	// A reference to the listener actor that collects all cracked passwords
	private final ActorRef listener;
	
	// The scheduling strategy that splits range messages into smaller tasks and distributes these to the workers
	private final PWSchedulingStrategy schedulingStrategy;

	// A helper variable to assign unique IDs to each range query
	private int nextQueryId = 0;

	// A flag indicating whether this actor is still accepting new range messages
	private boolean isAcceptingRequests = true;

	/**
	 * Construct a new {@link PWMaster} object.
	 * 
	 * @param listener a reference to an {@link Listener} actor to send results to
	 * @param schedulingStrategyFactory defines which {@link SchedulingStrategy} to use
	 * @param numLocalWorkers number of workers that this master should start locally
	 */
	public PWMaster(final ActorRef listener, PWSchedulingStrategy.PWFactory schedulingStrategyFactory, int numLocalWorkers) {
		
		// Save the reference to the Listener actor
		this.listener = listener;

		// Create a scheduling strategy.
		this.schedulingStrategy = schedulingStrategyFactory.create(this.getSelf());
		
		// Start the specified number of local workers
		for (int i = 0; i < numLocalWorkers; i++) {
			
			// Create a new worker
			ActorRef worker = this.getContext().actorOf(Worker.props());
			this.schedulingStrategy.addWorker(worker);

			// Add the worker to the watch list and our router
			this.getContext().watch(worker);
		}
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// If the master has stopped, it can also stop the listener
		this.listener.tell(PoisonPill.getInstance(), this.getSelf());
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return PWMaster.strategy;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RemoteSystemMessage.class, this::handle)
				.match(PWHashMessage.class, this::handle)
				.match(PWMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RemoteSystemMessage message) {

		// Create a new worker with the given URI
		ActorRef worker = this.getContext().actorOf(Worker.props().withDeploy(new Deploy(new RemoteScope(message.remoteAddress))));
		
		// Add worker to the scheduler
		this.schedulingStrategy.addWorker(worker);

		// Add the worker to the watch list
		this.getContext().watch(worker);

		this.log().info("New worker: " + worker);
	}
	
	private void handle(PWMessage message) {

		// If the worker found the password tell the listener
		if (message.password > 0) { 
			String pw = String.valueOf(message.password);
			while (pw.length() < 7) { // if it's stupid, but working...
				pw = "0" + pw;
			}
		
			// Forward the cracked password to the listener
			this.listener.tell(new ExerciseListener.PWListenerMessage(pw, message.user), this.getSelf());
		}
		// Notify the scheduler that the worker has finished its task
		this.schedulingStrategy.finished(message.requestId, this.getSender());
		
		// Check if work is complete and stop the actor hierarchy if true
		if (this.hasFinished()) {
			this.stopSelfAndListener();
		}
	}

	private void handle(PWHashMessage message) {
		
		// Check if we are still accepting requests
		if (!this.isAcceptingRequests) {
			this.log().warning("Discarding request {}.", message);
			return;
		}

		// Schedule the request
		this.schedulingStrategy.schedule(this.nextQueryId, message.username, message.pwhash);
		this.nextQueryId++;
	}

	private void handle(ShutdownMessage message) {
		
		// Stop receiving new queries
		this.isAcceptingRequests = false;
		
		// Check if work is complete and stop the actor hierarchy if true
		if (this.hasFinished()) {
			this.stopSelfAndListener();
		}
	}
	
	private void handle(Terminated message) {
		
		// Find the sender of this message
		final ActorRef sender = this.getSender();
		
		// Remove the sender from the scheduler
		this.schedulingStrategy.removeWorker(sender);
		
		this.log().warning("{} has terminated.", sender);
		
		// Check if work is complete and stop the actor hierarchy if true
		if (this.hasFinished()) {
			this.stopSelfAndListener();
		}
	}

	private boolean hasFinished() {
		
		// The master has finished if (1) there will be no further requests and (2) either all requests have been processed or there are no more workers to process these requests
		return !this.isAcceptingRequests && (!this.schedulingStrategy.hasTasksInProgress() || this.schedulingStrategy.countWorkers() < 1);
	}

	private void stopSelfAndListener() {
		
		// Tell the listener to stop
		this.listener.tell(new ShutdownMessage(), this.getSelf());
		
		// Stop self and all child actors by sending a poison pill
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}
}