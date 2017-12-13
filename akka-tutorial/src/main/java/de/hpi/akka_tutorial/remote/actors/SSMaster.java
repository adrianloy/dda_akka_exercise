package de.hpi.akka_tutorial.remote.actors;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;
import com.typesafe.config.ConfigException;
import de.hpi.akka_tutorial.Participant;
import de.hpi.akka_tutorial.remote.actors.scheduling.SSSchedulingStrategy;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.stop;

/**
 * The master receives password hashes and ranges of possible passwords to brute force it. This is done by delegation to slaves.
 */
public class SSMaster extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "ssmaster";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link SSMaster} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props(final ActorRef listener, SSSchedulingStrategy.Factory schedulingStrategyFactory, final int numLocalWorkers) {
		return Props.create(SSMaster.class, () -> new SSMaster(listener, schedulingStrategyFactory, numLocalWorkers));
	}

	/**
	 * Answer to a {@link SSWorker.PWValidationMessage}. Tells the {@link SSMaster} the longest matching SS (substring) for a pair.
	 */
	public static class FinalizedMessage implements Serializable {

		private static final long serialVersionUID = 27870728456304769L;

		private int requestId;
		private Participant participant1;
		private Participant participant2;

		public FinalizedMessage(final int id, final Participant p1, final Participant p2) {
			this.requestId = id;
			this.participant1 = p1;
			this.participant2 = p2;
		}

		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private FinalizedMessage() {
		}
	}

	/**
	 * Asks the {@link SSMaster} to schedule work to a new remote actor system.
	 */
	public static class CompareMessage implements Serializable {

		private static final long serialVersionUID = 2786272840353304769L;

		private Participant participant1;
		private Participant participant2;

		public CompareMessage(final Participant p1, final Participant p2) {
			this.participant1 = p1;
			this.participant2 = p2;
		}

		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private CompareMessage() {
		}
	}

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
	private final SSSchedulingStrategy schedulingStrategy;

	// A helper variable to assign unique IDs to each range query
	private int nextQueryId = 0;

	// A flag indicating whether this actor is still accepting new range messages
	private boolean isAcceptingRequests = true;

	/**
	 * Construct a new {@link SSMaster} object.
	 *
	 * @param listener a reference to an {@link Listener} actor to send results to
	 * @param schedulingStrategyFactory defines which {@link SchedulingStrategy} to use
	 * @param numLocalWorkers number of workers that this master should start locally
	 */
	public SSMaster(final ActorRef listener, SSSchedulingStrategy.Factory schedulingStrategyFactory, int numLocalWorkers) {
		
		// Save the reference to the Listener actor
		this.listener = listener;

		// Create a scheduling strategy.
		this.schedulingStrategy = schedulingStrategyFactory.create(this.getSelf());
		
		// Start the specified number of local workers
		for (int i = 0; i < numLocalWorkers; i++) {
			
			// Create a new worker
			ActorRef worker = this.getContext().actorOf(SSWorker.props());
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
		return SSMaster.strategy;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(RemoteSystemMessage.class, this::handle)
				.match(CompareMessage.class, this::handle)
				.match(FinalizedMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(RemoteSystemMessage message) {

		// Create a new worker with the given URI
		ActorRef worker = this.getContext().actorOf(SSWorker.props().withDeploy(new Deploy(new RemoteScope(message.remoteAddress))));
		
		// Add worker to the scheduler
		this.schedulingStrategy.addWorker(worker);

		// Add the worker to the watch list
		this.getContext().watch(worker);

		this.log().info("New worker: " + worker);
	}
	
	private void handle(FinalizedMessage message) {
		// If the worker found the ss tell the listener
		if (message.participant1.getDna_match_partner_id() > -1) {
			// Forward the participants with its ss attribute set to the listener
			//this.log().info("told the lister that we found a ss");
			this.listener.tell(new ExerciseListener.SSListenerMessage(message.participant1, message.participant2), this.getSelf());
		}
		else {
			//System.out.println("weird. should not happen. master got finalize message but there is no dna match set");
		}
		// Notify the scheduler that the worker has finished its task
		this.schedulingStrategy.finished(message.requestId, this.getSender());
		
		// Check if work is complete and stop the actor hierarchy if true
		if (this.hasFinished()) {
			this.stopSelfAndListener();
		}
	}

	private void handle(CompareMessage message) {
		
		// Check if we are still accepting requests
		if (!this.isAcceptingRequests) {
			this.log().warning("Discarding request {}.", message);
			return;
		}

		// Schedule the request
		//System.out.println(" master schedules query with id " + this.nextQueryId);
		this.schedulingStrategy.schedule(this.nextQueryId, message.participant1, message.participant2);
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