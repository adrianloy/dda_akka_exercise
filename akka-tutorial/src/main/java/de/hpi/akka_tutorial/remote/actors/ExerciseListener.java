package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import akka.actor.AbstractLoggingActor;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;

/**
 * The listener collects password hashes and responds to action requests.
 */
public class ExerciseListener extends AbstractLoggingActor {

	public static final String DEFAULT_NAME = "exerciselistener";

	/**
	 * Create the {@link Props} necessary to instantiate new {@link ExerciseListener} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(ExerciseListener.class);
	}

	/**
	 * Asks the {@link ExerciseListener} to store a given password to a user.
	 */
	public static class PWListenerMessage implements Serializable {
		
		private static final long serialVersionUID = -1779142448823490939L;

		private String password;
		
		private String user;
		
		/**
		 * Construct a new {@link PrimesMessage} object.
		 * 
		 * @param primes A list of prime numbers
		 */
		public PWListenerMessage(final String password, final String user) {
			this.password = password;
			this.user = user;
		}

		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private PWListenerMessage() {
		}
	}
	
	// The set of all users and passwords received by this listener actor
	private final Map<String, String> pw_map = new HashMap<String, String>();
	
	@Override
	public void preStart() throws Exception {
		super.preStart();
		
		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}


	@Override
	public void postStop() throws Exception {
		super.postStop();
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PWListenerMessage.class, this::handle)
				.match(ShutdownMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}
	
	private void handle(PWListenerMessage message) {
		this.pw_map.put(message.user, message.password);
	}
	
	private void handle(ShutdownMessage message) {
		// We could write all primes to disk here
		
		this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
	}
	
}
