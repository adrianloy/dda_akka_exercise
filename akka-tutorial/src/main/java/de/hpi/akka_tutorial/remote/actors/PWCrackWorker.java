package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;

/**
 * The worker waits tests ranges of numbers for passwords.
 */
public class PWCrackWorker extends AbstractLoggingActor {

	/**
	 * Create the {@link Props} necessary to instantiate new {@link PWCrackWorker} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(PWCrackWorker.class);
	}

	/**
	 * Asks the {@link PWCrackWorker} to brute force a password in a given range.
	 */
	public static class PWValidationMessage implements Serializable {
		
		private static final long serialVersionUID = -7467053227355130231L;
		
		private int id;

		private int rangeMin;

		private int rangeMax;
		
		private String user;
		
		private String pwhash;
		
		/**
		 * Construct a new {@link PWValidationMessage} object.
		 * 
		 * @param id the id of the task that this range belongs to
		 * @param rangeMin first number in the range to be checked as password (inclusive)
		 * @param rangeMax last number in the range to be checked as password (inclusive)
		 */
		public PWValidationMessage(int id, int rangeMin, int rangeMax, String user, String pwhash) {
			this.id = id;
			this.rangeMin = rangeMin;
			this.rangeMax = rangeMax;
			this.pwhash = pwhash;
			this.user = user;
		}
		
		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private PWValidationMessage() {
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
		
		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(PWValidationMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(PWValidationMessage message) {
		
		// Log that we started processing the current task
		this.log().info("Brute force values [start range, end range, hash]: [{},{}] ...", message.rangeMin, message.rangeMax);

		// Iterate over the range of numbers and check if we cracked the hash
		for (int i = message.rangeMin; i <= message.rangeMax; i++) {
			if (isPassword(i, message.pwhash)) {
				
				// Found the password. Tell master and stop checking.
				this.getSender().tell(new PWMaster.PWMessage(message.id, i, message.user), this.getSelf());
				return;
				
			}
		}

		// Couldn't find the password in that range, return null
		this.getSender().tell(new PWMaster.PWMessage(message.id, -1, message.user), this.getSelf());

		// Asynchronous version: Consider using a dedicated executor service.
//		ActorRef sender = this.getSender();
//		ActorRef self = this.getSelf();
//		getContext().getSystem().dispatcher().execute(() -> {
//			final List<Object> result = new ArrayList<>();
//			for (Long number : message.getNumbers())
//				if (this.isPrime(number.longValue()))
//					result.add(number);
//
//			sender.tell(new Master.ObjectMessage(message.getId(), result), self);
//		});
	}

	private boolean isPassword(int i, String pwhash) {
		// Check if i as a 7 character string (e.g. 0000204) has a SHA-256 value equal to pwhash
		String pw2 = String.valueOf(i);
		while (pw2.length() < 7) { // if it's stupid, but working...
			pw2 = "0" + pw2;
		}
		try {
			String pw2_hash = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(pw2.getBytes("UTF-8"))));
			
			return pw2_hash.equals(pwhash);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
}