package de.hpi.akka_tutorial.remote.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.akka_tutorial.Participant;

/**
 * The worker waits tests ranges of numbers for passwords.
 */
public class SSWorker extends AbstractLoggingActor {

	/**
	 * Create the {@link Props} necessary to instantiate new {@link SSWorker} actors.
	 *
	 * @return the {@link Props}
	 */
	public static Props props() {
		return Props.create(SSWorker.class);
	}

	/**
	 * Asks the {@link SSWorker} to get the longest common substring of two participants.
	 */
	public static class SSValidationMessage implements Serializable {
		
		private static final long serialVersionUID = -7467053227355130231L;
		
		private int id;
		
		private Participant p1;
		
		private Participant p2;
		
		/**
		 * Construct a new {@link PWValidationMessage} object.
		 * 
		 * @param id the id of the task that this range belongs to
		 * @param rangeMin first number in the range to be checked as password (inclusive)
		 * @param rangeMax last number in the range to be checked as password (inclusive)
		 */
		public SSValidationMessage(int id, Participant p1, Participant p2) {
			this.id = id;
			this.p1 = p1;
			this.p2 = p2;
		}
		
		/**
		 * For serialization/deserialization only.
		 */
		@SuppressWarnings("unused")
		private SSValidationMessage() {
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
				.match(SSValidationMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(SSValidationMessage message) {
		
		// Log that we started processing the current task
		this.log().info("Start searching for the longest common substring between [{},{}]", message.p1.getName(), message.p2.getName());

		String ss = getLongestCommonSubstring(message.p1.getDna(), message.p2.getDna());
		this.log().info("Found longest common substring between [{},{}], its {}", message.p1.getName(), message.p2.getName(), ss);
		Participant p1 = message.p1;
		Participant p2 = message.p2;
		p1.setDna_match_partner_id(p2.getId());
		p2.setDna_match_partner_id(p1.getId());
		p1.setDna_match(ss);
		p2.setDna_match(ss);

		this.getSender().tell(new SSMaster.FinalizedMessage(message.id, p1, p2), this.getSelf());
	}
	
	/**
	 * This function implements the dynamic programming approach 
	 * to solve the longest common substring problem for two strings
	 * @return A String containing the longest common substring
	 */
	private String getLongestCommonSubstring(String a, String b) {
		if (a.length() != b.length()) {
			System.out.println("DNA strings do not have the same length");
		}
		int[][] D =  new int[a.length()+1][b.length()+1]; //+1 to have extra coloumn and row with 0s
		int max = 0;
		int[] max_pos = new int[2];
		for (int i=1; i < D.length;i++) {
			for (int k=1; k<D[0].length;k++) {
				if (a.charAt(i-1) == (b.charAt(k-1))){
					D[i][k] = D[i-1][k-1] + 1;
				}
				else {
					D[i][k] = 0;
				}
				if (D[i][k] > max) { //keep track of maximum and its position
					max_pos[0] = i;
					max_pos[1] = k;
					max = D[i][k];
				}
			}
		}
		//retrieve substring using max_help
		int start_idx = max_pos[0]- max;
		String lcsb = a.substring(start_idx, max_pos[0]);
		String lcsa = b.substring(max_pos[1]-max, max_pos[1]);
		// sanity check
		if (!lcsb.equals(lcsa)) {
			//log a problem
			this.log().info("Something ain't right. This worker seems to produce a wrong result");

		}
		//System.out.println(lcsa);
		//System.out.println(lcsb);
		return lcsa;
	}
}