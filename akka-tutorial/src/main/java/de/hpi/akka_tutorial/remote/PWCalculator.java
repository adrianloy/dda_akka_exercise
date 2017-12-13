package de.hpi.akka_tutorial.remote;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.PoisonPill;
import de.hpi.akka_tutorial.remote.actors.scheduling.PWReactiveSchedulingStrategy.PWFactory;
import de.hpi.akka_tutorial.remote.actors.scheduling.SSReactiveSchedulingStrategy.SSFactory;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;
import de.hpi.akka_tutorial.Participant;
import de.hpi.akka_tutorial.remote.actors.ExerciseListener;
import de.hpi.akka_tutorial.remote.actors.PWMaster;
import de.hpi.akka_tutorial.remote.actors.SSMaster;
import de.hpi.akka_tutorial.remote.actors.Reaper;
import de.hpi.akka_tutorial.remote.actors.Shepherd;
import de.hpi.akka_tutorial.remote.actors.Slave;
import de.hpi.akka_tutorial.util.AkkaUtils;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class PWCalculator {

	private static final String DEFAULT_MASTER_SYSTEM_NAME = "MasterActorSystem";
	private static final String DEFAULT_SLAVE_SYSTEM_NAME = "SlaveActorSystem";

	private static void shutdown(final ActorRef shepherd, final ActorRef master1, final ActorRef master2) {
		
		// Tell the master that we will not send any further requests and want to shutdown the system after all current jobs finished
		master1.tell(new ShutdownMessage(), ActorRef.noSender());
		master2.tell(new ShutdownMessage(), ActorRef.noSender());
		
		// Do not accept any new subscriptions
		shepherd.tell(new ShutdownMessage(), ActorRef.noSender());
	}
	
	private static void kill(final ActorRef listener, final ActorRef master1, final ActorRef master2, final ActorRef shepherd) {
		
		// End the listener
		listener.tell(PoisonPill.getInstance(), ActorRef.noSender());

		// End the masters
		master1.tell(PoisonPill.getInstance(), ActorRef.noSender());
		master2.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		// End the shepherd
		shepherd.tell(PoisonPill.getInstance(), ActorRef.noSender()); 
	}
	
	public static void awaitTermination(final ActorSystem actorSystem) {
		try {
			Await.ready(actorSystem.whenTerminated(), Duration.Inf());
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("ActorSystem terminated!");
	}

	public static void runSlave(String host, int port, String masterHost, int masterPort) {

		// Create the local ActorSystem
		final Config config = AkkaUtils.createRemoteAkkaConfig(host, port);
		final ActorSystem actorSystem = ActorSystem.create(DEFAULT_SLAVE_SYSTEM_NAME, config);
		
		// Create the reaper.
		actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

		// Create a Slave
		final ActorRef slave = actorSystem.actorOf(Slave.props(), Slave.DEFAULT_NAME);

		// Tell the Slave to register the local ActorSystem
		slave.tell(new Slave.AddressMessage(new Address("akka.tcp", DEFAULT_MASTER_SYSTEM_NAME, masterHost, masterPort)), ActorRef.noSender());
		
		// Await termination: The termination should be issued by the reaper
		PWCalculator.awaitTermination(actorSystem);
	}

	public static void runMaster(String host, int port, PWFactory schedulingStrategyFactory, SSFactory ssfac, int numLocalWorkers, ArrayList<Participant> all_participants) {

		// Create the ActorSystem
		final Config config = AkkaUtils.createRemoteAkkaConfig(host, port);
		final ActorSystem actorSystem = ActorSystem.create(DEFAULT_MASTER_SYSTEM_NAME, config);

		// Create the Reaper.
		actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(ExerciseListener.props(), ExerciseListener.DEFAULT_NAME);

		// Create the Masters
		final ActorRef pwmaster = actorSystem.actorOf(PWMaster.props(listener, schedulingStrategyFactory, numLocalWorkers), PWMaster.DEFAULT_NAME);
		final ActorRef ssmaster = actorSystem.actorOf(SSMaster.props(listener, ssfac, numLocalWorkers), SSMaster.DEFAULT_NAME);

		// Create the Shepherd
		final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(pwmaster), Shepherd.DEFAULT_NAME);

		// Schedule all pw cracking jobs
		for (Participant p : all_participants) {
			pwmaster.tell(new PWMaster.PWHashMessage(p.getName(), p.getPwhash()), ActorRef.noSender());
		}

		// schedule all substring matching jobs, reducing combinations to a minimum
		for (int i = 0; i < all_participants.size(); ++i) { // TODO: change
			for (int j = i+1;  j < all_participants.size(); ++j) {
				//System.out.println("Schedule job: " + all_participants.get(i).getName() + " " +  all_participants.get(j).getName());
				ssmaster.tell(new SSMaster.CompareMessage(all_participants.get(i), all_participants.get(j)), ActorRef.noSender());
			}
		}
		
		PWCalculator.enterInteractiveLoop(listener, pwmaster, ssmaster, shepherd);
		PWCalculator.shutdown(shepherd, pwmaster, ssmaster);
		
		System.out.println("Stopping...");

		// Await termination: The termination should be issued by the reaper
		PWCalculator.awaitTermination(actorSystem);
		
	}
	
	private static void enterInteractiveLoop(final ActorRef listener, final ActorRef master1, final ActorRef master2, final ActorRef shepherd) {
		
		// Read ranges from the console and process them
		final Scanner scanner = new Scanner(System.in);
		while (true) {
			// Sleep to reduce mixing of log messages with the regular stdout messages.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			
			// Read input
			System.out.println("> Enter ...\n"
					+ "  \"exit\" for a graceful shutdown,\n"
					+ "  \"kill\" for a hard shutdown:");
			String line = scanner.nextLine();

			switch (line) {
				case "exit":
					PWCalculator.shutdown(shepherd, master1, master2);
					scanner.close();
					return;
				case "kill":
					PWCalculator.kill(listener, master1, master2, shepherd);
					scanner.close();
					return;
				default:
					System.out.println("Invalid option.");
			}
		}
	}
	
}
