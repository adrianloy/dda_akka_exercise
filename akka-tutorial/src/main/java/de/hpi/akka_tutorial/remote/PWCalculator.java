package de.hpi.akka_tutorial.remote;

import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.PoisonPill;
import de.hpi.akka_tutorial.remote.actors.scheduling.PWReactiveSchedulingStrategy.Factory;
import de.hpi.akka_tutorial.remote.actors.scheduling.SchedulingStrategy;
import de.hpi.akka_tutorial.remote.messages.ShutdownMessage;
import de.hpi.akka_tutorial.Participant;
import de.hpi.akka_tutorial.remote.actors.ExerciseListener;
import de.hpi.akka_tutorial.remote.actors.Listener;
import de.hpi.akka_tutorial.remote.actors.Master;
import de.hpi.akka_tutorial.remote.actors.PWMaster;
import de.hpi.akka_tutorial.remote.actors.Reaper;
import de.hpi.akka_tutorial.remote.actors.Shepherd;
import de.hpi.akka_tutorial.remote.actors.Slave;
import de.hpi.akka_tutorial.util.AkkaUtils;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class PWCalculator {

	private static final String DEFAULT_MASTER_SYSTEM_NAME = "MasterActorSystem";
	private static final String DEFAULT_SLAVE_SYSTEM_NAME = "SlaveActorSystem";

	private static void shutdown(final ActorRef shepherd, final ActorRef master) {
		
		// Tell the master that we will not send any further requests and want to shutdown the system after all current jobs finished
		master.tell(new ShutdownMessage(), ActorRef.noSender());
		
		// Do not accept any new subscriptions
		shepherd.tell(new ShutdownMessage(), ActorRef.noSender());
	}
	
	private static void kill(final ActorRef listener, final ActorRef master, final ActorRef shepherd) {
		
		// End the listener
		listener.tell(PoisonPill.getInstance(), ActorRef.noSender());

		// End the master
		master.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
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

	public static void runMaster(String host, int port, Factory schedulingStrategyFactory, int numLocalWorkers, HashSet<Participant> all_participants) {
		
		// Create the ActorSystem
		final Config config = AkkaUtils.createRemoteAkkaConfig(host, port);
		final ActorSystem actorSystem = ActorSystem.create(DEFAULT_MASTER_SYSTEM_NAME, config);

		// Create the Reaper.
		actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

		// Create the Listener
		final ActorRef listener = actorSystem.actorOf(ExerciseListener.props(), Listener.DEFAULT_NAME);

		// Create the Master
		final ActorRef master = actorSystem.actorOf(PWMaster.props(listener, schedulingStrategyFactory, numLocalWorkers), PWMaster.DEFAULT_NAME);

		// Create the Shepherd
		final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(master), Shepherd.DEFAULT_NAME);
		
		// Schedule all pw cracking jobs
		for (Participant p : all_participants) {
			master.tell(new PWMaster.PWHashMessage(p.getName(), p.getPwhash()), ActorRef.noSender());
		}
		PWCalculator.shutdown(shepherd, master);
		
		System.out.println("Stopping...");

		// Await termination: The termination should be issued by the reaper
		PWCalculator.awaitTermination(actorSystem);
		
	}
}
