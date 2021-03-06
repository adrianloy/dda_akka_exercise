package de.hpi.akka_tutorial.remote.actors.scheduling;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.PWCrackWorker;

public interface PWSchedulingStrategy {

	/**
	 * A factory for a {@link PWSchedulingStrategy}.
	 */
	interface PWFactory {

		/**
		 * Create a new {@link PWSchedulingStrategy}.
		 *
		 * @param master that will employ the new instance
		 * @return the new {@link PWSchedulingStrategy}
		 */
		PWSchedulingStrategy create(ActorRef master);

	}

	/**
	 * Schedule a new prime checking task in the given range.
	 *
	 * @param taskId the id of the task that is to be split and scheduled
	 * @param startNumber first number of the range
	 * @param endNumber last number of the range
	 */
	void schedule(final int taskId, final Integer username, final String pwhash);

	/**
	 * Notify the completion of a worker's task.
	 *
	 * @param taskId the id of the task this worker was working on
	 * @param worker the reference to the worker who finished the task
	 */
	void finished(final int taskId, final ActorRef worker);

	/**
	 * Check if there are still any pending tasks.
	 *
	 * @return {@code true} if tasks are still pending
	 */
	boolean hasTasksInProgress();

	/**
	 * Add a new {@link Worker} actor.
	 *
	 * @param worker the worker actor to add
	 */
	void addWorker(final ActorRef worker);

	/**
	 * Remove a {@link Worker} actor.
	 *
	 * @param worker the worker actor to remove
	 */
	void removeWorker(final ActorRef worker);

	/**
	 * Count the number of active {@link Worker} actors.
	 */
	int countWorkers();
}
