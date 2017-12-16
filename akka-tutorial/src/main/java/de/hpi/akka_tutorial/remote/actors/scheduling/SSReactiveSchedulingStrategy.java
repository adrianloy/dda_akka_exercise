package de.hpi.akka_tutorial.remote.actors.scheduling;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.scheduling.SSSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.SSWorker;
import de.hpi.akka_tutorial.remote.actors.SSWorker.SSValidationMessage;
import de.hpi.akka_tutorial.Participant;

import java.util.*;
import java.util.stream.Collectors;

public class SSReactiveSchedulingStrategy implements SSSchedulingStrategy {

	/**
	 * {@link SchedulingStrategy.Factory} implementation for the {@link SSReactiveSchedulingStrategy}.
	 */
	public static class SSFactory implements SSSchedulingStrategy.Factory {

		@Override
		public SSSchedulingStrategy create(ActorRef master) {
			return new SSReactiveSchedulingStrategy(master);
		}
	}

	/**
	 * This class supervises the state of a query to find the longest ss of two participants.
	 */
	private class QueryTracker {

		// This is the ID of the query that is being tracked.
		private final int id;

		
		// keeps track of the actor processing our query
		private ActorRef myworker;
		
		// Keeps track if the query failed to reschedule it
		private boolean failed = true;
		
		//keep track if we are done
		private boolean finito = false;
		
		// Keeps track of the currently posed subqueries and which actor is processing it.
		//private final Map<ActorRef, SSWorker.SSValidationMessage> runningSubqueries = new HashMap<>();

		// Keeps track of failed subqueries, so as to reschedule them to some worker.
		//private final Queue<SSWorker.SSValidationMessage> failedSubqueries = new LinkedList<>();

		private Participant p1;
		private Participant p2;

		QueryTracker(final int id, Participant p1, Participant p2) {
			this.id = id;
			this.p1 = p1;
			this.p2 = p2;
			this.failed = false;
			this.finito = false;
			this.myworker = null;
		}

		/**
		 * Assign a query of the tracked query to the worker. If a subquery was available, a {@link SSWorker.SSValidationMessage} is send to the worker with master as sender.
		 *
		 * @return a new subquery or {@code null}
		 */
		boolean assignWork(ActorRef worker, ActorRef master) {
			//System.out.println("try to  assign work to query with id " + this.id);
			// we can only assign work to a worker if we dont already have a worker that is doing the job
			if (this.myworker != null) {
				//System.out.println("  na im busy. cant assign");
				return false;
			}
			else {
				//System.out.println("  did it");
				SSValidationMessage query = new SSWorker.SSValidationMessage(this.id, this.p1, this.p2);
				worker.tell(query, master);
				this.myworker = worker;
				this.failed = false;
				return true;
			}			
			/**
			// Select a failed subquery if any
			SSWorker.SSValidationMessage subquery = this.failedSubqueries.poll();

			// Create a new subquery if no failed subquery was selected
			if (subquery == null) {
				subquery = new SSWorker.SSValidationMessage(this.id, this.p1, this.p2);
			}
			else {
			// Return false if no work was assigned
				return false;
			}

			// Assign and send the subquery to the worker
			worker.tell(subquery, master);
			this.runningSubqueries.put(worker, subquery);

			return true;
			**/
		}

		/**
		 * Handle the failure of this query. That is, prepare to re-schedule the failed subquery.
		 *
		 * @param worker the actor that just failed
		 */
		void workFailed(ActorRef worker) {
			this.failed = true;
			this.myworker = null;
		}

		/**
		 * Handle the completion of a subquery.
		 *
		 * @param worker the actor that just completed
		 */
		void workCompleted(ActorRef worker) {
			this.finito = true;
		}

		public boolean isComplete() {
			return this.finito && ! this.failed;
		}
	}


	// A mapping of pending range queries to the query tracker that watches the progress of each range query; the queries are kept in their insertion order
	private final LinkedHashMap<Integer, QueryTracker> queryId2tracker = new LinkedHashMap<>();

	// A mapping of known works to their current task
	private final Map<ActorRef, QueryTracker> worker2tracker = new HashMap<>();

	// A reference to the actor in whose name we send messages
	private final ActorRef master;

	public SSReactiveSchedulingStrategy(ActorRef master) {
		this.master = master;
	}

	@Override
	public void schedule(final int taskId, final Participant p1, final Participant p2) {

		// Create a new tracker for the query
		QueryTracker tracker = new QueryTracker(taskId, p1, p2);
		this.queryId2tracker.put(tracker.id, tracker);
		//System.out.println("schedule job with trackerid " + taskId);
		// Assign existing, possible free, workers to the new query
		this.assignQueries();
	}

	@Override
	public boolean hasTasksInProgress() {
		return !this.queryId2tracker.isEmpty();
	}

	@Override
	public void finished(final int taskId, final ActorRef worker) {

		//System.out.println("TASK IS finished: " + taskId + " ");
		// Find the query being processed
		QueryTracker queryTracker = this.queryId2tracker.get(taskId);
		
		// mark query as free
		queryTracker.workCompleted(worker);
		// Mark the worker as free
		this.worker2tracker.put(worker, null);

		// Check if the query is complete
		if (queryTracker.isComplete()) {
			// Remove the query tracker
			this.queryId2tracker.remove(queryTracker.id);
			//System.out.println("REMOVE TASK FROM TRACKER");
		} else {
			// Re-assign the now free worker
			//System.out.println("Work aint done " + queryTracker.id + " ");
			this.assignQueries();
		}
		this.assignQueries();

	}

	@Override
	public void addWorker(final ActorRef worker) {

		// Add the new worker
		this.worker2tracker.put(worker, null);

		// Assign possibly open subqueries to the new worker
		this.assignQueries();
	}

	@Override
	public void removeWorker(final ActorRef worker) {

		// Remove the worker from the list of workers
		QueryTracker processedTracker = this.worker2tracker.remove(worker);

		// If the worker was processing some query, then we need to re-schedule this query
		if (processedTracker != null) {
			processedTracker.workFailed(worker);

			// We might have some free workers that could process the re-scheduled subquery
			this.assignQueries();
		}
	}

	private void assignQueries() {

		// Collect all currently idle workers
		Collection<ActorRef> idleWorkers = this.worker2tracker.entrySet().stream()
				.filter(e -> e.getValue() == null)
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());
		
		// Assign idle workers to queries as long as there is work to do
		Iterator<QueryTracker> queryTrackerIterator = this.queryId2tracker.values().iterator();
		for (ActorRef idleWorker : idleWorkers) {
			QueryTracker queryTracker;
			
			// Find a query tracker that can assign a query to this idle worker
			do {
				// Check if there is any (further) on-going query
				if (!queryTrackerIterator.hasNext()) 
					return;
				
				// Select the (next) query tracker
				queryTracker = queryTrackerIterator.next();
			}
			while (!queryTracker.assignWork(idleWorker, this.master));

			// Assign the query to the worker and keep track of the assignment
			this.worker2tracker.put(idleWorker, queryTracker);
		}
	}

	@Override
	public int countWorkers() {
		return this.worker2tracker.keySet().size();
	}
}
