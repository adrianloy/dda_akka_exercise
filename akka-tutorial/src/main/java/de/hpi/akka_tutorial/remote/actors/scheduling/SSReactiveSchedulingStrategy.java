package de.hpi.akka_tutorial.remote.actors.scheduling;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.scheduling.SSSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.SSWorker;
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
	 * This class supervises the state of a range query for password cracking.
	 */
	private class QueryTracker {

		// Give each worker at most this many numbers at once to check.
		private final int MAX_SUBQUERY_RANGE_SIZE = 100_000;

		// The range of values that was not yet scheduled to workers.
		private int remainingRangeStartNumber, remainingRangeEndNumber;

		// This is the ID of the query that is being tracked.
		private final int id;

		// Keeps track of the currently posed subqueries and which actor is processing it.
		private final Map<ActorRef, SSWorker.SSValidationMessage> runningSubqueries = new HashMap<>();

		// Keeps track of failed subqueries, so as to reschedule them to some worker.
		private final Queue<SSWorker.SSValidationMessage> failedSubqueries = new LinkedList<>();

		private Participant p1;
		private Participant p2;

		QueryTracker(final int id, Participant p1, Participant p2) {
			this.id = id;
			this.p1 = p1;
			this.p2 = p2;
		}

		/**
		 * Assign a subquery of the tracked query to the worker. If a subquery was available, a {@link SSWorker.SSValidationMessage} is send to the worker with master as sender.
		 *
		 * @return a new subquery or {@code null}
		 */
		boolean assignWork(ActorRef worker, ActorRef master) {

			// Select a failed subquery if any
			SSWorker.SSValidationMessage subquery = this.failedSubqueries.poll();

			// Create a new subquery if no failed subquery was selected
			if (subquery == null) {
				subquery = new SSWorker.SSValidationMessage(this.id, this.p1, this.p2);
			}

			// Return false if no work was assigned
			if (subquery == null) {
				return false;
			}

			// Assign and send the subquery to the worker
			worker.tell(subquery, master);
			this.runningSubqueries.put(worker, subquery);

			return true;
		}

		/**
		 * Handle the failure of a subquery. That is, prepare to re-schedule the failed subquery.
		 *
		 * @param worker the actor that just failed
		 */
		void workFailed(ActorRef worker) {
			SSWorker.SSValidationMessage failedTask = this.runningSubqueries.remove(worker);
			if (failedTask != null) {
				this.failedSubqueries.add(failedTask);
			}
		}

		/**
		 * Handle the completion of a subquery.
		 *
		 * @param worker the actor that just completed
		 */
		void workCompleted(ActorRef worker) {
			SSWorker.SSValidationMessage completedTask = this.runningSubqueries.remove(worker);
			assert completedTask != null;
		}

		/**
		 * Check whether this query is complete, i.e., there are no more open or running subqueries.
		 *
		 * @return whether this query is complete
		 */
		boolean isComplete() {
			return this.runningSubqueries.isEmpty()
					&& this.failedSubqueries.isEmpty()
					&& this.remainingRangeStartNumber > this.remainingRangeEndNumber;
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

		// Assign existing, possible free, workers to the new query
		this.assignSubqueries();
	}

	@Override
	public boolean hasTasksInProgress() {
		return !this.queryId2tracker.isEmpty();
	}

	@Override
	public void finished(final int taskId, final ActorRef worker) {
		
		// Find the query being processed
		QueryTracker queryTracker = this.queryId2tracker.get(taskId);

		// Mark the worker as free
		queryTracker.workCompleted(worker);
		this.worker2tracker.put(worker, null);

		// Check if the query is complete
		if (queryTracker.isComplete()) {
			// Remove the query tracker
			this.queryId2tracker.remove(queryTracker.id);
		} else {
			// Re-assign the now free worker
			this.assignSubqueries();
		}
	}

	@Override
	public void addWorker(final ActorRef worker) {

		// Add the new worker
		this.worker2tracker.put(worker, null);

		// Assign possibly open subqueries to the new worker
		this.assignSubqueries();
	}

	@Override
	public void removeWorker(final ActorRef worker) {

		// Remove the worker from the list of workers
		QueryTracker processedTracker = this.worker2tracker.remove(worker);

		// If the worker was processing some subquery, then we need to re-schedule this subquery
		if (processedTracker != null) {
			processedTracker.workFailed(worker);

			// We might have some free workers that could process the re-scheduled subquery
			this.assignSubqueries();
		}
	}

	private void assignSubqueries() {

		// Collect all currently idle workers
		Collection<ActorRef> idleWorkers = this.worker2tracker.entrySet().stream()
				.filter(e -> e.getValue() == null)
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());
		
		// Assign idle workers to subqueries as long as there is work to do
		Iterator<QueryTracker> queryTrackerIterator = this.queryId2tracker.values().iterator();
		for (ActorRef idleWorker : idleWorkers) {
			QueryTracker queryTracker;
			
			// Find a query tracker that can assign a subquery to this idle worker
			do {
				// Check if there is any (further) on-going query
				if (!queryTrackerIterator.hasNext()) 
					return;
				
				// Select the (next) query tracker
				queryTracker = queryTrackerIterator.next();
			}
			while (!queryTracker.assignWork(idleWorker, this.master));

			// Assign the subquery to the worker and keep track of the assignment
			this.worker2tracker.put(idleWorker, queryTracker);
		}
	}

	@Override
	public int countWorkers() {
		return this.worker2tracker.keySet().size();
	}
}
