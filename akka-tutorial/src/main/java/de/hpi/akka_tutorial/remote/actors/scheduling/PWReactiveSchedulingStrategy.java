package de.hpi.akka_tutorial.remote.actors.scheduling;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import akka.actor.ActorRef;
import de.hpi.akka_tutorial.remote.actors.PWCrackWorker;

public class PWReactiveSchedulingStrategy implements PWSchedulingStrategy {

	public static class PWFactory implements PWSchedulingStrategy.PWFactory {

		@Override
		public PWSchedulingStrategy create(ActorRef master) {
			return new PWReactiveSchedulingStrategy(master);
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
		private final Map<ActorRef, PWCrackWorker.PWValidationMessage> runningSubqueries = new HashMap<>();

		// Keeps track of failed subqueries, so as to reschedule them to some worker.
		private final Queue<PWCrackWorker.PWValidationMessage> failedSubqueries = new LinkedList<>();

		private final Integer userid;

		private final String pwhash;

		QueryTracker(final int id, final Integer userid, final String pwhash) {
			this.id = id;
			this.remainingRangeStartNumber = 0;
			this.remainingRangeEndNumber = 9_999_999; // 7 digit password 
			this.userid = userid;
			this.pwhash = pwhash;
		}

		boolean assignWork(ActorRef worker, ActorRef master) {

			// Select a failed subquery if any
			PWCrackWorker.PWValidationMessage subquery = this.failedSubqueries.poll();
			
			// Create a new subquery if no failed subquery was selected
			if (subquery == null) {
				int subqueryRangeSize = Math.min(this.remainingRangeEndNumber - this.remainingRangeStartNumber + 1, MAX_SUBQUERY_RANGE_SIZE);
				if (subqueryRangeSize > 0) {
					subquery = new PWCrackWorker.PWValidationMessage(this.id, this.remainingRangeStartNumber, this.remainingRangeStartNumber + subqueryRangeSize - 1, this.userid, this.pwhash);
					this.remainingRangeStartNumber += subqueryRangeSize;
				}
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
			PWCrackWorker.PWValidationMessage failedTask = this.runningSubqueries.remove(worker);
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
			PWCrackWorker.PWValidationMessage completedTask = this.runningSubqueries.remove(worker);
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

	public PWReactiveSchedulingStrategy(ActorRef master) {
		this.master = master;
	}

	@Override
	public void schedule(final int taskId, final Integer userid, final String pwhash) {

		// Create a new tracker for the query
		QueryTracker tracker = new QueryTracker(taskId, userid, pwhash);
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
