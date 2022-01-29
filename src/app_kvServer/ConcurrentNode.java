package app_kvServer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ConcurrentNode {
	private Queue<int[]> q;
	private boolean deleted;
	ScheduledFuture<?> deleteThread;

	public ConcurrentNode() {
		this.q = new ConcurrentLinkedQueue<int[]>();
		this.deleted = false;
	}

	public int[] peek() {
		return q.peek();
	}

	public void remove() {
		q.remove();
	}

	public void addToQueue(int[] node) {
		q.add(node);
	}

	public boolean isEmpty() {
		return q.isEmpty();
	}

	public int len() {
		return q.size();
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public void startPruning(final Runnable pruneDelete) {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		deleteThread = scheduler.schedule(new Runnable() {
			@Override
			public void run() {
				System.out.println("Starting pruning thread");
				pruneDelete.run();
			}
		}, 0, TimeUnit.MILLISECONDS);
	}

	public void stopPruning() {
		if (deleteThread != null) {
			deleteThread.cancel(false);
		}
	}

	public void setQ(Queue<int[]> q2) {
		this.q = q2;
	}

	public Queue<int[]> getQ() {
		return this.q;
	}

}
