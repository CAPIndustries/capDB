package app_kvServer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

public class ConcurrentNode {
	private Queue<int[]> q;
	private boolean deleted;
	private Semaphore read; // semaphore to allow multiple reads
	private ScheduledFuture<?> deleteThread;

	public ConcurrentNode(int max_reads) {
		this.q = new ConcurrentLinkedQueue<int[]>();
		this.deleted = false;
		this.read = new Semaphore(max_reads);
	}

	public int availablePermits() {
		return this.read.availablePermits();
	}

	public void acquire() throws InterruptedException {
		this.read.acquire();
	}

	public void release() {
		this.read.release();
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
		}, 5, TimeUnit.SECONDS);
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

	public String printQ() {
		StringBuilder res = new StringBuilder();
		for (int[] node : this.q) {
			StringBuilder subRes = new StringBuilder();
			for (int i = 0; i < node.length; ++i) {
				subRes.append(node[i]);
				if (i != node.length-1) subRes.append(".");
			}
			res.append(subRes.toString() + " ");
		}

		return res.toString();
	}

}
