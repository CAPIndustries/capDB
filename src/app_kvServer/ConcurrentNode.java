package app_kvServer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConcurrentNode {
	private Queue<int[]> q;
	private boolean deleted;
	
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

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
}
