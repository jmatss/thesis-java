package se.kth.ii142x.thesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import se.kth.ii142x.thesis.DTO.ComparisonDTO;
import se.kth.ii142x.thesis.model.Block;

/**
 * In charge of all comparisons during the merge.
 * Creates threads that will get a range of blocks each and do comparisons
 * on those. The MergeHandler will then gather the results in a priority
 * queue and fetch the smallest one of all the blocks.
 *
 * The results will be put in the "minResult" queue that is accessible
 * from the main thread.
 */
class MergeHandler implements Runnable {
	/*
		"minResult" contains complete comparison results that will be fetched from
		the main thread. It will contain the "ultimate" smallest hash that
		hasn't been processed by the main thread yet.
	 */
	private final BlockingQueue<byte[]> minResult;

	/*
		Contains comparisons done by the subthreads that is to be processed by this handler.
		One BlockingQueue in the array corresponds to one thread.
	 */
	private final BlockingQueue<ComparisonDTO>[] pending;

	/*
		Priority queue to store the current smallest items from every thread.
		This will be used to sort the hashes in lg(n) time and the smallest hash
		will be popped and put into the "minResult" queue.
	 */
	private final Queue<ComparisonDTO> priorityQueue;

	private final List<Thread> threads;
	private final List<Block> blocks;

	@SuppressWarnings("unchecked")
	MergeHandler(List<Block> blocks, BlockingQueue<byte[]> result, int amountOfThreads, int queueSize) {
		this.blocks = blocks;
		this.threads = new ArrayList<>(amountOfThreads);

		this.minResult = result;
		this.pending = new ArrayBlockingQueue[amountOfThreads];
		this.priorityQueue = new PriorityQueue<>(amountOfThreads);

		initThreads(amountOfThreads, queueSize);
	}

	private void initThreads(int amountOfThreads, int queueSize) {
		// If less blocks than threads, limit amount of threads
		if (this.blocks.size() < amountOfThreads) {
			amountOfThreads = this.blocks.size();
		}

		// ~range of blocks that every thread will take
		int threadRange = this.blocks.size() / amountOfThreads;
		int currentStart, currentEnd;

		// Create the threads that does comparisons over a range of blocks.
		for(int i = 0; i < amountOfThreads; i++) {
			// every thread has its own buffer that it writes its comparison results to
			this.pending[i] = new ArrayBlockingQueue<>(queueSize / amountOfThreads);

			currentStart = i * threadRange;
			currentEnd = (i + 1) * threadRange - 1;

			// if true: this it the last thread,
			// take rest of blocks to prevent "overflow"
			if (i == amountOfThreads - 1) {
				currentEnd = this.blocks.size() - 1;
			}

			MergeHandlerThread mht = new MergeHandlerThread(i, currentStart, currentEnd,
					this.blocks, this.pending[i]);
			this.threads.add(new Thread(mht));
		}
	}

	@Override
	public void run() {
		try {
			for (Thread thread : this.threads) {
				thread.start();
			}

			// populate the priority queue with "ultimate" minimum from every thread
			for (int threadId = 0; threadId < this.threads.size(); threadId++) {
				this.priorityQueue.add(this.pending[threadId].take());
			}

			ComparisonDTO current, next;

			while(true) {
				if (this.priorityQueue.isEmpty())
					break;

				// remove min from priority queue. Get next min from same thread (i.e. same block range)
				current = this.priorityQueue.poll();
				next = this.pending[current.getId()].take();

				// if next == null: this thread is done and all its hashes have been processed/sorted
				if (next.getHash() != null)
					this.priorityQueue.add(next);

				this.minResult.put(current.getHash());
			}

			// Done executing, BlockingQueue doesn't allow null, so use byte[].length != 16 as indicator
			// for the main thread that everything is finished.
			this.minResult.put(new byte[]{0});

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		for (Thread thread : this.threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * A subthread that will do comparisons on a range of blocks from "start" through "end".
	 * Returns results into the parent MergeHandler's "pending" array with the index
	 * corresponding to this threads "threadId".
	 */
	private class MergeHandlerThread implements Runnable {
		private final int threadId;
		private final int start;
		private final int end;

		private final List<Block> blocks;
		private final BlockingQueue<ComparisonDTO> pending;
		private final Queue<ComparisonDTO> priorityQueue;

		private MergeHandlerThread(int threadId, int start, int end,
								   List<Block> blocks, BlockingQueue<ComparisonDTO> pending) {
			this.threadId = threadId;
			this.start = start;
			this.end = end;

			this.blocks = blocks;
			this.pending = pending;
			this.priorityQueue = new PriorityQueue<>(end - start + 1);
		}

		// TODO: This method is very similar to the run of "run" function of the parent
		//  handler, possible to merge?
		@Override
		public void run() {
			try {
				// populate the priority queue with "ultimate" minimum from every block
				for (int i = this.start; i <= this.end; i++) {
					this.priorityQueue.add(
							new ComparisonDTO(i, this.blocks.get(i).pop())
					);
				}

				ComparisonDTO current, next;

				while(true) {
					if (this.priorityQueue.isEmpty())
						break;

					// Remove min from priority queue and get next min from same block.
					current = this.priorityQueue.poll();
					next = new ComparisonDTO(
							current.getId(),
							this.blocks.get(current.getId()).pop()
					);

					// If next == null: this block is done, dont add next to priority queue.
					if (next.getHash() != null)
						this.priorityQueue.add(next);

					this.pending.put(new ComparisonDTO(threadId, current.getHash()));
				}

				// Use null to indicate that this thread is finished.
				this.pending.put(new ComparisonDTO(threadId, null));

			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}
}