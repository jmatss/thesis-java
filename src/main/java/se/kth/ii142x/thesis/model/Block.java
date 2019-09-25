package se.kth.ii142x.thesis.model;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.List;
import se.kth.ii142x.thesis.CreateSortedWordlist;
import se.kth.ii142x.thesis.util.Quicksort;

public class Block {
	private final static String ENCODING = "UTF-8";

	private final long start;
	private final long end;
	private final int amountOfThreads;
	private final int amountOfHashes;
	private final String filename;
	private ByteBuffer buffer;  // store as bytes to speed up disk IO
	private ReverseFileReader reader;

	public Block(long start, long end, int blockId,
				 int amountOfThreads, String filename) {
		this.start = start;
		this.end = end;
		this.amountOfHashes = (int)(end - start + 1);
		this.amountOfThreads = amountOfThreads;
		this.filename = filename + blockId;

		this.buffer = ByteBuffer.allocate(this.amountOfHashes * CreateSortedWordlist.HASH_SIZE);
	}

	/**
	 * Produces hashes and puts them into the this.buffer
	 */
	public void generateHashes() throws NoSuchAlgorithmException, InterruptedException, ExecutionException {
		ExecutorService executor = Executors.newFixedThreadPool(amountOfThreads);
		List<Future<Boolean>> futureList = new ArrayList<>(amountOfThreads);

		// threadRange ~= how many hashes each thread should produce
		int threadRange = this.amountOfHashes / this.amountOfThreads;
		long currentStart;
		long currentEnd;

		for (int i = 0; i < this.amountOfThreads; i++) {
			currentStart = this.start + i * (threadRange);
			currentEnd = this.start + ((i+1) * (threadRange)) - 1;

			// if true: this it the last thread,
			// take rest of hashes to prevent "overflow"
			if (i == this.amountOfThreads - 1) {
				currentEnd = this.end;
			}

			Callable<Boolean> generateHashesThread = new GenerateHashesThread(
					currentStart, currentEnd, this.buffer, this.start);
			Future<Boolean> future = executor.submit(generateHashesThread);
			futureList.add(future);
		}

		executor.shutdown();
		// Fetch result from futures, the Boolean doesn't matter.
		// The only thing of importance is if an exception has occurred in one of the threads
		// the exception will be re-thrown from here.
		for (Future<Boolean> future : futureList) {
			future.get();
		}
	}

	public void sort() throws NullPointerException {
		if (this.buffer == null) {
			throw new NullPointerException("this.buffer == null. " +
					"Probably haven't used block.createHashes() yet.");
		}

		ForkJoinPool pool = new ForkJoinPool(this.amountOfThreads);
		pool.invoke(new Quicksort(this.buffer, 0, this.amountOfHashes - 1));
	}

	public void writeToFile() throws IOException {
		this.buffer.position(this.buffer.capacity());
		this.buffer.flip();

		try (FileOutputStream fos = new FileOutputStream(this.filename)) {
			fos.getChannel().write(this.buffer);
		}

		this.buffer = null;	// clear so that it can be GC:d
	}

	public void initReverseFileReader(int bufferSize) throws IOException {
		this.reader = new ReverseFileReader(bufferSize, this.filename);
	}

	public byte[] pop() throws IOException {
		return this.reader.pop();
	}

	/**
	 * Generates all hashes.
	 * Callable so that the "parent" thread can fetch exceptions.
	 * The returned bool does not represent anything.
	 */
	private class GenerateHashesThread implements Callable<Boolean> {
		private final long start;
		private final long end;
		private final long blockStart;
		private final ByteBuffer hashes;
		private final MessageDigest md;

		GenerateHashesThread(long threadStart, long threadEnd, ByteBuffer hashes, long blockStart)
		throws NoSuchAlgorithmException {
			this.start = threadStart;
			this.end = threadEnd;
			this.hashes = hashes;
			this.md = MessageDigest.getInstance("MD5");
			this.blockStart = blockStart;
		}

		public Boolean call() throws Exception {
			long currentNumber = this.start;
			String currentNumberString;
			byte[] currentHash;

			while (currentNumber <= this.end) {
				currentNumberString = padZeros(Long.toHexString(currentNumber)) + "\n";
				currentHash = this.md.digest(currentNumberString.getBytes(ENCODING));

				// Using put() with index makes it threadsafe
				// (as long as the other threads aren't using the same index).
				int startIndex = (int)((currentNumber - this.blockStart) * CreateSortedWordlist.HASH_SIZE);
				for (int i = 0; i < CreateSortedWordlist.HASH_SIZE; i++) {
					this.hashes.put((startIndex + i), currentHash[i]);
				}

				currentNumber++;
			}

			return true;
		}

		private String padZeros(String hexString) {
			return "0".repeat(CreateSortedWordlist.HASH_SIZE - hexString.length()) + hexString;
		}
	}
}