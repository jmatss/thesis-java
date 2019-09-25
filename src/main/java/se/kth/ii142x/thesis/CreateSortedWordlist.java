package se.kth.ii142x.thesis;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.util.logging.Level;
import se.kth.ii142x.thesis.model.Block;

public class CreateSortedWordlist {
	private static final Logger LOGGER = Logger.getLogger(CreateSortedWordlist.class.getName());
	public static final int HASH_SIZE = 16;  // 16 bytes (= 128 bits per md5 hash)

	public static void main (String[] args)
	throws NoSuchAlgorithmException, InterruptedException, IOException, ExecutionException {
		long start = 0L;        // The last 8 hex chars of the raspberry pi serial number,
		long end = 0xffffffL;   // will loop all possible serial numbers from "start" through "end"

		String filename = "list";
		int amountOfThreads = 8;
		int bufferSize = Integer.MAX_VALUE;  // ~Max amount of bytes in buffers at the same time
		int printAmount = (int)1e7;          // Print status every "printAmount" merge iteration

		// floor to multiple of HASH_SIZE
		bufferSize -= bufferSize % HASH_SIZE;

		if (bufferSize <= 0) {
			System.err.printf("bufferSize <= 0: (%d < 0)" +
					", might have wrapped around Integer.MAX_VALUE. bufferSize is " +
					"limited by ByteBuffers limit of Integer.MAX_VALUE.\n", bufferSize);
			System.exit(0);
		}

		/*
			STEP 1
			Create blocks. Every block will contain (bufferSize / HASH_SIZE) hashes.
			The blocks will be sorted in DESC and written to disk in files "filename + blockId"
		*/
		CreateBlocks createBlocks = new CreateBlocks(start, end, amountOfThreads, bufferSize, filename);
		List<Block> blocks = createBlocks.create();
		LOGGER.log(Level.INFO, "--- Done creating " + blocks.size() + " blocks. ---");

		/*
			STEP 2
			Merges the blocks into one single sorted file "filename".
			Removes hashes from disk as soon as they have been read into memory, no backup.
		*/
		LOGGER.log(Level.INFO, "--- Starting " + blocks.size() + "-way merge. ---");
		MergeBlocks mergeBlocks = new MergeBlocks(blocks, amountOfThreads, bufferSize, filename, printAmount);
		mergeBlocks.merge();
		LOGGER.log(Level.INFO, "--- DONE ---");
	}
}
