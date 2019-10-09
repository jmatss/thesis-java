package se.kth.ii142x.thesis;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.kth.ii142x.thesis.model.Block;

class CreateBlocks {
	private static final Logger LOGGER = Logger.getLogger(CreateBlocks.class.getName());

	private final long start;
	private final long end;
	private final int amountOfThreads;
	private final int hashesPerBlock;
	private final String filename;

	CreateBlocks(long start, long end, int amountOfThreads, int bufferSize, String filename) {
		this.start = start;
		this.end = end;
		this.amountOfThreads = amountOfThreads;
		this.filename = filename;

		// floor to multiple of HASH_SIZE
		bufferSize -= bufferSize % CreateSortedWordlist.HASH_SIZE;
		if (bufferSize <= 0) {
			throw new IllegalArgumentException(String.format("bufferSize <= 0: (%d < 0)" +
				", might have wrapped around Integer.MAX_VALUE. bufferSize is " +
				"limited by ByteBuffers limit of Integer.MAX_VALUE.\n", bufferSize));
		}
		this.hashesPerBlock = bufferSize / CreateSortedWordlist.HASH_SIZE;
	}

	/**
	 * Creates all blocks, sorts them and writes them to individual files
	 */
	List<Block> create()
	throws NoSuchAlgorithmException, InterruptedException, IOException, ExecutionException {
		List<Block> blocks = new ArrayList<>();

		int currentBlockId = 0;
		long currentBlockStart = this.start;
		long currentBlockEnd;
		Block currentBlock;

		// Create one block per while loop
		while (true) {
			currentBlockEnd = currentBlockStart + this.hashesPerBlock - 1;

			// if true: this it the last block,
			// take rest of hashes to prevent "overflow"
			if (currentBlockEnd > this.end) {
				currentBlockEnd = this.end;
			}

			LOGGER.log(Level.INFO, String.format("--- Block %d: 0x%08x through 0x%08x ---",
					currentBlockId, currentBlockStart, currentBlockEnd));
			currentBlock = new Block(currentBlockStart, currentBlockEnd, currentBlockId,
					amountOfThreads, filename);

			currentBlock.generateHashes();
			LOGGER.log(Level.INFO, "Done generating hashes.");

			currentBlock.sort();
			LOGGER.log(Level.INFO, "Done sorting hashes.");

			currentBlock.writeToFile();
			LOGGER.log(Level.INFO, "Done writing hashes to file.");

			blocks.add(currentBlock);

			currentBlockId++;
			currentBlockStart = currentBlockEnd + 1;
			if (currentBlockStart > this.end) {
				break;
			}
		}

		// all blocks created, sorted and written to disk
		return blocks;
	}
}
