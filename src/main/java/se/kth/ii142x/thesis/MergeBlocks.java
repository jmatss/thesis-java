package se.kth.ii142x.thesis;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;
import se.kth.ii142x.thesis.model.Block;

/**
 * Merges the blocks stored on disk into one single sorted file
 */
class MergeBlocks {
	private static final Logger LOGGER = Logger.getLogger(MergeBlocks.class.getName());
	private static final int QUEUE_SIZE = 1 << 14;	// arbitrary value

	private final List<Block> blocks;
	private final int amountOfThreads;
	private final String filename;
	private final int printAmount;

	MergeBlocks(List<Block> blocks, int amountOfThreads, int bufferSize, String filename, int printAmount)
			throws IOException {
		this.blocks = blocks;
		this.amountOfThreads = amountOfThreads;
		this.filename = filename;
		this.printAmount = printAmount;

		int blockBufferSize = bufferSize / blocks.size();
		// floor to multiple of HASH_SIZE
		blockBufferSize -= blockBufferSize % CreateSortedWordlist.HASH_SIZE;
		for (Block block : blocks) {
			block.initReverseFileReader(blockBufferSize);
		}
	}

	@SuppressWarnings("unchecked")
	void merge() throws IOException, InterruptedException
	{
		BlockingQueue<byte[]> minResult = new ArrayBlockingQueue(QUEUE_SIZE);
		MergeHandler mergeHandler = new MergeHandler(this.blocks, minResult, this.amountOfThreads, QUEUE_SIZE);
		new Thread(mergeHandler).start();

		long iterations = 0;
		byte[] minBlock;

		try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(this.filename))) {
			// Remove smallest item from the blocks found by comparisonHandler
			// and write result to out. getMin() returns byte[].length != 16 when
			// all hashes have been merged (BlockingQueue doesn't allow null).
			while (true) {
				minBlock = minResult.take();
				if (minBlock.length != 16) {
					break;
				}
				out.write(minBlock);

				if (++iterations % this.printAmount == 0) {
					LOGGER.log(Level.INFO, String.format("%d hashes merged", iterations));
				}
			}

			out.flush();
		}
	}
}