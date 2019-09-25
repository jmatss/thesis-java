package se.kth.ii142x.thesis.model;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import se.kth.ii142x.thesis.CreateSortedWordlist;

/**
 * Reads and buffers file in reverse. This is needed since blocks are
 * sorted in descending order to allow easy removal of hashes from disk
 * after they have been read into memory (by truncating the files).
 */
class ReverseFileReader {
	private final ByteBuffer buffer;
	private final String filename;
	private boolean empty;

	ReverseFileReader(int bufferSize, String filename) throws IOException {
		this.buffer = ByteBuffer.allocate(bufferSize);
		this.filename = filename;

		// empty == true: no more hashes on disk
		this.empty = false;
		refillBuffer();
	}

	private void refillBuffer() throws IOException {
		if (this.empty) {
			this.buffer.limit(0);
			return;
		}

		// Need to close and reopen raf every refill to be able
		// to shrink size on disk after truncation.
		RandomAccessFile raf = new RandomAccessFile(this.filename, "rw");
		int amountOfBytesToRead;

		// if true: no more hashes in file after this fetch, set empty flag
		if (raf.length() <= this.buffer.capacity()) {
			amountOfBytesToRead = (int)raf.length();
			this.empty = true;
		} else {
			amountOfBytesToRead = this.buffer.capacity();
		}

		// position pointer correctly and read
		this.buffer.clear();
		raf.seek(raf.length() - amountOfBytesToRead);
		raf.getChannel().read(this.buffer);

		// set position and limit, descending order
		this.buffer
			.position(amountOfBytesToRead - CreateSortedWordlist.HASH_SIZE)
			.limit(amountOfBytesToRead);

		// Truncate and remove hashes from disk. If crash or interrupt: data lost.
		raf.setLength(raf.length() - amountOfBytesToRead);
		raf.close();

		if (this.empty)
			new File(this.filename).delete();
	}

	/**
	 * Assumes that there are hashes in the buffer at the start of the function.
	 * Will fetch new hashes from disk if it sees that the buffer is empty at
	 * the end of it's own pop.
	 */
	byte[] pop() throws IOException {
		if (this.empty && this.buffer.limit() == 0)
			return null;

		// Need to decrement the position instead of increment since the buffer
		// is read in reverse.
		int oldPosition = this.buffer.position();
		int oldLimit = this.buffer.limit();

		byte[] result = new byte[CreateSortedWordlist.HASH_SIZE];
		this.buffer.get(result, 0, result.length);  // not thread safe

		// If true: this function call just popped last item,
		// refill buffer with new hashes from disk if possible.
		if (oldPosition == 0) {
			refillBuffer();
		} else {
			int newPosition = oldPosition - CreateSortedWordlist.HASH_SIZE;
			int newLimit = oldLimit - CreateSortedWordlist.HASH_SIZE;
			this.buffer.position(newPosition).limit(newLimit);
		}

		return result;
	}
}