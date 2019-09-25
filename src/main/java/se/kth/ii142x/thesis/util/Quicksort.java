package se.kth.ii142x.thesis.util;

import java.nio.ByteBuffer;
import java.util.concurrent.RecursiveAction;
import se.kth.ii142x.thesis.CreateSortedWordlist;

/**
 * Quick sort/insertion sort on the given bytebuffer.
 * Every item is CreateSortedWordlist.HASH_SIZE bytes.
 * Orders by the last 6 bytes(== SSID).
 * Uses the last element as pivot.
 * Assumes no items are equal.
 * Descending order.
 */
public class Quicksort extends RecursiveAction {
	private static final int SIZE = CreateSortedWordlist.HASH_SIZE;

	// last 12 hex chars starts at byte index 10 (when HASH_SIZE == 16)
	private static final int COMP_START = 10;
	// compare last 12 hex chars = 6 bytes
	private static final int COMP_AMOUNT = 6;

	private final int insertionSortThreshold;
	private final int start;
	private final int end;
	private final ByteBuffer buffer;

	// Expects to receive regular hash indices.
	public Quicksort(ByteBuffer hashes, int start, int end) {
		this.insertionSortThreshold = 8;
		this.buffer = hashes;
		this.start = start;
		this.end = end;
	}

	@Override
	protected void compute() {
		if (this.end - this.start <= this.insertionSortThreshold) {
			insertionSort(this.start, this.end);
			return;
		}

		int current = this.start * SIZE;
		int pivot = this.end * SIZE;
		int gtPivot = this.start * SIZE;	// greater than

		// Iterates from left to right (low to high) swapping
		// any hashes larger than or equals pivot to the left (low).
		while(current < this.end * SIZE) {
			if (compareTo(current, pivot) >= 0) {
				swap(current, gtPivot);
				gtPivot += SIZE;
			}
			current += SIZE;
		}

		swap(pivot, gtPivot);
		pivot = gtPivot;

		invokeAll(new Quicksort(this.buffer, this.start, (pivot / SIZE) - 1),
				new Quicksort(this.buffer, (pivot / SIZE) + 1, this.end));
	}

	// Expects to receive regular hash indices.
	private void insertionSort(int start, int end) {
		// if true: insertion sort cutoff set to 0, no insertion sort needed
		if (start == end) {
			return;
		}

		int j, jIndex, jMinusOneIndex;
		for (int i = 1; i < (end - start + 1); i++) {
			j = i + start;
			while (j > start) {
				jIndex = j * SIZE;
				jMinusOneIndex = (j - 1) * SIZE;

				// descending order
				if (compareTo(jIndex, jMinusOneIndex) > 0) {
					swap(jIndex, jMinusOneIndex);
				} else {
					break;
				}
				j--;
			}
		}
	}

	// Thread safe as long as you use indices while putting.
	// Expects to receive bytebuffer indices (i.e. index * SIZE).
	private void swap(int l, int r) {
		ByteBuffer tempSwap = ByteBuffer.allocate(SIZE);
		for (int i = 0; i < SIZE; i++) {
			tempSwap.put(i, this.buffer.get(l + i));
			this.buffer.put(l + i, this.buffer.get(r + i));
			this.buffer.put(r + i , tempSwap.get(i));
		}
	}

	// Expects to receive bytebuffer indices (i.e. index * SIZE).
	private int compareTo(int left, int right) {
		short l;
		short r;

		for (int i = 0; i < COMP_AMOUNT; i++) {
			// convert to short before comparing since the sign of bytes ruins comparisons
			l = (short)(this.buffer.get(left + COMP_START + i) & 0xff);
			r = (short)(this.buffer.get(right + COMP_START + i) & 0xff);

			if (l < r) {
				return -1;
			} else if (l > r) {
				return 1;
			}
		}

		return 0;	// equal
	}
}
