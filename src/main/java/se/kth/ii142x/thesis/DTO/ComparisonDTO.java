package se.kth.ii142x.thesis.DTO;

/**
 * DTO for comparison results. Groups a hash with an id so that
 * one can identify from which thread/block this hash came from.
 */
public class ComparisonDTO implements Comparable {
	// last 12 hex chars starts at byte index 10 (when HASH_SIZE == 16)
	private static final int COMP_START = 10;
	// compare last 12 hex chars = 6 bytes
	private static final int COMP_AMOUNT = 6;

	private final int id;
	private final byte[] hash;

	public ComparisonDTO(int id, byte[] hash) {
		this.id = id;
		this.hash = hash;
	}

	@Override
	public int compareTo(Object o) {
		return compare(this.getHash(), ((ComparisonDTO)o).getHash());
	}

	public byte[] getHash() {
		return this.hash;
	}
	public int getId() {
		return this.id;
	}

	// Counts null as greater than (shouldn't matter)
	public static int compare(byte[] left, byte[] right) {
		if (right == null)  // will be true if both are null
			return -1;
		else if (left == null)
			return 1;

		short lShort;
		short rShort;

		for (int i = 0; i < COMP_AMOUNT; i++) {
			// convert to short before comparing since the sign of bytes ruins comparisons
			lShort = (short)(left[COMP_START + i] & 0xff);
			rShort = (short)(right[COMP_START + i] & 0xff);
			if (lShort < rShort)
				return -1;
			else if (lShort > rShort)
				return 1;
		}

		return 0;	// equal
	}
}