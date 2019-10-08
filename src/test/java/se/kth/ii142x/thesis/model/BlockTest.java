package se.kth.ii142x.thesis.model;

import java.io.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import se.kth.ii142x.thesis.CreateSortedWordlist;
import se.kth.ii142x.thesis.DTO.ComparisonDTO;

public class BlockTest {
	private final String filename = "testlist";
	private final String blockFilename  = "testlist0";
	private final int blockId = 0;

	private File createHashes(TestCase t) throws Exception {
		Block block = new Block(t.start, t.end, this.blockId, t.amountOfThreads, this.filename);
		block.generateHashes();
		block.sort();
		block.writeToFile();

		File f = new File(this.blockFilename);
		if (!f.exists()) {
			fail(String.format("file wasn't created with inputs:" +
				" start=%d, end=%d & amountOfThreads=%d", t.start, t.end, t.amountOfThreads));
		}
		if (f.length() != (t.end - t.start + 1) * CreateSortedWordlist.HASH_SIZE) {
			fail(String.format("file written to file is incorrect size with inputs:" +
				" start=%d, end=%d & amountOfThreads=%d", t.start, t.end, t.amountOfThreads));
		}

		return f;
	}

	private class TestCase {
		long start;
		long end;
		int amountOfThreads;

		TestCase(long start, long end, int amountOfThreads) {
			this.start = start;
			this.end = end;
			this.amountOfThreads = amountOfThreads;
		}
	}

	@Test
	public void testCreateHashesCorrectly() {
		TestCase[] testCases = {
			new TestCase(0, 0, 1),
			new TestCase(0, 0, 2),
			new TestCase(16, 16, 1),
			new TestCase(16, 16, 2),
			new TestCase(0, 16, 1),
			new TestCase(0, 16, 2),
			new TestCase(0, 0xfffe, 1),
			new TestCase(0, 0xfffe, 2),
			new TestCase(0, 0xfffe, 8),
			new TestCase(0, 0xfffe, 15),
		};

		File f = null;
		for (TestCase t : testCases) {
			try {
				f = createHashes(t);
			} catch (Exception e) {
				fail(e);
			} finally {
				if (f != null)
					f.delete();
			}
		}
	}

	@Test
	public void testCreateHashesWithExceptions() {
		TestCase[] testCases = {
				new TestCase(-1, 0, 1),
				new TestCase(16, 0, 1),
				new TestCase(16, 16, -1),
				new TestCase(0, 16, -1),
		};

		File f = null;
		for (TestCase t : testCases) {
			try {
				f = createHashes(t);
				fail(String.format("was supposed to get an exception while creating hashes with inputs:" +
					" start=%d, end=%d & amountOfThreads=%d", t.start, t.end, t.amountOfThreads));
			} catch (Exception e) {
				// success
			} finally {
				if (f != null)
					f.delete();
			}
		}
	}

	@Test
	public void testHashesSorted() {
		TestCase[] testCases = {
				new TestCase(0, 0, 1),
				new TestCase(0, 0, 2),
				new TestCase(16, 16, 1),
				new TestCase(16, 16, 2),
				new TestCase(0, 16, 1),
				new TestCase(0, 16, 2),
				new TestCase(0, 0xfffe, 1),
				new TestCase(0, 0xfffe, 2),
				new TestCase(0, 0xfffe, 8),
				new TestCase(0, 0xfffe, 15),
		};

		File f = null;
		for (TestCase t : testCases) {
			try {
				f = createHashes(t);
			} catch (Exception e) {
				fail(e);
			}

			int n;
			byte[] prev = new byte[CreateSortedWordlist.HASH_SIZE];
			byte[] current = new byte[CreateSortedWordlist.HASH_SIZE];

			try(FileInputStream fis = new FileInputStream(this.blockFilename)) {
				fis.read(prev);

				while(true) {
					n = fis.read(current);
					if (n <= 0) {
						break;
					}

					if (ComparisonDTO.compare(current, prev) > 0) {
						fail(String.format("hashes not sorted correctly with inputs:" +
							" start=%d, end=%d & amountOfThreads=%d", t.start, t.end, t.amountOfThreads));
					}

					prev = current;
				}
			} catch (Exception e) {
				fail(e);
			} finally {
				if (f != null)
					f.delete();
			}
		}
	}
}