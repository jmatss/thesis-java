package se.kth.ii142x.thesis;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.fail;

import se.kth.ii142x.thesis.DTO.ComparisonDTO;
import se.kth.ii142x.thesis.model.Block;

public class CreateSortedWordlistTest {
	private static final String filename = "testlist";

	private File createHashes(TestCase t) throws Exception {
		CreateBlocks createBlocks = new CreateBlocks(
			t.start, t.end, t.amountOfThreads, t.bufferSize, this.filename
		);
		List<Block> blocks = createBlocks.create();

		MergeBlocks mergeBlocks = new MergeBlocks(
				blocks, t.amountOfThreads, t.bufferSize, this.filename, Integer.MAX_VALUE
		);
		mergeBlocks.merge();

		File f = new File(this.filename);
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
		int bufferSize;

		TestCase(long start, long end, int amountOfThreads, int bufferSize) {
			this.start = start;
			this.end = end;
			this.amountOfThreads = amountOfThreads;
			this.bufferSize = bufferSize;
		}
	}

	@Test
	public void testCreateHashesWithExceptions() {
		TestCase[] testCases = {
			new TestCase(-1, 0, 1, Integer.MAX_VALUE),
			new TestCase(16, 0, 1, 0xff),
			new TestCase(16, 16, -1, 0xff),
			new TestCase(0, 16, -1, Integer.MAX_VALUE),
			new TestCase(0, 16, 1, -1),
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
			new TestCase(0, 0, 1, 16),
			new TestCase(0, 0, 2, Integer.MAX_VALUE),
			new TestCase(16, 16, 1, Integer.MAX_VALUE),
			new TestCase(16, 16, 2, 16),
			new TestCase(0, 16, 1, Integer.MAX_VALUE),
			new TestCase(0, 16, 2, Integer.MAX_VALUE),
			new TestCase(0, 0xfffe, 1, 0xfff * CreateSortedWordlist.HASH_SIZE),
			new TestCase(0, 0xfffe, 2, 0xffe * CreateSortedWordlist.HASH_SIZE),
			new TestCase(0, 0xfffe, 8, Integer.MAX_VALUE),
			new TestCase(0, 0xfffe, 15, Integer.MAX_VALUE),
			new TestCase(0xf3, 0xff, 1, Integer.MAX_VALUE),
			new TestCase(0xfffff00, 0xfffffff, 2, Integer.MAX_VALUE),
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

			try(FileInputStream fis = new FileInputStream(this.filename)) {
				fis.read(prev);

				while(true) {
					n = fis.read(current);
					if (n <= 0) {
						break;
					}

					if (ComparisonDTO.compare(current, prev) < 0) {
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
