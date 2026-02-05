package coordinator.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * HDFS Integration для MapReduce системы.
 *
 * Позволяет:
 * 1. Читать данные из HDFS вместо локального файла
 * 2. Получать информацию о блоках для data locality
 * 3. Стримить данные из HDFS
 *
 * @author Expert Review
 */
public class HDFSDataSource {

    private final Configuration conf;
    private final FileSystem fs;
    private final String hdfsUri;

    public HDFSDataSource(String hdfsUri) throws IOException {
        this.hdfsUri = hdfsUri;
        this.conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("dfs.client.use.datanode.hostname", "true");

        this.fs = FileSystem.get(URI.create(hdfsUri), conf);

        System.out.println("✓ Connected to HDFS: " + hdfsUri);
    }

    public ChunkIterator getChunkIterator(String filePath, int maxLinesPerChunk)
            throws IOException {
        return new ChunkIterator(fs, new Path(filePath), maxLinesPerChunk);
    }

    public List<BlockInfo> getBlockLocations(String filePath) throws IOException {
        Path path = new Path(filePath);
        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());

        List<BlockInfo> result = new ArrayList<>();
        for (int i = 0; i < blocks.length; i++) {
            BlockLocation block = blocks[i];
            result.add(new BlockInfo(
                    i,
                    block.getOffset(),
                    block.getLength(),
                    block.getHosts()
            ));
        }

        return result;
    }

    public String readBlock(String filePath, long offset, long length) throws IOException {
        Path path = new Path(filePath);

        try (FSDataInputStream in = fs.open(path)) {
            in.seek(offset);

            byte[] buffer = new byte[(int) length];
            int bytesRead = in.read(buffer);

            return new String(buffer, 0, bytesRead);
        }
    }

    public boolean exists(String filePath) throws IOException {
        return fs.exists(new Path(filePath));
    }

    public long getFileSize(String filePath) throws IOException {
        return fs.getFileStatus(new Path(filePath)).getLen();
    }

    public void close() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    // =================== INNER CLASSES ===================

    /**
     * Итератор для потокового чтения chunk'ов из HDFS.
     * ИСПРАВЛЕННАЯ ВЕРСИЯ — корректно читает ВЕСЬ файл.
     */
    public static class ChunkIterator implements Iterator<ChunkData>, Closeable {

        private final BufferedReader reader;
        private final int maxLines;
        private int chunkIndex = 0;
        private String nextLine; // look-ahead

        ChunkIterator(FileSystem fs, Path path, int maxLines) throws IOException {
            this.reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            this.maxLines = maxLines;
            this.nextLine = reader.readLine(); // первая строка
        }

        @Override
        public boolean hasNext() {
            return nextLine != null;
        }

        @Override
        public ChunkData next() {
            if (nextLine == null) {
                throw new NoSuchElementException("No more chunks");
            }

            StringBuilder buffer = new StringBuilder();
            int lines = 0;

            try {
                while (lines < maxLines && nextLine != null) {
                    buffer.append(nextLine).append("\n");
                    lines++;
                    nextLine = reader.readLine();
                }

                if (lines == 0) {
                    return null;
                }

                int currentChunkIndex = chunkIndex++;
                return new ChunkData(currentChunkIndex, buffer.toString(), lines);

            } catch (IOException e) {
                throw new RuntimeException("Error reading from HDFS", e);
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    public static class ChunkData {
        public final int index;
        public final String content;
        public final int lines;

        public ChunkData(int index, String content, int lines) {
            this.index = index;
            this.content = content;
            this.lines = lines;
        }
    }

    public static class BlockInfo {
        public final int index;
        public final long offset;
        public final long length;
        public final String[] hosts;

        public BlockInfo(int index, long offset, long length, String[] hosts) {
            this.index = index;
            this.offset = offset;
            this.length = length;
            this.hosts = hosts;
        }

        @Override
        public String toString() {
            return String.format(
                    "Block[%d: offset=%d, len=%d, hosts=%s]",
                    index, offset, length, Arrays.toString(hosts)
            );
        }
    }
}
