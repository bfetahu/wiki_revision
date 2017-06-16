package mapred.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by besnik on 13.06.17.
 */

public class PairInputFormat extends FileInputFormat<LongWritable, TextList> {
    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

    public RecordReader<LongWritable, TextList> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());
        System.out.println("The length of this split is " + genericSplit.getLength());
        return new CustomRecordReader(context.getConfiguration().getInt(LINES_PER_MAP, 100));
    }

    /**
     * Logically splits the set of input files for the job, splits N lines
     * of the input as one split.
     *
     * @see FileInputFormat#getSplits(JobContext)
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (FileStatus status : listStatus(job)) {
            splits.addAll(getSplitsForFile(status, job.getConfiguration()));
        }
        return splits;
    }

    public static List<FileSplit> getSplitsForFile(FileStatus status, Configuration conf) throws IOException {
        List<FileSplit> splits = new ArrayList<FileSplit>();
        Path fileName = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + fileName);
        }
        FileSystem fs = fileName.getFileSystem(conf);
        LineReader lr = null;
        try {
            FSDataInputStream in = fs.open(fileName);
            lr = new LineReader(in, conf);
            Text line = new Text();
            long begin = 0;
            int num = -1;

            //determine the split length in number of lines
            int num_lines = conf.getInt(LINES_PER_MAP, 100);

            long prev_line_length = 0;
            List<Integer> line_offsets = new ArrayList<>();
            while ((num = lr.readLine(line)) > 0) {
                line_offsets.add(num);
            }

            //in case the number of lines to read together is bigger than the actual number of lines in the file
            if (num_lines >= line_offsets.size()) {
                splits.add(createFileSplit(fileName, begin, line_offsets.stream().mapToLong(x -> x).sum()));
            } else {
                int line_counter = 1;
                long length = 0;
                for (int i = 0; i < line_offsets.size(); i++, line_counter++) {
                    if (line_counter == num_lines) {
                        long start = begin == 0 ? begin : begin - prev_line_length;
                        splits.add(createFileSplit(fileName, start, start + length));

                        begin += length;
                        length = 0;
                        line_counter = 1;
                        prev_line_length = line_offsets.get(i);
                    }
                    length += line_offsets.get(i);
                }
            }
        } finally {
            if (lr != null) {
                lr.close();
            }
        }
        return splits;
    }

    /**
     * NLineInputFormat uses LineRecordReader, which always reads
     * (and consumes) at least one character out of its upper split
     * boundary. So to make sure that each mapper gets N lines, we
     * move back the upper split limits of each split
     * by one character here.
     *
     * @param fileName Path of file
     * @param begin    the position of the first byte in the file to process
     * @param length   number of bytes in InputSplit
     * @return FileSplit
     */
    protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
        return (begin == 0)
                ? new FileSplit(fileName, begin, length - 1, new String[]{})
                : new FileSplit(fileName, begin - 1, length, new String[]{});
    }

}
