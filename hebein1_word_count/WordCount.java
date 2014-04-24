import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class WordCount {

	public static class TitleTextInputFormat extends FileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
			return new TitleTextRecordReader((FileSplit) inputSplit, jobConf);
		}

		public static class TitleTextRecordReader implements RecordReader<LongWritable, Text> {
			private byte[] startDelimiter;
			private byte[] endDelimiter;
			private long startOffset;
			private long endOffset;
			private FSDataInputStream fsDataIn;
			private final DataOutputBuffer buffer = new DataOutputBuffer();

			private String startString = new String("MP4_START_MP4");
			private String endString = new String("MP4_END_MP4");

			public TitleTextRecordReader(FileSplit split, JobConf jobConf) throws IOException {
				startDelimiter = startString.getBytes();
				endDelimiter = endString.getBytes();

				// seek to the start of the split in the file
				startOffset = split.getStart();
				endOffset = startOffset + split.getLength();
				Path file = split.getPath();
				FileSystem fs = file.getFileSystem(jobConf);
				fsDataIn = fs.open(split.getPath());
				fsDataIn.seek(startOffset);
			}

			@Override
			public boolean next(LongWritable key, Text value) throws IOException {
				if (fsDataIn.getPos() < endOffset) {
					if (readUntilMatch(startDelimiter, false)) { // reads past the end of the startDelimiter
						try {
							key.set(fsDataIn.getPos()); // set the key to be the offset of the end of the startDelimiter
							if (readUntilMatch(endDelimiter, true)) { // reads past the end of the endDelimiter
								value.set(buffer.getData(), 1, buffer.getLength() - 12); // set the value to the contents of the buffer
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}

			@Override
			public LongWritable createKey() {
				return new LongWritable();
			}

			@Override
			public Text createValue() {
				return new Text();
			}

			@Override
			public long getPos() throws IOException {
				return fsDataIn.getPos();
			}

			@Override
			public void close() throws IOException {
				fsDataIn.close();
			}

			@Override
			public float getProgress() throws IOException {
				return ((fsDataIn.getPos() - startOffset) / (float) (endOffset - startOffset));
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
				int i = 0;
				while (true) {
					int b = fsDataIn.read();
					if (b == -1) return false; // end of file
					if (withinBlock) buffer.write(b); // save to buffer

					// check if we have found the match argument
					if (b == match[i]) {
						i++;
						if (i >= match.length) {
							return true;
						}
					} else {
						i = 0;
					}

					if (!withinBlock && i == 0 && fsDataIn.getPos() >= endOffset) return false; // passed the stop point
				}
			}
		}
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text offsetCommaFile = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			String filename = fileSplit.getPath().getName();

			String line = value.toString();
			String[] tokens = line.toLowerCase().split("[^a-z]");

			for(int i = 0; i < tokens.length; i++) {
				if(tokens[i].trim().length() == 0) { continue; }
				word.set(tokens[i]);
				offsetCommaFile.set(key.toString() + "," + filename);
				output.collect(word, offsetCommaFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// read "offset,filename" into a hash, if we see the same string increment it
			HashMap<String, Integer> hm = new HashMap<String, Integer>();
			while (values.hasNext()) {
				String cur = values.next().toString();
				if(hm.containsKey(cur)) {
					hm.put(cur, hm.get(cur) + 1);
				} else {
					hm.put(cur, 1);
				}
			}

			// output the hash to a string
			Text result = new Text();
			StringBuilder sb = new StringBuilder();
			Iterator it = hm.keySet().iterator();

			while(it.hasNext()) {
				String k = (String)it.next();
				int v = (Integer)hm.get(k);
  				if(it.hasNext()) {
					sb.append(k + "," + v + "|");
				} else {
					sb.append(k + "," + v);
				}
			}
				
			result.set(sb.toString());

			// write it to output collector
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TitleTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
