import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.NullWritable;
public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      Text filename = new Text(((FileSplit)context.getInputSplit()).getPath().getName());
      while (itr.hasMoreTokens()) {
        String val = itr.nextToken().replaceAll("[^A-Za-z]", " ").toLowerCase();
	String[] vals = val.split(" "); 
        for (String v: vals) {
          if (!v.equals("")){
            word.set(v);
            context.write(word, filename);
          }
	}
      }
    }
  }
  public static class OutputRecord {
    public int num;
    public String chapter;
    public OutputRecord(int n, String c){
      this.num = n;
      this.chapter = c;
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,NullWritable> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Map<String, Integer> chapterToNum = new HashMap<String, Integer>();
      for (Text val : values) {
        Integer thisSum = chapterToNum.get(val.toString());
        if (thisSum == null) {
           thisSum = 0;
        }
        chapterToNum.put(val.toString(), thisSum + 1);
      }
      context.write(key, NullWritable.get());
      List<OutputRecord> records = new ArrayList<OutputRecord>();
      for (String chapter : chapterToNum.keySet()){
        records.add(new OutputRecord(chapterToNum.get(chapter), chapter)); 
      }
      Collections.sort(records, new Comparator<OutputRecord>(){
        @Override
        public int compare(final OutputRecord lhs, OutputRecord rhs) {
          if (lhs.num == rhs.num) {
            return Integer.parseInt(lhs.chapter.replaceAll("[^0-9]", "")) - Integer.parseInt(rhs.chapter.replaceAll("[^0-9]", ""));
          }
          return rhs.num - lhs.num;
        }
      });
      for (OutputRecord record: records){
        context.write(new Text("<" + record.chapter + ", " + record.num + ">"), NullWritable.get());

      }
    context.write(new Text(""), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    //job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
