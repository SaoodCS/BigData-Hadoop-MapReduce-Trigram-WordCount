//bucket name: ngram-mapreduce-assignment
//input directory = gs://ngram-mapreduce-assignment/coc105-gutenburg-10000books
//output directory = gs://ngram-mapreduce-assignment/output
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class WordCount {

  public static class WCMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      // Student ID = B626182 --> (626182 mod 5) + 1 = 3. Therefore this MapReduce program will find all the trigrams from the input. Also replacing punct. with whitespace below:
      String prevWord1 = null;
      String prevWord2 = null;
      StringTokenizer itr = new StringTokenizer(
        value.toString().replaceAll("\\p{P}", "")
      );

      while (itr.hasMoreTokens()) {
        String str = itr.nextToken();
        if (
          ((!str.equals("")) && (str != null) && (str.matches("^[a-zA-Z]*$"))) // if the string is a word and is not empty nor is whitespace..
        ) {
          if ((prevWord1 != null) && (prevWord2 != null)) { // Setting the trigram, then setting the first word as the second word and second word as the first for the next trigram
            word.set(prevWord1 + " " + prevWord2 + " " + str);
            prevWord1 = prevWord2;
            prevWord2 = str;
            context.write(word, one); //writing the occurance of the trigram
          } else if ((prevWord2 == null) && (prevWord1 != null)) { // For the initial trigram set the 2nd word as the 2nd token
            prevWord2 = str;
          } else if ((prevWord1 == null) && (prevWord2 == null)) { // For the initial trigram set the 1st token as the first word
            prevWord1 = str;
          }
        }
      }
    }
  }

  public static class WCReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get(); //reducer sums up all the occurances of the trigrams
      }
      result.set(sum);
      context.write(key, result);
    }
  }

 public static class IntComparator extends WritableComparator{ // this class compares the trigrams based on no. of bytes (ie. their length using Java's ByteBuffer)
     public IntComparator(){
         super(IntWritable.class);
     }

     @Override
     public int compare (byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
         Integer v1 = ByteBuffer.wrap(b1,s1,l1).getInt();
         Integer v2 = ByteBuffer.wrap(b2,s2,l2).getInt();
         return v1.compareTo(v2) * (-1); 
     }
 }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.input.fileinputformat.split.maxsize","268435456"); //concatenate the input files under a max size of 248mb
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class); 
    job.setMapperClass(WCMapper.class); 
    job.setReducerClass(WCReducer.class);
    job.setInputFormatClass(CombineTextInputFormat.class); // set the input format class to the imported CombineTextInputFormat
    job.setNumReduceTasks(7); //2 nodes and with 4 cores per node in my cluster therefore 7 is optimal reducer number
    job.setOutputKeyClass(Text.class); 
    job.setOutputValueClass(IntWritable.class); 
    job.setSortComparatorClass(IntComparator.class); // the class that sorts the trigrams based on the length of the trigram in descending order
    job.setCombinerClass(WCReducer.class); // adding a combiner class to combine the same word occurances that are in the same file to optimise performance. 
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

