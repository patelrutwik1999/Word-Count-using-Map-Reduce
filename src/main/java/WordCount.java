import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCount extends Configured {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration(), "Count Words exempting Stop Words.");

        //To find the file having stop words.
        for (int i = 0; i < args.length; i ++ ) {
            if ("stopword".equals(args[i])) {
                job.getConfiguration().setBoolean("wordcount", true);
                i += 1;

                job.addCacheFile(new Path(args[i]).toUri());
            }
        }
        job.setJarByClass(WordCount.class);

        //Input Files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        //Output Files
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapperClass(MapperWordCount.class);
        job.setCombinerClass(ReduceWordCount.class);
        job.setReducerClass(ReduceWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}