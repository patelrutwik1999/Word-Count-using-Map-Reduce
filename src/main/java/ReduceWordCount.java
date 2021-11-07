import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable values : counts) {
            sum += values.get();
        }
        context.write(word, new IntWritable(sum));
    }
}
