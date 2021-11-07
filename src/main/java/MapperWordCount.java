import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

// References :- http://www.regular-expressions.info/wordboundaries.html

public class MapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Set<String> stopWordsList = new HashSet<String>();
    private static final Pattern getTheBoundaryLetter = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
            throws IOException {

        Configuration configuration = context.getConfiguration();

        if (configuration.getBoolean("wordcount", false)) {
            URI[] localPaths = context.getCacheFiles();
            getStopWordFile(localPaths[0]);
        }
    }

    public void getStopWordFile(URI toUri) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(toUri.getPath()).getName()));
        String stopWords;
        while ((stopWords = bufferedReader.readLine()) != null) {
            stopWordsList.add(stopWords.toLowerCase().trim());
        }
    }

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {
        String line = lineText.toString();

        Text sendContentToReduce;
        for (String content : getTheBoundaryLetter.split(line)) {
            if (!stopWordsList.contains(content.toLowerCase().trim())) {
                sendContentToReduce = new Text(content);
                context.write(sendContentToReduce, one);
            }
        }
    }
}

