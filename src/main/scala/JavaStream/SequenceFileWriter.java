package JavaStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;

/**
 * Created by ramakrishna on 1/16/17.
 */
public class SequenceFileWriter {
    public static void  main(String[] args) throws IOException{
        if(args.length !=2) {
            throw new IOException("not able to find the arguments");
        }
        Random randomNumberGenerator = new Random();
        final String uri = args[0];
        final int N = Integer.parseInt(args[1]);

        Configuration conf = new Configuration();
                conf.set("fs.defaultFS","hdfs://localhost:9000/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(uri);

        //
        Text key = new Text();
        IntWritable value = new IntWritable();
        SequenceFile.Writer writer = null;
        try {
            writer=SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());
            for (int i = 1; i < N; i++) {
                int randomInt = randomNumberGenerator.nextInt(100);
                key.set("cat" + i);
                value.set(randomInt);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
        }
        finally {
            IOUtils.closeStream(writer);
        }

    }
}
