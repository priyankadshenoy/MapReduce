import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by ps on 2/22/17.
 */

// DriverProgram is the main java class running individual
// pre-processing, iterating over jobs and finding top-k jobs
// I have not set any reducer for PageDeltaJob since it is a map only task
// By default identity reducer id run when no reduce task is mentioned
public class DriverProgram  extends Configured implements Tool {

    static double delta = 0.0;
    static int totalNoOfPages;

    //Enum for accessing, updating and storing counter values
    public enum PAGE_COUNTER {
        Delta, TotalPages;

    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new DriverProgram(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String ipath;
        String rPath = null;

        // args[0] = input args[1]= output
        // adding "i" for intermediate output files being generated
        boolean isCompleted = pagRankJob1(args[0], args[1] + "0");
        if (!isCompleted) return 1;

        for (int i = 0; i < 10; i++) {
            ipath = args[1] + i;
            rPath = args[1] + (i + 1);

            isCompleted = pageRankJob2(ipath, rPath);
            if (!isCompleted) return 1;
        }

        //running job to add delta values obtained after 10th iteration
        isCompleted = pagRankJob3(rPath, args[1]+"delta");
        if (!isCompleted) return 1;

        //running job to find top-100 pages
        isCompleted = pagRankJob4(args[1]+"delta", args[1]);
        if (!isCompleted) return 1;

        return 0;
    }

    // pre-processing job
    private boolean pagRankJob1(String inPath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Job1");

        job1.setJarByClass(DriverProgram.class);
        job1.setMapperClass(PageRankPreprocessing.PageRankJob1_Map.class);
        job1.setReducerClass(PageRankPreprocessing.PageRankJob1_Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job1, new Path(inPath));
        Path outputPath = new Path(outPath);
        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        boolean complete = job1.waitForCompletion(true);
        if (!complete) {
            return false;
        }

        totalNoOfPages= (int) job1.getCounters().findCounter(PAGE_COUNTER.TotalPages).getValue();

        return job1.waitForCompletion(true);
    }

    // calculating page rank job
    private boolean pageRankJob2(String inPath, String outPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.setInt("TotalPages", totalNoOfPages);
        conf.setDouble("Delta", delta);

        Job job2 = Job.getInstance(conf, "Job2");

        job2.setJarByClass(DriverProgram.class);
        job2.setMapperClass(PageRankJob.PageRank_Job2_Map.class);
        job2.setReducerClass(PageRankJob.PageRank_Job2_Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(inPath));
        Path outputPath = new Path(outPath);
        FileOutputFormat.setOutputPath(job2, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        boolean complete = job2.waitForCompletion(true);
        if (!complete) {
            return false;
        }
        delta = Double.longBitsToDouble(job2.getCounters().findCounter(PAGE_COUNTER.Delta).getValue());

        return true;
    }

    // adding additional delta to map for accurate page rank values
    private boolean pagRankJob3(String rPath, String output10)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        conf.setInt("TotalPages", totalNoOfPages);
        conf.setDouble("Delta", delta);


        Job job3 = Job.getInstance(conf, "Job3");

        job3.setJarByClass(DriverProgram.class);
        job3.setMapperClass(PageRankDeltaJob.PageRank_Job3_Map.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(rPath));
        Path outputPath = new Path(output10);
        FileOutputFormat.setOutputPath(job3, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        boolean complete = job3.waitForCompletion(true);
        if (!complete) {
            return false;
        }
        return true;
    }

    // top-100 values
    private boolean pagRankJob4(String rPath, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job4 = Job.getInstance(conf, "Top Ten Users by Reputation");
        job4.setJarByClass(DriverProgram.class);
        job4.setMapperClass(PageRankTopK.SOTopTenMapper.class);
        job4.setReducerClass(PageRankTopK.SOTopTenReducer.class);
        job4.setNumReduceTasks(1);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(rPath));
        Path o = new Path(output);
        FileOutputFormat.setOutputPath(job4, o);
        o.getFileSystem(conf).delete(o, true);

        System.exit(job4.waitForCompletion(true) ? 0 : 1);


        boolean complete = job4.waitForCompletion(true);
        if (!complete) {
            return false;
        }

        return true;
    }
}