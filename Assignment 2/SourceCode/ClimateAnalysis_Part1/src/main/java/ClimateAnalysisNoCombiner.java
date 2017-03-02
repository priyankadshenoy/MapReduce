/**
 * Created by ps on 2/1/17.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

// This class shows implementation of finding average min and max temperature without using combiner
public class ClimateAnalysisNoCombiner {

    //Mapper Class`
    public static class ClimateAnalysisNoCombiner_Map
            extends Mapper<Object, Text, Text, ClimateDataType> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] itr = value.toString().split("\n");
            for (String s: itr) {
                String[] stationDetails = s.split(",");
                // If 3rd column is "TMIN" sets minOrMax=false and creates object
                // of ClimateDataType with min value (same for max)
                boolean minOrMax = !stationDetails[2].equals("TMIN");    // returns false for MIN else return true
                word.set(stationDetails[0]);
                if(minOrMax) {
                    context.write(word, new ClimateDataType
                            (0.0, Double.parseDouble(stationDetails[3]), true));
                }
                else
                    context.write(word, new ClimateDataType
                            (Double.parseDouble(stationDetails[3]),0.0, false));
            }
        }
    }


    //Reducer Class
    public static class ClimateAnalysisNoCombiner_Reduce extends Reducer<Text, ClimateDataType, Text, NullWritable>{
        NullWritable res = NullWritable.get();
        public void reduce(Text key, Iterable<ClimateDataType> values, Context context) throws IOException, InterruptedException {
            double sumMin=0.0;
            double sumMax=0.0;
            int countForMin =0;
            int countForMax =0;
            // if value is TMIN, adds new min sum and increments counter by 1 (same for max)
            for(ClimateDataType value : values){
                if(!value.isMinOrMax())
                {
                    sumMin += value.getStationMinTemperature();
                    countForMin++;
                }
                else
                {
                    sumMax += value.getStationMaxTemperature();
                    countForMax++;
                }

            }
            // finding actual average
            Object avgMin = countForMin==0 ? "None" : sumMin / countForMin;
            Object avgMax = countForMin==0 ? "None" : sumMax / countForMax;
            String outputValue = key.toString()+ ", " + avgMin+", "+ avgMax;
            context.write(new Text(outputValue), res);


        }

    }

    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg min|max");
        job.setJarByClass(ClimateAnalysisNoCombiner.class);
        job.setMapperClass(ClimateAnalysisNoCombiner_Map.class);
        job.setReducerClass(ClimateAnalysisNoCombiner_Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ClimateDataType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path out= new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,out);
        out.getFileSystem(conf).delete(out,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
