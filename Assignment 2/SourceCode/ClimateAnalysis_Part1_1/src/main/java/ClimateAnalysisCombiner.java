/**
 * Created by ps on 2/4/17.
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

// This class shows implementation of finding average min and max temperature with combiner
public class ClimateAnalysisCombiner {

    public static class ClimateAnalysisCombiner_Map
            extends Mapper<Object, Text, Text, ClimateDataType> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] itr = value.toString().split("\n");
            // If 3rd column is "TMIN" sets minOrMax=false and creates object
            // of ClimateDataType with min value (same for max)
            for (String s: itr) {
                String[] stationDetails = s.split(",");
                word.set(stationDetails[0]);

                if(stationDetails[2].equals("TMAX")) {
                    context.write(word, new ClimateDataType
                            (0.0,
                                    Double.parseDouble(stationDetails[3]),
                                    0,
                                    1));
                }
                else if(stationDetails[2].equals("TMIN"))
                {
                    context.write(word, new ClimateDataType
                            (Double.parseDouble(stationDetails[3]),
                            0.0,
                            1,
                            0));
                }

            }
        }
    }

    // This class shows implementation of finding total tmax/tmin sum as combiner class
    // we find only the sum and increment count since it is not certain if combiner will be executed or not
    public static class ClimateAnalysisCombiner_Combiner
            extends Reducer<Text, ClimateDataType, Text, ClimateDataType> {
        public void reduce(Text key, Iterable<ClimateDataType> values, Context context) throws IOException, InterruptedException {
            double sumMin = 0.0;
            double sumMax = 0.0;
            int count_min = 0;
            int count_max = 0;
            // adding tmax and tmin sum and incrementing count respectively
            for (ClimateDataType value : values) {
                sumMin += value.getStationMinTemperature();
                sumMax += value.getStationMaxTemperature();
                count_min += value.getCountMin();
                count_max += value.getCountMax();
            }
            context.write(key, new ClimateDataType(sumMin, sumMax, count_min, count_max));

        }
    }

    //Reducer Class
    public static class ClimateAnalysisCombiner_Reducer
            extends Reducer<Text, ClimateDataType, Text, NullWritable>{
        NullWritable res = NullWritable.get();
        public void reduce(Text key, Iterable<ClimateDataType> values, Context context) throws IOException, InterruptedException {
            double sumMin=0.0;
            double sumMax=0.0;
            int count_min=0;
            int count_max=0;
            // adding tmax and tmin sum and incrementing count respectively
            for(ClimateDataType value : values){
                sumMin += value.getStationMinTemperature();
                sumMax += value.getStationMaxTemperature();
                count_min += value.getCountMin();
                count_max += value.getCountMax();
            }
            // finding actual average
            Object avgMin = count_min==0 ? "None" : sumMin / count_min;
            Object avgMax = count_max==0 ? "None" : sumMax / count_max;
            String outputValue = key.toString()+ ", " + avgMin+", "+ avgMax;
            context.write(new Text(outputValue), res);


        }

    }

    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg min|max");
        job.setJarByClass(ClimateAnalysisCombiner.class);
        job.setMapperClass(ClimateAnalysisCombiner_Map.class);
        job.setCombinerClass(ClimateAnalysisCombiner_Combiner.class);
        job.setReducerClass(ClimateAnalysisCombiner_Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ClimateDataType.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path out= new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);
        out.getFileSystem(conf).delete(out,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}

