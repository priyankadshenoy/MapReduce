import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ps on 2/4/17.
 */
// This class shows implementation of finding average min and max temperature with in mapper combiner
public class ClimateAnalysisInMapperCombiner {
    public static class ClimateAnalysisInMapperCombiner_MapCombiner
            extends Mapper<Object, Text, Text, ClimateDataType> {

        Map<Text, ClimateDataType> inMap;

        // creating hashmap used for in mapper computation
        protected void setup(Mapper.Context context) {
             inMap = new HashMap<Text, ClimateDataType>();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Text word = new Text();
            String[] itr = value.toString().split("\n");

            // If 3rd column is "TMIN" sets minOrMax=false and creates object
            // of ClimateDataType with min value (same for max)
            for (String s: itr) {
                String[] stationDetails = s.split(",");
                word.set(stationDetails[0]);
                double temperature= Double.parseDouble(stationDetails[3]);
                if(stationDetails[2].equals("TMAX")) {

                    // checks if key is present in hashmap and updates it, else adds new entry to hashmap
                    if (inMap.containsKey(word)) {
                        inMap.get(word).updateMax(temperature);
                    }
                    else
                    inMap.put(word, new ClimateDataType
                            (0.0,
                                    Double.parseDouble(stationDetails[3]),
                                    0,
                                    1));
                }
                else if(stationDetails[2].equals("TMIN"))
                {
                    if (inMap.containsKey(word)) {
                        inMap.get(word).updateMin(temperature);
                    }
                    else{
                   inMap.put(word, new ClimateDataType
                            (Double.parseDouble(stationDetails[3]),
                                    0.0,
                                    1,
                                    0));
                    }
                }
            }
        }

        // writes key value pair to be sent to reducer
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Text key: inMap.keySet()){
                context.write(key , inMap.get(key));
            }
        }
    }

    // Reducer class
    public static class ClimateAnalysisInMapperCombiner_Reducer
            extends Reducer<Text, ClimateDataType, Text, NullWritable> {
        NullWritable res = NullWritable.get();

        //reduce method adds all individual values of tmax, tmin and count
        //of ClimateDataType and finds average
        public void reduce(Text key, Iterable<ClimateDataType> values, Context context)
                throws IOException, InterruptedException {
            double sumMin = 0.0;
            double sumMax = 0.0;
            int count_min = 0;
            int count_max = 0;
            for (ClimateDataType value : values) {
                sumMin += value.getStationMinTemperature();
                sumMax += value.getStationMaxTemperature();
                count_min += value.getCountMin();
                count_max += value.getCountMax();
            }

            Object avgMin = count_min==0? "None" : sumMin / count_min;
            Object avgMax = count_max==0? "None" : sumMax / count_max;
            String outputValue = key.toString() + "," + avgMin + "," + avgMax;
            context.write(new Text(outputValue), res);

        }
    }

    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg min|max");
        job.setJarByClass(ClimateAnalysisInMapperCombiner.class);
        job.setMapperClass(ClimateAnalysisInMapperCombiner_MapCombiner.class);
        job.setReducerClass(ClimateAnalysisInMapperCombiner_Reducer.class);
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

