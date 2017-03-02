import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * Created by ps on 2/7/17.
 */
public class ClimateAnalysis_SecondarySort {

    // Mapper class
    public static class ClimateAnalysisSecondarySort_Map
            extends Mapper<Object, Text, CompositeKey, ClimateDataType> {

        // map method splits the incoming csv file into year, temperature and adds to
        // accumulation data structure according to TMAX or TMIN values

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int year;

            String[] itr = value.toString().split("\n");
            for (String s: itr) {
                String[] stationDetails = s.split(",");
                year = Integer.parseInt(stationDetails[1].substring(0,4));
                double temperature= Double.parseDouble(stationDetails[3]);

                //Since our output format is ID, year ... I have composite key for
                //adding station and year specific data

                CompositeKey ckey=new CompositeKey(stationDetails[0], year);

                // according to TMAX or TMIN adding to accumulation data structure

                if(stationDetails[2].equals("TMAX")) {

                    context.write(ckey, new ClimateDataType
                            (0.0, temperature,0,1));

                }
                else if(stationDetails[2].equals("TMIN"))
                {
                    context.write(ckey, new ClimateDataType
                            (temperature,0.0, 1,0));
                    }
                }
            }
    }

    //Partitioner Class
    public static class SortPartitioner extends
            Partitioner<CompositeKey, ClimateDataType> {

        //Partitions the data coming out of Mapper into number of parts according to available machines
        // i.e.reduce tasks in our case 5 {Using Math.abs since hascode()%5 may go < or > permissible int value}

        @Override
        public int getPartition(CompositeKey compositeKey, ClimateDataType cliData, int reduceTasks) {

            return (Math.abs(compositeKey.getStationId().hashCode()%reduceTasks));

        }
    }

    //Grouping Comparator Class
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(CompositeKey.class, true);
        }

        //Used for comparing ONLY stationID. Specifically used in reducer when we need
        //to group values by year and rest of station data

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeKey key1 = (CompositeKey) w1;
            CompositeKey key2 = (CompositeKey) w2;
            return key1.getStationId().compareTo(key2.getStationId());
        }
    }


    //Natural Comparator Class
    public static class NaturalComparator extends WritableComparator {

        //Used for comparing entire composite key, this is the comparator normally used
        // when compare to is called.

        protected NaturalComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeKey key1 = (CompositeKey) w1;
            CompositeKey key2 = (CompositeKey) w2;

            int cmpResult = key1.getStationId().compareTo(key2.getStationId());
            if (cmpResult != 0)
                return cmpResult;
            else
                return key1.getYear().compareTo(key2.getYear());
        }
    }


    // Combiner class -- May or May not be called
    // Initially I used InMapperCombiner to reduce load on network but changed
    // to normal combiner to avoid using excessive memory through HashMap
    public static class ClimateAnalysisSecondarySort_Combiner
            extends Reducer<CompositeKey, ClimateDataType, CompositeKey, ClimateDataType> {
        public void reduce(CompositeKey key, Iterable<ClimateDataType> values, Context context)
                throws IOException, InterruptedException {
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
    public static class ClimateAnalysisSecondarySort_Reducer
            extends Reducer<CompositeKey,ClimateDataType,Text,NullWritable> {

        private NullWritable result = NullWritable.get();
        public void reduce (CompositeKey key, Iterable<ClimateDataType> values, Context context)
                throws IOException, InterruptedException {
            double sumMin = 0.0;
            double sumMax = 0.0;
            int count_min = 0;
            int count_max = 0;
            StringBuilder output = new StringBuilder("");
            output.append(key.getStationId()).append(",[");

            //the following code checks if the year under consideration is same as previous
            // data, if it is, it only updates the temperature and count values
            // else it creates new values

            int prevyear = key.getYear();
            for (ClimateDataType value : values) {
                if (key.getYear() == prevyear) {
                    sumMin += value.getStationMinTemperature();
                    sumMax += value.getStationMaxTemperature();
                    count_min += value.getCountMin();
                    count_max += value.getCountMax();
                }
                else {
                    output.append(",");
                    sumMin = value.getStationMinTemperature();
                    sumMax = value.getStationMaxTemperature();
                    count_min = value.getCountMin();
                    count_max = value.getCountMax();
                }

                //display output :: using "none" id we get NaN
                Object avgMin = Double.isNaN(sumMin/count_min)? "None" : sumMin/count_min;
                Object avgMax = Double.isNaN(sumMax/count_max)? "None" : sumMax/count_max;
                output.append("(")
                        .append(key.getYear())
                        .append(",")
                        .append(avgMin)
                        .append(",")
                        .append(avgMax)
                        .append(")");
            }
            output.append("]");
            context.write(new Text(output.toString()), result);
        }
    }


    public static void main(String args[])throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "avg min|max");
        job.setJarByClass(ClimateAnalysis_SecondarySort.class);
        job.setMapperClass(ClimateAnalysisSecondarySort_Map.class);
        job.setSortComparatorClass(NaturalComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setCombinerClass(ClimateAnalysisSecondarySort_Combiner.class);
        job.setReducerClass(ClimateAnalysisSecondarySort_Reducer.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(ClimateDataType.class);
        job.setPartitionerClass(SortPartitioner.class);
        job.setNumReduceTasks(5);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path out= new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,out);
        out.getFileSystem(conf).delete(out,true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
