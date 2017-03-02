import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by ps on 2/21/17.
 */

// Job responsible for pre-processing data
class PageRankPreprocessing {

    //static double totalPages = 0;
    public static class PageRankJob1_Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //creating object of pre-processing calling method
            //which converts into human-readable form
            PreProcessing pre = new PreProcessing();
            String itr = value.toString();
            PageData p = pre.readXML(itr);

            // sample output for A [B C D] is 'A' 'B' 'C' 'D' and 'A B C D'
            if (p != null) {
                for (String s : p.linkPageNames) {

                    // writing all out link pages
                    context.write(new Text(s), new Text());
                }

                // writing page names
                context.write(new Text(p.pageName), new Text());

                // writing page name and outlinks
                context.write(new Text(p.pageName), new Text(p.linkPageNames.toString()));
            }
        }
    }


    public static class PageRankJob1_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String output = "";

            // iterates over all values for a specific key and stores in output variable
            for (Text t : values) {
                    output = output + t.toString();
            }

            // if key has no values output has only []
            if(output.equals(""))
            {
                output= "[]";
            }

            // adding default -1.0 page rank for pre-processing
            output = output + " {-1.0}";

            //incrementing total pages counter by 1
            context.getCounter(DriverProgram.PAGE_COUNTER.TotalPages).increment(1);

            context.write(key, new Text(output));
        }

    }

}

