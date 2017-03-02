import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ps on 2/21/17.
 */
public class PageRankJob {

    // Read articles online which mentioned higher the value of alpha,
    // delta will not converge. To converge data we need to take a suitably
    // Google in most papers mentioned 0.2, I used 0.15 as suggested by TA-Ankur
    static double alpha = 0.15;

    public static class PageRank_Job2_Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // getting delta from counter
            double delt = Double.parseDouble(context.getConfiguration().get("Delta"));
            // getting total number of pages from counter
            int totalPagesJob = Integer.parseInt(context.getConfiguration().get("TotalPages"));

            String[] itr = value.toString().split("\n");

            for (String s : itr) {
                // getting page name, link name and page rank of page values obtained
                String pageName = s.substring(0, s.indexOf('[') - 1);
                String linkName = s.substring(s.indexOf('[') + 1, s.indexOf(']'));
                String pageRank = s.substring(s.indexOf('{') + 1, s.indexOf('}'));
                String[] linkNameListArray = linkName.split(", ");
                List<String> linkNameList = Arrays.asList(linkNameListArray);

                double pageRankValue=0;

                // checking for page rank values, -1.0 is for preprocessing only
                if(pageRank.equals("-1.0")){
                    pageRankValue = 1.0/totalPagesJob;
                }
                else
                {
                    pageRankValue= Double.parseDouble(pageRank);
                }

                // if dangling nodes are obtained add rank with delta
                if (delt != 0.0) {
                    pageRankValue += (1 - alpha) * (delt / totalPagesJob);
                }

                PageData pd = new PageData(linkNameList, pageRankValue);
                context.write(new Text(pageName), new Text(pd.toString()));

                // if outlink list is empty(dangling node) emit dummy
                // and page rank which is accumulated and aggregated by reducer
                if (linkNameList.get(0).equals("") && linkNameList.size() == 1)
                    context.write(new Text("dummy"), new Text(Double.toString(pageRankValue)));

                // if page has outlinks then divide rank by size of outlink array
                else {
                    for (String l : linkNameListArray) {
                        double rankOutLink = pageRankValue / linkNameListArray.length;
                        context.write(new Text(l), new Text(Double.toString(rankOutLink)));
                    }
                }
            }
        }
    }

    public static class PageRank_Job2_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double pageRankInterim = 0.0;
            String output = "";
            PageData pd = null;
            double s= 0.0;

            int totalPagesJob = Integer.parseInt(context.getConfiguration().get("TotalPages"));

            // iterating through values for dangling node to calculate delta
            if (key.toString().equals("dummy")){
                for (Text str : values) {
                    String pageRank = str.toString();
                    s += Double.parseDouble(pageRank);

                    // update value of delta counter
                    context.getCounter(DriverProgram.PAGE_COUNTER.Delta).setValue(Double.doubleToLongBits(s));
                }
            }
            else {
                // iterating through non dangling nodes and updating page rank
                for (Text str : values) {
                    String val = str.toString();
                    try {
                        double pageRankValue = Double.parseDouble(val);
                        pageRankInterim += pageRankValue;

                    } catch(NumberFormatException e){

                        // getting page name, link name and page rank of page values obtained
                        String linkName = val.substring(val.indexOf('[')+1, val.indexOf(']'));

                        // Used ", " as I did not consider a possibility of URLs having same string
                        // Tried running the program with "~" as a split factor on local system
                        // which did not cause significant change in results.
                        String [] linkNameListArray = linkName.split(", ");
                        String pageRank = val.substring(val.indexOf('{') + 1, val.indexOf('}'));
                        PageData pd1 = new PageData(Arrays.asList(linkNameListArray), Double.parseDouble(pageRank));

                        pd = new PageData (pd1.linkPageNames, pd1.pageRank);
                    }
                }

                pd.pageRank = (1-alpha) * (pageRankInterim)+ (alpha/totalPagesJob) ;

                // converting object to string
                output = pd.toString() ;

                context.write(key, new Text (output));
            }

        }
    }
}

