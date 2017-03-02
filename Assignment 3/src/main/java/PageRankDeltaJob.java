import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ps on 2/22/17.
 */
// extra map task for correcting delta values after 10th iteration
class PageRankDeltaJob {

    // Read articles online which mentioned higher the value of alpha,
    // delta will not converge. To converge data we need to take a suitably
    // Google in most papers mentioned 0.2, I used 0.15 as suggested by TA-Ankur
    static double alpha = 0.15;

    public static class PageRank_Job3_Map extends Mapper<Object, Text, Text, Text> {

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

                // checking for page rank values, -1.0 is for post preprocessing only
                if(pageRank.equals("-1.0")){
                    pageRankValue = 1.0/totalPagesJob;
                }

                // for all other times
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
            }
        }
    }
}
