import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankTopK {

    public static class SOTopTenMapper extends
            Mapper<Object, Text, Text, Text> {
        // Our output key and value Writables
        private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String values= value.toString();

            // getting page name, link name and page rank of page values obtained
            String pageRank = values.substring(values.indexOf('{') + 1, values.indexOf('}'));
            String pageName = values.substring(0, values.indexOf('[') - 1);

            double pageRankValue = Double.parseDouble(pageRank);

            String finalText = pageRankValue +" "+pageName;

            repToRecordMap.put(pageRankValue, new Text(finalText));

            if (repToRecordMap.size() > 100) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : repToRecordMap.values()) {
                //String output= t.toString() + " " + repToRecordMap.get(t);
                context.write(new Text("hello"), new Text(t));
            }
        }
    }

    public static class SOTopTenReducer extends
            Reducer<Text, Text, Text, Text> {

        private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            for (Text value : values) {

                String valueS= value.toString();
                String[] v= valueS.split(" ");

                double keyV = Double.parseDouble(v[0]);
                repToRecordMap.put(keyV, new Text(v[1]));


                if (repToRecordMap.size() > 100) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            for (Double t : repToRecordMap.descendingMap().keySet()) {
                context.write(new Text(repToRecordMap.get(t)),new Text(t.toString()));
            }
        }
    }
}