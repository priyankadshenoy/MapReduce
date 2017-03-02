//import java.io.*;
//
//import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
///**
// * Created by ps on 2/22/17.
// */
//
//public class HumanReadableForm {
//    public static void main(String args[]) throws IOException {
//        if (args.length != 1) {
//            System.exit(1);
//        }
//        BufferedReader reader = null;
//        File inputFile = new File(args[0]);
//        if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
//            System.out.println("File does not exist" + args[0]);
//            System.exit(1);
//        }
//
//        BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
//        reader = new BufferedReader(new InputStreamReader(inputStream));
//        PrintWriter out = new PrintWriter(new FileWriter("HumanReadable.txt"));
//        String line;
//
//        while ((line = reader.readLine()) != null) {
//            Document html = Jsoup.parse(line);
//            out.print(html);
//        }
//        out.close();
//
//    }
//}