import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by ps on 2/18/17.
 */
//Class which holds data about pages
class PageData implements Writable {
    String pageName;
    List<String> linkPageNames;
    double pageRank;

    PageData(){}

    PageData(String pageName, List<String> linkPageNames) {
        this.pageName = pageName;
        this.linkPageNames = linkPageNames;
    }


    PageData(List<String> linkPageNames, double pageRank) {
        this.linkPageNames = linkPageNames;
        this.pageRank= pageRank;
    }




    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pageName);
        for(String str : linkPageNames){
            dataOutput.writeUTF(str);
        }
        dataOutput.writeDouble(pageRank);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pageName= dataInput.readUTF();
        this.pageRank = dataInput.readDouble();

    }

    // overridden toString() to convert PageData object to String
    public String toString(){
        if(linkPageNames!=null)
        {
            // Used ", " as I did not consider a possibility of URLs having same string
            // Tried running the program with "~" as a split factor on local system
            // which did not cause significant change in results.

            String joinedString = String.join(", ", linkPageNames);
            return "[" + joinedString +"]" + "{" + pageRank + "}";
        }

        else {
            return "{" + pageRank + "}";
        }
    }
}
