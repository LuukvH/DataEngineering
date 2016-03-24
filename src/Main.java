import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by laj on 8-3-2016.
 */

// Questions
// =========
// 1. Find out where the peaks of amount of edits are (per article if id’s are comparable) per community.
// 2. Which language has the most active unique users during which time (where an active user is a user that has during a timespan X at least Y number of posts).
// 3. Measure the similarity of pages (expressed in the number of edits for this article over a certain time span and the number of unique users that were involved in these edits)
// 4. Cascading of edits over time
// 5. Evolution of editing behaviour (will need data for a long time span)

public class Main {

    public static void main(String[] args) throws Exception {
        Vraag3.Solve();
    }
}