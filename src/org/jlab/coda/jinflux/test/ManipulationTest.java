package org.jlab.coda.jinflux.test;

import org.jlab.coda.jinflux.JinFlux;
import org.jlab.coda.jinflux.JinFluxException;
import org.jlab.coda.jinflux.JinTime;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/19/16
 * @version 3.x
 */
public class ManipulationTest {
    public static void main(String[] args) {
        String dbName = "afecs";
        String dbNode = "claraweb.jlab.org";
        String expid = "experiment_7";
        String runTypeTag = "config";
        String runTypeValue = "R2_RunType";
        int retention = 1;
        JinTime retentionMeasure = JinTime.DAY;


        // connect to the database
        try {

            JinFlux jinFlux = new JinFlux(dbNode);


//            // create db
//            jinFlux.createDB(dbName, 1, JinTime.DAY);
//
//            // check if db exists
//            System.out.println(jinFlux.existsDB(dbName));
//
//
//            // print default retention policy
//            System.out.println(jinFlux.showRP(dbName));
//
//
//            Map<String, String> tags = new HashMap<>();
//
//            tags.put(runTypeTag, runTypeValue);
//            tags.put("codaName", "ROC1");
//            for (int j = 0; j<10;j++) {
//                Point.Builder p = jinFlux.open(expid, tags);
//                jinFlux.addDP(p, "eventRate", 123.3);
//                jinFlux.addDP(p, "type", "ROC");
//                jinFlux.write(dbName, p);
//                JinUtil.sleep(1);
//            }
//
//            tags.put(runTypeTag, runTypeValue);
//            tags.put("codaName", "ROC2");
//            for (int j = 0; j<10;j++) {
//                Point.Builder p = jinFlux.open(expid, tags);
//                jinFlux.addDP(p, "eventRate", 123.3);
//                jinFlux.addDP(p, "type", "ROC");
//                jinFlux.write(dbName, p);
//                JinUtil.sleep(1);
//            }
//
//
//            JinUtil.sleep(1000);

            // dumpTB the measurement
            jinFlux.dumpTB(dbName, expid);

        } catch (JinFluxException e) {
            e.printStackTrace();
            System.exit(1);
        }


    }
}
