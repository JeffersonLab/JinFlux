package org.jlab.coda.jinflux;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class description here....
 * <p>
 *
 * @author gurjyan
 *         Date 4/19/16
 * @version 3.x
 */
public class JinFlux {
    private InfluxDB influxDB;

    public JinFlux(String influxDbHost, String user, String password) {
        this.influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":8086", user, password);

        // Flush every 1000 points, at least every 100ms (which one comes first)
        this.influxDB.enableBatch(1000, 100, TimeUnit.MILLISECONDS);
    }

    public JinFlux(String influxDbHost) {
        this(influxDbHost, "root", "root");
    }

    public boolean ping(int timeout) throws Exception {
        int tries = 0;
        timeout = timeout * 10;
        boolean influxStarted = false;
        do {
            Pong response;
            response = this.influxDB.ping();
            if (!response.getVersion().equalsIgnoreCase("unknown")) {
                influxStarted = true;
            }
            Thread.sleep(100L);
            tries++;
        } while (!influxStarted || (tries < timeout));

        return (tries < timeout);
    }

    public boolean dbCreate(String dbName) {
        influxDB.createDatabase(dbName);
        return doesDbExists(dbName);
    }

    public void dbRemove(String dbName) {
        influxDB.deleteDatabase(dbName);
    }

    public List<String> dbList() {
        return influxDB.describeDatabases();
    }

    public boolean doesDbExists(String dbName) {
        boolean exists = false;
        List<String> result = dbList();
        if (result != null && result.size() > 0) {
            for (String database : result) {
                if (database.equals(dbName)) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

    private Point.Builder addTagValue(Point.Builder point,
                                      String tag,
                                      Object value) {

        if (value instanceof String) {
            point.addField(tag, (String) value);

        } else if (value instanceof Boolean) {
            point.addField(tag, (Boolean) value);

        } else if (value instanceof Long) {
            point.addField(tag, (Long) value);

        } else if (value instanceof Double) {
            point.addField(tag, (Double) value);

        } else if (value instanceof Number) {
            point.addField(tag, (Number) value);
        }
        return point;
    }

    private Point.Builder addTagValue(Point.Builder point,
                                      List<String> tags,
                                      List<Object> values) throws JinFluxException {
        if (tags.size() != values.size())
            throw new JinFluxException("Inconsistent tab-value list sizes");

        for (String tag : tags) {
            for (Object value : values) {
                addTagValue(point, tag, value);
            }
        }
        return point;
    }

    private Point getJinxPoint(String measurement,
                               String tag,
                               Object value) {

        Point.Builder point = Point.measurement(measurement);

        addTagValue(point, tag, value)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        return point.build();
    }

    private Point getJinxPoint(String measurement,
                               List<String> tags,
                               List<Object> values) throws JinFluxException {

        Point.Builder point = Point.measurement(measurement);
        addTagValue(point, tags, values);

        point.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        return point.build();
    }

    private Point getJinxPointArray(String measurement,
                                    String tag,
                                    List<Object> values) {

        Point.Builder point = Point.measurement(measurement);
        int i = 0;
        for (Object value : values) {
            addTagValue(point, tag + i, value);
            i++;
        }

        point.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        return point.build();
    }

    public void writeSingle(String dbName, String measurement,
                            String tag,
                            Object value) {

        Point point = getJinxPoint(measurement, tag, value);
        // write the point to the database
        influxDB.write(dbName, "default", point);
    }

    public void writeHisto(String dbName, String measurement,
                           String tag,
                           List<Object> value) {

        Point point = getJinxPointArray(measurement, tag, value);
        // write the point to the database
        influxDB.write(dbName, "default", point);
    }


    public void writeGroup(String dbName, String measurement,
                           List<String> tags,
                           List<Object> values) throws JinFluxException {
        Point point = getJinxPoint(measurement, tags, values);
        // write the point to the database
        influxDB.write(dbName, "default", point);
    }

    public Map<Object, Object> read(String dbName, String measurement, String tag) throws JinFluxException {
        if (tag.equals("*")) throw new JinFluxException("wildcards are not supported");
        Map<Object, Object> rm = new LinkedHashMap<>();
        Object o1 = null;
        Object o2 = null;
        Query query = new Query("SELECT " + tag + " FROM " + measurement, dbName);
        QueryResult r = influxDB.query(query);
        if (r != null) {
            for (QueryResult.Result qr : r.getResults()) {
                for (QueryResult.Series sr : qr.getSeries()) {
                    for (List<Object> l : sr.getValues()) {
                        boolean first = true;
                        for (Object ll : l) {
                            if (first) {
                                o1 = ll;
                                first = false;
                            } else {
                                o2 = ll;
                                first = true;
                            }
                            if (first) {
                                rm.put(o1, o2);
                            }
                        }
                    }
                }
            }
        }
        return rm;
    }

    public List<String> readTags(String dbName, String measurement) {
        List<String> rl = new ArrayList<>();
        Query query = new Query("SELECT *  FROM " + measurement, dbName);
        QueryResult r = influxDB.query(query);
        for (QueryResult.Result qr : r.getResults()) {
            for (QueryResult.Series sr : qr.getSeries()) {
                rl.add(sr.getName());
            }
        }
        return rl;
    }

    /**
     * Queries the database. excepts "*" wildcard.
     *
     * @param dbName
     * @param measurement
     * @param tag
     */
    public void dump(String dbName, String measurement, String tag) {
        Query query = new Query("SELECT " + tag + " FROM " + measurement, dbName);
        QueryResult r = influxDB.query(query);
        for (QueryResult.Result qr : r.getResults()) {
            for (QueryResult.Series sr : qr.getSeries()) {
                System.out.println("===================================================");
                System.out.println("              " + sr.getName());
                System.out.println("---------------------------------------------------");

                for (String column : sr.getColumns()) {
                    if (column.equals("time"))
                        System.out.print(column + "                        ");
                    else System.out.print(column + "\t");
                }
                System.out.println();
                System.out.println("---------------------------------------------------");
                for (List<Object> l : sr.getValues()) {
                    boolean first = true;
                    for (Object ll : l) {
                        if (first) {
                            System.out.print(ll + "\t");
                            first = false;
                        } else {
                            System.out.print(ll + "\t\t");
                        }
                    }
                    System.out.println();
                }
                System.out.println("===================================================");
            }
        }
    }

}
