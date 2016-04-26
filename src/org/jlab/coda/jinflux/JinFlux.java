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

    public JinFlux(String influxDbHost, String user, String password) throws JinFluxException {
        try {
            this.influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":8086", user, password);

            // Flush every 1000 points, at least every 100ms (which one comes first)
            this.influxDB.enableBatch(1000, 100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    public JinFlux(String influxDbHost) throws JinFluxException {
        this(influxDbHost, "root", "root");
    }

    public void rpCreate(String dbName, int value, JinTime tm) throws JinFluxException {

        Query query = new Query("CREATE RETENTION POLICY " + tm.name() + "_" + value +
                " ON " + dbName + " DURATION " + value + tm.weight() + " REPLICATION 1 DEFAULT", dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    public void rpRemove(String dbName, String rpName) throws JinFluxException {
        Query query = new Query("DROP RETENTION POLICY " + rpName +
                " ON " + dbName, dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

    }


    public String rpShow(String dbName) throws JinFluxException {
        Query query = new Query("SHOW RETENTION POLICIES ON " + dbName, dbName);
        try {
            QueryResult r = influxDB.query(query);

            String rpName = null;
            boolean isRpDefault = false;

            if (r.getResults() != null) {
                for (QueryResult.Result qr : r.getResults()) {
                    for (QueryResult.Series sr : qr.getSeries()) {
                        for (List<Object> l : sr.getValues()) {
                            boolean first = true;
                            for (Object ll : l) {
                                if (first) {
                                    isRpDefault = false;
                                    rpName = (String) ll;
                                    first = false;
                                } else {
                                    if (ll instanceof Boolean) {
                                        Boolean tmp = (Boolean) ll;
                                        if (tmp) isRpDefault = true;
                                    }
                                }
                            }
                            if (isRpDefault) {
                                return rpName;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

        return null;
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

    public boolean dbCreate(String dbName) throws JinFluxException {
        influxDB.createDatabase(dbName);
        return doesDbExists(dbName);
    }

    public void dbRemove(String dbName) throws JinFluxException {
        try {
            influxDB.deleteDatabase(dbName);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    public List<String> dbList() throws JinFluxException {
        try {
            return influxDB.describeDatabases();
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    public boolean doesDbExists(String dbName) throws JinFluxException {
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

    private Point.Builder addFieldValue(Point.Builder point,
                                        String field,
                                        Object value) {

        if (value instanceof String) {
            point.addField(field, (String) value);

        } else if (value instanceof Boolean) {
            point.addField(field, (Boolean) value);

        } else if (value instanceof Long) {
            point.addField(field, (Long) value);

        } else if (value instanceof Double) {
            point.addField(field, (Double) value);

        } else if (value instanceof Number) {
            point.addField(field, (Number) value);
        }
        return point;
    }

    private Point.Builder addFieldValue(Point.Builder point,
                                        Map<String, Object> tags) {

        for (String tag : tags.keySet()) {
            addFieldValue(point, tag, tags.get(tag));
        }
        return point;
    }


    public Point.Builder open(String measurement) {

        return Point.measurement(measurement);

    }

    public Point.Builder open(String measurement,
                              String tagName,
                              String value) {

        return Point.measurement(measurement).tag(tagName, value);
    }

    public Point.Builder open(String measurement,
                              Map<String, String> tags) {

        return Point.measurement(measurement).tag(tags);

    }


    public void write(String dbName,
                      Point.Builder spot,
                      String fieldName,
                      Object fieldValue) throws JinFluxException {

        Point.Builder p = addFieldValue(spot, fieldName, fieldValue)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        // write the point to the database
        try {
            influxDB.write(dbName, "default", p.build());
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

    }

    public void write(String dbName,
                      Point.Builder spot,
                      String fieldName,
                      List<Object> fieldValues) throws JinFluxException {

        Point.Builder p = addFieldValue(spot, fieldName, fieldValues)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        // write the point to the database
        try {
            influxDB.write(dbName, "default", p.build());
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

    }

    public void write(String dbName,
                      Point.Builder spot,
                      Map<String, Object> fields) throws JinFluxException {

        Point.Builder p = addFieldValue(spot, fields)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

        // write the point to the database
        try {
            influxDB.write(dbName, "default", p.build());
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

    }


    public Map<Object, Object> read(String dbName, String measurement, String tag) throws JinFluxException {
        if (tag.equals("*")) throw new JinFluxException("wildcards are not supported");
        Map<Object, Object> rm = new LinkedHashMap<>();
        Object o1 = null;
        Object o2 = null;
        Query query = new Query("SELECT " + tag + " FROM " + measurement, dbName);
        try {
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
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return rm;
    }

    public List<String> readTags(String dbName, String measurement) throws JinFluxException {
        List<String> rl = new ArrayList<>();
        Query query = new Query("SELECT *  FROM " + measurement, dbName);
        try {
            QueryResult r = influxDB.query(query);

            for (QueryResult.Result qr : r.getResults()) {
                for (QueryResult.Series sr : qr.getSeries()) {
                    List<String> l = sr.getColumns();
                    rl.addAll(l);
                }
            }
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

        return rl;
    }

    /**
     * Queries the database. excepts "*" wildcard.
     *
     * @param dbName
     * @param measurement
     */
    public void dump(String dbName, String measurement) throws JinFluxException {
        Query query = new Query("SELECT * FROM " + measurement, dbName);

        try {
            QueryResult r = influxDB.query(query);

            System.out.println(r);
            printQueryResult(r);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

    }

    public boolean resetDb(String dbName) throws JinFluxException {
        if (doesDbExists(dbName)) dbRemove(dbName);
        return dbCreate(dbName);

    }

    private void printQueryResult(QueryResult r) {
        if (r.getResults() != null) {
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
}
