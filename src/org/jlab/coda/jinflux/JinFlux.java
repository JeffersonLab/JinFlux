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

    /**
     * Constructor
     * Opens connection to the InfluxDB server using authentication.
     * Enables batch mode where we flush every 1000 points at least
     * every 100 milli seconds (which one comes first)
     * <p>
     *
     * @param influxDbHost host of the InfluxDB server
     * @param port port number of the InfluxDB server
     * @param user database username
     * @param password password
     * @throws JinFluxException
     */
    public JinFlux(String influxDbHost, int port, String user, String password) throws JinFluxException {
        try {
            this.influxDB = InfluxDBFactory.connect("http://" + influxDbHost + ":"+port, user, password);

            // Flush every 1000 points, at least every 100ms (which one comes first)
            this.influxDB.enableBatch(1000, 100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
    }

    /**
     * Constructor
     * Opens connection to the InfluxDB server using authentication.
     * Uses default server port = 8086
     * <p>
     *
     * @param influxDbHost host of the InfluxDB server
     * @param user database username
     * @param password password
     * @throws JinFluxException
     */
    public JinFlux(String influxDbHost, String user, String password) throws JinFluxException {
        this(influxDbHost,8086,user,password);
     }

    /**
     * Constructor
     * Opens connection to the InfluxDB server using default authentication and port number.
     * <p>
     *
     * @param influxDbHost
     * @throws JinFluxException
     */
    public JinFlux(String influxDbHost) throws JinFluxException {
        this(influxDbHost, 8086, "root", "root");
    }


    /**
     * Shows retention policy, configured for a database
     * <p>
     *
     * @param dbName database name
     * @return String representing the retention policy
     * @throws JinFluxException
     */
    public String rpShow(String dbName) throws JinFluxException {
        Query query = new Query("SHOW RETENTION POLICIES ON " + dbName, dbName);
        try {
            QueryResult r = influxDB.query(query);

            boolean isRpDefault = false;

            if (r.getResults() != null) {
                for (QueryResult.Result qr : r.getResults()) {
                    for (QueryResult.Series sr : qr.getSeries()) {
                        for (List<Object> l : sr.getValues()) {
                            boolean first = true;
                            StringBuilder sb = new StringBuilder();
                            for (Object ll : l) {
                                sb.append(ll).append(" ");
                                if (first) {
                                    isRpDefault = false;
                                    first = false;
                                } else {
                                    if (ll instanceof Boolean) {
                                        Boolean tmp = (Boolean) ll;
                                        if (tmp) isRpDefault = true;
                                    }
                                }
                            }
                            if (isRpDefault) {
                                return sb.toString();
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

    /**
     * Ping the database server to see if it is up and running
     * <p>
     *
     * @param timeout in seconds
     * @return true if database is up and running
     * @throws Exception
     */
    public boolean ping(int timeout) throws Exception {
        int tries = 0;
        timeout = timeout * 10;
        boolean influxStarted = false;
        do {
            Pong response = this.influxDB.ping();

            if (!response.getVersion().equalsIgnoreCase("unknown")) {
                influxStarted = true;
            }
            Thread.sleep(100L);
            tries++;
        } while (!influxStarted && (tries < timeout));

        return (tries < timeout);
    }

    /**
     * Creates a database with specific data retention policy
     * <p>
     *
     * @param dbName database name
     * @param retentionTime retention time
     * @param tm retention time unite {@link JinTime}
     * @param rpName retention policy name
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean dbCreate(String dbName, int retentionTime, JinTime tm, String rpName)
            throws JinFluxException {
        if(doesDbExists(dbName)){
            dbRemove(dbName);
        }
        Query query = new Query("CREATE DATABASE " + dbName +
                " WITH DURATION " + retentionTime+tm.weight()+" REPLICATION 1 NAME "+rpName, dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }
        return doesDbExists(dbName);
    }

    /**
     * Creates a database with specific data retentio
     * <p>
     *
     * @param dbName database name
     * @param retentionTime retention time
     * @param tm retention time unite {@link JinTime}
     * @return true if succeeded to create the database
     * @throws JinFluxException
     */
    public boolean dbCreate(String dbName, int retentionTime, JinTime tm)
            throws JinFluxException {
        return dbCreate(dbName, retentionTime, tm, "rpafecs");
     }

    public boolean dbCreate(String dbName)
            throws JinFluxException {
        influxDB.createDatabase(dbName);
        return doesDbExists(dbName);
    }



    public void measureRemove(String dbName, String measurement) throws JinFluxException {
        Query query = new Query("DROP MEASUREMENT " + measurement, dbName);
        try {
            influxDB.query(query);
        } catch (Exception e) {
            throw new JinFluxException(e.getMessage());
        }

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

    public Point.Builder add(Point.Builder point,
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

        } else if (value instanceof Float) {
            point.addField(field, (Float) value);

        } else if (value instanceof Number) {
            point.addField(field, (Number) value);

        }else if (value instanceof List) {
            List l = (List)value;
            for(int i=0; i<l.size();i++) {
                if(l.get(i) instanceof Number) {
                    point.addField(field + "_" + i, (Number)l.get(i));
                } else break;
            }
        }
        return point;
    }

    public Point.Builder add(Point.Builder point,
                                        Map<String, Object> tags) {

        for (String tag : tags.keySet()) {
            add(point, tag, tags.get(tag));
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


    public void flush(String dbName,
                      Point.Builder spot) throws JinFluxException {

        spot.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        // write the point to the database
            influxDB.write(dbName, "rpafecs", spot.build());

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

    public boolean dbReset(String dbName) throws JinFluxException {
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
