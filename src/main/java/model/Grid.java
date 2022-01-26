package model;

import java.io.Serializable;

public class Grid implements Serializable {
    private final Double min_longitude;
    private final Double max_longitude;
    private final Double min_latitude;
    private final Double max_latitude;
    private final int n = 10;
    private final int mod = 191;

    public Grid(Double min_longitude, Double max_longitude, Double min_latitude, Double max_latitude) {
        this.min_longitude = min_longitude;
        this.max_longitude = max_longitude;
        this.min_latitude = min_latitude;
        this.max_latitude = max_latitude;
    }

    public int getMod() {
        return mod;
    }

    public int getN() {
        return n;
    }

    public int getHashId(double lon, double lat) {
        double box_lon = (max_longitude - min_longitude) / n;
        double box_lat = (max_latitude - min_latitude) / n;
        int x = (int) ((lon - min_longitude) / box_lon);
        int y = (int) ((lat - min_latitude) / box_lat);
        return x * mod + y;
    }

    public int getHashId(Point poi) {
        double lon = poi.getLongitude();
        double lat = poi.getLatitude();
        return getHashId(lon, lat);
    }
}
