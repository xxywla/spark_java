package model;

import java.io.Serializable;

public class Point implements Serializable {
    private float longitude;
    private float latitude;

    public Point(float longitude, float latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    @Override
    public String toString() {
        return "model.Point{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
}
