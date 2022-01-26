package model;

import java.io.Serializable;
import java.util.Date;

public class Point2 implements Serializable {
    private float longitude;
    private float latitude;
    private Date utc;
    private String planNumber;
    private String truckNumber;

    public Point2() {

    }

    public Point2(float longitude, float latitude, Date utc, String planNumber, String truckNumber) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.utc = utc;
        this.planNumber = planNumber;
        this.truckNumber = truckNumber;
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

    public Date getUtc() {
        return utc;
    }

    public void setUtc(Date utc) {
        this.utc = utc;
    }

    public String getPlanNumber() {
        return planNumber;
    }

    public void setPlanNumber(String planNumber) {
        this.planNumber = planNumber;
    }

    public String getTruckNumber() {
        return truckNumber;
    }

    public void setTruckNumber(String truckNumber) {
        this.truckNumber = truckNumber;
    }

    @Override
    public String toString() {
        return "Point2{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", utc=" + utc +
                ", planNumber='" + planNumber + '\'' +
                ", truckNumber='" + truckNumber + '\'' +
                '}';
    }
}
