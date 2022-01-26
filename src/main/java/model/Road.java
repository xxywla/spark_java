package model;

import java.io.Serializable;

public class Road implements Serializable {
    private int id;
    private Point start_point;
    private Point end_point;


    public Road(int id, Point start_point, Point end_point) {
        this.id = id;
        this.start_point = start_point;
        this.end_point = end_point;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Point getStart_point() {
        return start_point;
    }

    public void setStart_point(Point start_point) {
        this.start_point = start_point;
    }

    public Point getEnd_point() {
        return end_point;
    }

    public void setEnd_point(Point end_point) {
        this.end_point = end_point;
    }

    @Override
    public String toString() {
        return "model.Road{" +
                "id=" + id +
                ", start_point=" + start_point +
                ", end_point=" + end_point +
                '}';
    }
}
