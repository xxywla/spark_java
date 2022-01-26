public class CalPoiToPoiDis {
    public static void main(String[] args) {
        float a_lon = Float.parseFloat(args[0]);
        float a_lat = Float.parseFloat(args[1]);
        float b_lon = Float.parseFloat(args[2]);
        float b_lat = Float.parseFloat(args[3]);
        double res = Utils.calPoiToPoiDis(a_lon, a_lat, b_lon, b_lat);
        System.out.println(res);
    }
}
