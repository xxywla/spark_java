import java.util.Arrays;

public class Wgs84ToMercator {
    public static void main(String[] args) {
        float lon = Float.parseFloat(args[0]);
        float lat = Float.parseFloat(args[1]);
        double[] res = Utils.wgs84ToMercator(lon, lat);
        System.out.println(Arrays.toString(res));
    }
}
