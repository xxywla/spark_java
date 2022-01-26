public class Utils {
    public static double calPoiToPoiDis(float a_lon, float a_lat, float b_lon, float b_lat) {
        double r = 6370996.81;
        double x1 = a_lon * Math.PI / 180;
        double x2 = b_lon * Math.PI / 180;
        double y1 = a_lat * Math.PI / 180;
        double y2 = b_lat * Math.PI / 180;
        double dx = Math.abs(x1 - x2);
        double dy = Math.abs(y1 - y2);
        double p = Math.pow(Math.sin(dx / 2), 2) + Math.cos(x1) * Math.cos(x2) * Math.pow(Math.sin(dy / 2), 2);
        double d = r * 2 * Math.asin(Math.sqrt(p));
        return d;
    }

    public static double[] wgs84ToMercator(float lon, float lat) {
        double x = lon * 20037508.342789 / 180;
        double y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180);
        y = y * 20037508.34789 / 180;
        return new double[]{x, y};
    }

    public static double[] mercatorToWgs84(double x, double y) {
        double lon = x / 20037508.34 * 180;
        double lat = y / 20037508.34 * 180;
        lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);
        return new double[]{lon, lat};
    }

    public static void main(String[] args) {
        float a_lon = 118.54f;
        float a_lat = 36.55f;
        float b_lon = 118.54659f;
        float b_lat = 36.55667f;
        double res = calPoiToPoiDis(a_lon, a_lat, b_lon, b_lat);
        System.out.println(res);
    }
}
