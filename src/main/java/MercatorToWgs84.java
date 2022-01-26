import java.util.Arrays;

public class MercatorToWgs84 {
    public static void main(String[] args) {
        double x = Double.parseDouble(args[0]);
        double y = Double.parseDouble(args[1]);
        double[] res = Utils.mercatorToWgs84(x, y);
        System.out.println(Arrays.toString(res));
    }
}
