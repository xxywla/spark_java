import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestSomething {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date utc = simpleDateFormat.parse("2021-03-04 12:00:32");
        System.out.println(utc);
    }
}
