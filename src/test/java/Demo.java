import org.apache.spark.sql.catalyst.expressions.In;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Scanner;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

/**
 * @author: 李煌民
 * @date: 2022-01-06 19:57
 **/
public class Demo {
    public static void main(String[] args) throws Exception {
        demo();
    }

    public static void t1() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line = br.readLine();

        Scanner scanner = new Scanner(System.in);
        String next = scanner.next();

        String[] split = line.split(",");

        System.out.println(Arrays.stream(split).sorted(
                (o1, o2) -> {
                    if (Integer.parseInt(o1) > Integer.parseInt(o2)) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
        ).reduce((s, s2) -> s + s2).get());
    }

    public static void demo() {
        Scanner sc = new Scanner(System.in);
        int count = sc.nextInt();
        ArrayList<String> arr = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            arr.add(new Scanner(System.in).next());
        }

        ArrayList<Integer> list = new ArrayList<>();
        arr.stream().forEach(s -> {
            String s1 = Arrays.stream(s.split(" "))
                    .min(new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            return o2.compareTo(o1);
                        }
                    }).get();

            list.add(Integer.valueOf(s1));
        });

        System.out.println(list);

        System.out.println(list.stream().reduce(new BinaryOperator<Integer>() {
            @Override
            public Integer apply(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        }).get());
    }
}
