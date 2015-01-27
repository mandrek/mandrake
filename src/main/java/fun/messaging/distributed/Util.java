package fun.messaging.distributed;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultByteArrayNodeFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created by kuldeep on 1/26/15.
 */
public class Util {

    static final Random rand = new Random(System.currentTimeMillis());

    public static String getRandString(int len) {

        char[] seq = new char[len];
        for (int i = 0; i < len; i++)
            seq[i] = (char) ('A' + rand.nextInt(26));

        return new String(seq);
    }

    public static List<String> getMultipleRandStrings(int stringLen, int noOfStrings) {
        List<String> strs = new ArrayList<>();
        for (int i = 0; i < noOfStrings; i++)
            strs.add(getRandString(stringLen));

        return strs;
    }

    public static List<byte[]> converToBytes(List<String> strings) {

        List<byte[]> values = new ArrayList<>();
        for (String str : strings) {
            try {
                byte[] bytes = str.getBytes("UTF-8");
                values.add(bytes);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return values;

    }

    public static void main(String[] args) {
        RadixTree<Object> tree = new ConcurrentRadixTree<Object>(new DefaultByteArrayNodeFactory());
        tree.put("SPX", "SPX");
        tree.put("SPY", "SPY");
        tree.put("SPXXX", "SPXX");

        Iterable msgs = tree.getValuesForKeysStartingWith("SPXXY");
        for (Object msg : msgs)
            System.out.println(msg);

        List<String> strings = getMultipleRandStrings(10, 20);
        RadixTree<String> t = new ConcurrentRadixTree<>(new DefaultByteArrayNodeFactory());
        long t1 = System.currentTimeMillis();
        for (String str : strings)
            t.put(str, str);

        long t2 = System.currentTimeMillis();
        Map<String, String> map = new HashMap<>();
        //strings = getMultipleRandStrings(10, 1000000);
        long t3 = System.currentTimeMillis();


        for (String str : strings)
            map.put(str, str);
        long t4 = System.currentTimeMillis();

        long t5 = System.currentTimeMillis();
        for (String str : strings)
            t.getValueForExactKey(str);

        long t6 = System.currentTimeMillis();


        long t7 = System.currentTimeMillis();
        for (String str : strings)
            map.get(str);

        long t8 = System.currentTimeMillis();

        long t9 = System.currentTimeMillis();
        for (String str : strings)
            t.getKeysStartingWith(str);

        long t10 = System.currentTimeMillis();

        System.out.println("Put->Trie: " + (t2 - t1) + " Map: " + (t4 - t3));
        System.out.println("Get->Trie: " + (t6 - t5) + " Map: " + (t8 - t7) + " Prefix: " + (t10 - t9));
        FilterTrie<byte[]> ft = new FilterTrie<>();

        List<byte[]> byteArrays = converToBytes(strings);


        long t11 = System.currentTimeMillis();

        for (byte[] str : byteArrays)
            ft.add(str, str);


        long t12 = System.currentTimeMillis();

        long t13 = System.currentTimeMillis();

        int counter = 0;
        for (byte[] str : byteArrays) {
            if (ft.get(str) != null)
                counter++;
            else {
                ft.print();
                //System.out.println(new String(str));
                for (int i = 0; i < byteArrays.size(); i++)
                    System.out.println(new String(byteArrays.get(i)));

                System.out.println(new String(str));

                break;
            }

            //ft.get(str);
        }


        long t14 = System.currentTimeMillis();
        System.out.println("Filter Trie Put: " + (t12 - t11) + " Get: " + (t14 - t13) + " Counter: " + counter);
        //ft.print();


        /*
        QKGJDXFCPZ
                DVHTKYLERV
        KNVWOAJFFP
                RYXXIBIAIN
        RNMVWYZMWE
                RCOUWAGSFP
        RJWPISCTCD
                TZWVCGTSSB
        VSXTABCDWR
                SJPRSIZNGA
        NXXOOBIPRM
                ZWAFMMWBGM
        KDKLHACMQL
                ESXMXVIBSA
        NKQOTNUBWF
                DOBTDNKUPI
        KBBJDRTOPH
        */


    }
}
