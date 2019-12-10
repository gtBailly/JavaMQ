package pku;

import java.io.File;
import java.io.FileInputStream;

public class testa {
    public static void main(String[] args) {
        try {
            File file = new File("data/Queue-0");
            FileInputStream in = new FileInputStream(file);
            int msgLen = 0;
            while ((msgLen = in.read()) > 0) {
                byte[] byteArr = new byte[msgLen];
                in.read(byteArr);
                String msg = new String(byteArr);
                System.out.println(msg);
                System.out.println(Long.valueOf(msg.split(" ")[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}