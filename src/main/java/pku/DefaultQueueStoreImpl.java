package pku;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();
    List<String> isInCaches = new LinkedList<>();
    Map<String, List<byte[]>> queueMap = new ConcurrentHashMap<String, List<byte[]>>(1000000);
    Map<String, List<byte[]>> queueMapCaches = new ConcurrentHashMap<String, List<byte[]>>(100000);

    public synchronized void put(String queueName, byte[] message) {
//        System.out.println("message ======= " + new String(message));
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new ArrayList<byte[]>());
        }
        queueMap.get(queueName).add(message);

        if (queueMap.get(queueName).size() > 100) {
            try {
                File dir = new File("data");
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                File file = new File("data/" + queueName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileOutputStream output = new FileOutputStream(file, true);
                BufferedOutputStream buffer_output = new BufferedOutputStream(output, 6144);
                Iterator<byte[]> it = queueMap.get(queueName).iterator();
                while (it.hasNext()) {
                    byte[] nxt = it.next();
                    buffer_output.write(nxt.length);
                    buffer_output.write(nxt);
                    it.remove();
                }
                buffer_output.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {
        System.out.println("offset ====== " + offset);
        if(queueMapCaches.containsKey(queueName)){
            List<byte[]> msgs = queueMapCaches.get(queueName);
            return msgs.subList((int) offset, offset + num > msgs.size() ? msgs.size() : (int) (offset + num));
        }
        try {
            File file = new File("data/" + queueName);
            if (!file.exists()) {
                return DefaultQueueStoreImpl.EMPTY;
            }else {
                List<byte[]> result= null;
                result = new ArrayList<>(300);
                FileInputStream in = new FileInputStream(file);
                int msgLen = 0;
                while ((msgLen = in.read()) > 0) {
//                while ((msgLen = in.read()) > 0 && num > 0) {
                    byte[] byteArr = new byte[msgLen];
                    in.read(byteArr);
                    System.out.println(new String(byteArr));
                    result.add(byteArr);
//                    String msg = new String(byteArr);
//                    if (Long.valueOf(msg.split(" ")[1]) == offset) {
//                        result.add(byteArr);
//                        num--;
//                        offset++;
//                    }
                }
                in.close();
                if (isInCaches.size() > 100000) {
                    queueMapCaches.remove(isInCaches.get(0));
                    isInCaches.remove(0);
                }
                isInCaches.add(queueName);
                queueMapCaches.put(queueName, result);
                return result.subList((int) offset, offset + num > result.size() ? result.size() : (int) (offset + num));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
