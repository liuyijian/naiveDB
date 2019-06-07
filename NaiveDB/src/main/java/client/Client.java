package client;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;

public class Client {
    /*
        Server运行后，允许创建连接，后面的可选（Server没运行时，报错或阻塞）
        循环读取SQL命令并发给Server，直至退出
     */
    public static int PORT = 12306;
    public static String IP_ADDR = "localhost";

    public static void main(String[] args) {
    	if (args.length == 2) {
    		IP_ADDR = args[0];
    		PORT = Integer.parseInt(args[1]);
    	}
        try (
                Socket socket = new Socket(IP_ADDR, PORT);
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            ) {
            System.out.println("NaiveDB client is running!");

            while(true) {
                String outputStr = bufferedReader.readLine();
                if (outputStr.startsWith("import")) {

                    String fileName = outputStr.split("\\s+")[1];
                    // 此处用了第三方jar包org.apache.commons.io.FileUtil
                    // 参考http://commons.apache.org/proper/commons-io/javadocs/api-2.6/index.html
                    try {
                        outputStr = FileUtils.readFileToString(new File(fileName), "UTF-8");
                    } catch (IOException e){
                        System.out.println("找不到txt文件");
                        continue;
                    }
                    out.writeUTF(outputStr);
                    long startTime = System.nanoTime();
                    String inputStr = input.readUTF();
                    long endTime = System.nanoTime();
                    double costTime = ((double)endTime - (double)startTime) / 1000000;
                    System.out.println("用时（毫秒）：\n" + costTime);
                    System.out.println("返回结果：\n" + inputStr);                 
                    if (inputStr.toUpperCase().indexOf("BYE") != -1) {
                        System.out.println("客户端将关闭连接");
                        throw new IOException("客户端主动退出");
                    }
                }
                else {
                    out.writeUTF(outputStr);
                    long startTime = System.nanoTime();
                    String inputStr = input.readUTF();
                    long endTime = System.nanoTime();
                    double costTime = ((double)endTime - (double)startTime) / 1000000;
                    System.out.println("用时（毫秒）：\n" + costTime);
                    System.out.println("返回结果：\n" + inputStr);
                    if (inputStr.toUpperCase().indexOf("BYE") != -1) {
                    	socket.close();
                        System.out.println("客户端将关闭连接");
                        throw new IOException("客户端主动退出");
                    }
                }
            }
        } catch (ConnectException e) {
            System.out.println("无法连接到服务器（忘了打开服务器了吗？）");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
