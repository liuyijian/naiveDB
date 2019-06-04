package server;

import metadata.MetaData;
import sqlparser.SQLParser;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    /*

        加载恢复元数据信息，
        接受client连接
        循环接受SQL命令，直至exit退出，销毁client的socket
        把命令发给SQLParser类处理，不同命令对应不同的操作（写在Parser里还是写个util类？）
     */
    // 服务器监听端口
    public static final int PORT = 12306;

    // （可选）维护一个客户端连接的socket列表，如果要做的话，记得客户退出后，把列表更新
    // List<Socket> socketList;

    public SQLParser sqlParser;

    //初始化服务器
    public void init() {
        // 出错的话会自动回收socket
        // 初始化一个parser类并加载元数据
        sqlParser = new SQLParser();
        // 监听连接
        try(ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("naiveDB server is running!");
            while(true) {
                // 阻塞接受客户端连接
                Socket client = serverSocket.accept();
                // 为客户端新建线程
                new HandlerThread(client);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //维护客户端线程的内部类
    private class HandlerThread implements Runnable {

        public Socket socket;

        public HandlerThread(Socket socket) {
            this.socket = socket;
            new Thread(this).start();
        }

        @Override
        public void run() {
            try (
                // 新建数据输入输出流对象，写入try的资源块中
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                ){

                while(true) {
                    // 此处未来可扩展的功能：权限认证，客户端quit命令，等等

                    //读取客户端数据
                    String inputStr = input.readUTF();
//                    System.out.println("客户端发来数据是：\n" + inputStr);
                    //向客户端发送数据，为调用SQLParser类的犯法处理输入后返回的结果
                    String outputStr = sqlParser.dealer(inputStr).toString();
                    out.writeUTF(outputStr);
                }
            } catch (IOException e) {
                System.out.println("服务器异常" + e.getMessage());
            } catch (Exception e) {
                // 这里修改为自定义的Exception，来对应client的quit命令，SQLParser中处理到quit就去throw一个出来;
                System.out.println("客户端终止了连接" + e.getMessage());
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (Exception e) {
                        socket = null;
                        System.out.println("服务器finally异常" + e.getMessage());
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        server.init();
    }
}


