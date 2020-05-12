import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.ArrayList;
import java.util.Map;

class Job {
  private int subTime;
  private int id;
  private int estTime;
  private int core;
  private int memory;
  private int disk;
  public Job(int subTime, int id, int estTime, int core, int memory, int disk) {
    this.subTime = subTime;
    this.id = id;
    this.estTime = estTime;
    this.core = core;
    this.memory = memory;
    this.disk = disk;
  }
  public Job(String jobn) {
    String[] jobStr = jobn.trim().split(" ");
    this.subTime = Integer.parseInt(jobStr[1]);
    this.id = Integer.parseInt(jobStr[2]);
    this.estTime = Integer.parseInt(jobStr[3]);
    this.core = Integer.parseInt(jobStr[4]);
    this.memory = Integer.parseInt(jobStr[5]);
    this.disk = Integer.parseInt(jobStr[6]);
    //System.out.printf("job %d: estTime:%d core:%d memory:%d disk:%d\n", this.id, this.estTime, this.core, this.memory, this.disk);
  }
  public String getRESC() {
    String ret = String.format("RESC All %d %d %d", this.core, this.memory, this.disk);
    return ret;
  }
  public int getID() {
    return this.id;
  }
  public Boolean available(int core, int mem, int disk) {
    return (this.core <= core && this.memory <= mem && this.disk <= disk);
  }
  public int fit(int core, int memory, int disk) {
    return core - this.core;
  }
}

class Server {
  private String type;
  private int id;
  private int state;
  private int availTime;
  private int core;
  private int memory;
  private int disk;
  public Server(String server) {
    String[] splitStr = server.trim().split(" ");
    this.type = splitStr[0];
    this.id = Integer.parseInt(splitStr[1]);
    this.state = Integer.parseInt(splitStr[2]);
    this.availTime = Integer.parseInt(splitStr[3]);
    this.core = Integer.parseInt(splitStr[4]);
    this.memory = Integer.parseInt(splitStr[5]);
    this.disk = Integer.parseInt(splitStr[6]);
  }
  public Server(String type, int core, int mem, int disk) {
    this.type = type;
    this.core = core;
    this.memory = mem;
    this.disk = disk;
  }
  public void update(String recv) {
    String[] splitStr = recv.trim().split(" ");
    this.state = Integer.parseInt(splitStr[2]);
    this.availTime = Integer.parseInt(splitStr[3]);
  }
  public Boolean available(Job job) {
    if (this.state > 3)//Unavailable
      return false;
    return job.available(this.core, this.memory, this.disk);
  }
  public String getCNTJ() {
    return String.format("CNTJ %s %d 1", this.type, this.id);
  }
  public String show() {
    return String.format("%s %d", this.type, this.id);
  }
  public String showAll() {
    return String.format("%s %d %d %d %d %d %d", this.type, this.id, this.state, this.availTime, this.core, this.memory, this.disk);
  }
  public int fit(Job job) {
    return job.fit(this.core, this.memory, this.disk);
  }
  public int getAvailTime() {
    return this.availTime;
  }
  public String getType() {
    return this.type;
  }
}
    
public class BFClient {
    private String bestType = null;
    private OutputStream out;
    private InputStream in;
    private ArrayList<Server> servers = new ArrayList<>();
    private ArrayList<Server> systemInfo = new ArrayList<>();
    private Boolean debug = false;

    private void sendMsg(String str) throws Exception {
        if(this.debug)
        System.out.printf("send %s\n", str);
        out.write(str.getBytes());
        out.flush();
    }

    private String recvMsg() throws Exception {
        byte[] buf = new byte[100];
        in.read(buf, 0, buf.length);
        String msg = new String(buf);
        if(this.debug)
        System.out.printf("recv %s\n", msg.trim());
        return msg.trim();
    }

    private String recvMsg(String str) throws Exception {
        String msg = this.recvMsg();
        if(!msg.startsWith(str)) {
            System.err.printf("expect: %s, recv err: %s.\n", str, msg);
            System.exit(1);
        }
        return msg;
    }

    private void cntj(Job job) throws Exception {
      String send, recv;
      Boolean find = false;
      int bestFit = Integer.MAX_VALUE;
      int minAvail = Integer.MAX_VALUE;
      Server select = null;

      for(Server server: this.servers) {
        send = server.getCNTJ();
        this.sendMsg(send);
        recv = this.recvMsg();
        //if(job.getID()==19)
         //System.out.printf("server: %s, avail:%b, fit:%d\n", server.showAll(), server.available(job), server.fit(job));
        if(server.available(job) && recv.equals("0")) {
          int fit = server.fit(job);
          int availTime = server.getAvailTime();
          if (fit < bestFit || (bestFit == fit && availTime < minAvail)) {
            bestFit = fit;
            minAvail = availTime;
            select = server;
            find = true;
          }
        }
      }
      if(find) {
        send = String.format("SCHD %d %s", job.getID(), select.show()); 
        this.sendMsg(send);
        this.recvMsg("OK");
      } else {
        for(Server server: this.systemInfo) {
          if(server.available(job)) {
            int fit = server.fit(job);
            if(fit < bestFit) {
              bestFit = fit;
              select = server;
            }
          }
        }
            
        send = String.format("SCHD %d %s 0", job.getID(), select.getType()); 
        this.sendMsg(send);
        this.recvMsg("OK");
      }
    }

    private void resc(Job job) throws Exception {
        String send, recv;
        this.recvMsg("DATA");
        this.sendMsg("OK");
        this.servers.clear();
        int index = 0;
        while (true) {
            recv = this.recvMsg();
            if (recv.equals(".")) {
                this.cntj(job);
                return;
            }
              this.servers.add(new Server(recv));
            index++;
            this.sendMsg("OK");
        }
    }

    private void sendJobs() throws Exception {
      String recv = "";
      String send = "";
      Boolean first = true;
      while(true) {
        this.sendMsg("REDY");
        recv = this.recvMsg("");
        if (recv.equals("NONE"))
          break;
        Job job = new Job(recv);
        send = job.getRESC();
        this.sendMsg(send);
        this.resc(job);
        if(first)
          first = false;
      }
    }

    private void parse() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document d = builder.parse("system.xml");
        
        NodeList sList = d.getElementsByTagName("server");
        for (int i = 0;i < sList.getLength();i++) {
            Element e = (Element)sList.item(i);
            int core = Integer.parseInt(e.getAttribute("coreCount"));
            int mem = Integer.parseInt(e.getAttribute("memory"));
            int disk = Integer.parseInt(e.getAttribute("disk"));
            String type = e.getAttribute("type");
            this.systemInfo.add(new Server(type, core, mem, disk));
        }
    }


    public void run() {
        try {
            this.parse();
            Socket socket=new Socket("localhost", 50000);
            out = socket.getOutputStream();
            in = socket.getInputStream();
            this.sendMsg("HELO");
            this.recvMsg("OK");
            Map<String, String> map = System.getenv();
            String user = map.get("USERNAME");
            this.sendMsg(String.format("AUTH %s", user));
            this.recvMsg("OK");

            this.sendJobs();
            this.sendMsg("QUIT");
            this.recvMsg("QUIT");
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
