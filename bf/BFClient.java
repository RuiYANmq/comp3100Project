package bf;
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
import java.util.List;
import java.util.Hashtable;
import java.util.Collections;
import java.util.Comparator;

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
    String ret = String.format("RESC Capable %d %d %d", this.core, this.memory, this.disk);
    return ret;
  }
  public int getID() {
    return this.id;
  }
  public Boolean available(int core, int mem, int disk) {
    return (this.core <= core && this.memory <= mem && this.disk <= disk);
  }
    public int getCore() {
        return this.core;
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
  public void update(int avail, int core, int mem, int disk) {
    this.availTime = avail;
    this.core = core;
    this.memory = mem;
    this.disk = disk;
  }
  public Boolean available(Job job) {
    if (this.state == 4)//Unavailable
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
    return this.core - job.getCore();
  }
  public int getAvailTime() {
    return this.availTime;
  }
  public String getType() {
    return this.type;
  }
    public int getState() {
        return this.state;
    }
}
    
class Type {
    private String name;
    private int limit;
    private int core;
    private int memory;
    private int disk;
    private List<Server> servers = new ArrayList<Server>();
    public Type(String name, int limit, int core, int memory, int disk) {
        this.name = name;
        this.limit = limit;
        this.core = core;
        this.memory = memory;
        this.disk = disk;
    }
    public void addServer(Server s) {
        this.servers.add(s);
    }
    public String getType() {
        return this.name;
    }
    public int computeFitness(Job job)  {
        return this.core - job.getCore();
    }
    public Server getServer(int id) {
        return this.servers.get(id);
    }
    public List<Server> getServers() {
        return this.servers;
    }
    public int getServerSize() {
        return this.servers.size();
    }
    public int getCore() {
        return this.core;
    }
    public Boolean available(Job job) {
        return job.available(this.core, this.memory, this.disk);
    }
    public Boolean exist(int id) {
      if(this.servers.size() > id)
        return true;
      return false;
    }
}
 
class SystemInfo {
    private List<Type> types = new ArrayList<Type>();
    private Hashtable<String, Type> typeMap = new Hashtable<>();
    public SystemInfo() {}
    public void addType(Type type) {
        this.types.add(type);
        this.typeMap.put(type.getType(), type);
    }
    public void sortType() {
        Collections.sort(this.types, new Comparator<Type>() {
            @Override
            public int compare(Type t1, Type t2) {
                return t1.getCore() - t2.getCore();
            }
        });
    }
    public void updateServer(String recv) {
        String[] splitStr = recv.split(" ");
        String type = splitStr[0];
        int id = Integer.parseInt(splitStr[1]);
        Type typeObj = this.typeMap.get(type);
        if(typeObj.exist(id)) {
          Server s = this.typeMap.get(type).getServer(id);
          int avail = Integer.parseInt(splitStr[3]);
          int core = Integer.parseInt(splitStr[4]);
          int mem = Integer.parseInt(splitStr[5]);
          int disk = Integer.parseInt(splitStr[6]);
          s.update(avail, core, mem, disk);
        } else {
          typeObj.addServer(new Server(recv));
        }
    }
    public Type getType(String type) {
        return this.typeMap.get(type);
    }
    public List<Type> getTypeList() {
        return this.types;
    }
}

public class BFClient {
    private String bestType = null;
    private OutputStream out;
    private InputStream in;
    private Boolean debug = false;
    private SystemInfo system = new SystemInfo();

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
        return msg.trim();
    }

    private void cntj(Job job) throws Exception {
      String send, recv;
      Boolean find = false;
      int bestFit = Integer.MAX_VALUE;
      int minAvail = Integer.MAX_VALUE;
      int wstFit = Integer.MAX_VALUE;

      Server select = null;
    Server selectW = null;

    for(Type type: this.system.getTypeList()) {
      for(Server server: type.getServers()) {
        send = server.getCNTJ();
        this.sendMsg(send);
        recv = this.recvMsg();
        //if(job.getID()==19)
         //System.out.printf("server: %s, avail:%b, fit:%d\n", server.showAll(), server.available(job), server.fit(job));
        int state = server.getState();
        int fit = server.fit(job);
        if(server.available(job)) {
          int availTime = server.getAvailTime();
          if (fit < bestFit || (bestFit == fit && availTime < minAvail)) {
            bestFit = fit;
            minAvail = availTime;
            select = server;
            find = true;
          }
        //} else if ((state % 2 == 1) && type.available(job)) {
        } else if (type.available(job)) {
            fit = type.computeFitness(job);
            if (fit < wstFit) {
                wstFit = fit;
                selectW = server;
            }
        }
      }
    }

      if(find) {
        send = String.format("SCHD %d %s", job.getID(), select.show()); 
        this.sendMsg(send);
        this.recvMsg("OK");
      } else {
        send = String.format("SCHD %d %s", job.getID(), selectW.show()); 
        this.sendMsg(send);
        this.recvMsg("OK");
      }
    }

    private void resc(Job job, Boolean first) throws Exception {
        String send, recv;
        this.recvMsg("DATA");
        this.sendMsg("OK");
        int index = 0;
        while (true) {
            recv = this.recvMsg();
            if (recv.equals(".")) {
                this.cntj(job);
                return;
            }
            this.system.updateServer(recv);
            index++;
            this.sendMsg("OK");
        }
    }

    private void resc(Job job) throws Exception {
        this.resc(job, false);
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
        this.resc(job, first);
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
            int limit = Integer.parseInt(e.getAttribute("limit"));
            int core = Integer.parseInt(e.getAttribute("coreCount"));
            int mem = Integer.parseInt(e.getAttribute("memory"));
            int disk = Integer.parseInt(e.getAttribute("disk"));
            String type = e.getAttribute("type");
            this.system.addType(new Type(type, limit, core, mem, disk));
        }
    }


    public void run() {
        try {
            Socket socket=new Socket("localhost", 50000);
            out = socket.getOutputStream();
            in = socket.getInputStream();
            this.sendMsg("HELO");
            this.recvMsg("OK");
            Map<String, String> map = System.getenv();
            String user = map.get("USERNAME");
            this.sendMsg(String.format("AUTH %s", user));
            this.recvMsg("OK");
            this.parse();

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
