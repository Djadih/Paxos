package kvpaxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class Client {
    String[] servers;
    int[] ports;

    // Your data here
    // the client determined what is the next seq number to use.
    // In other words, the client keeps track of how much Put or Get it has
    // issued and number them from 0 to nextSeqToUse - 1.
    int nextSeqToUse;


    public Client(String[] servers, int[] ports){
        this.servers = servers;
        this.ports = ports;
        // Your initialization code here
        this.nextSeqToUse = 0;
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;
        KVPaxosRMI stub;
        try{
            Registry registry= LocateRegistry.getRegistry(this.ports[id]);
            stub=(KVPaxosRMI) registry.lookup("KVPaxos");
            if(rmi.equals("Get"))
                callReply = stub.Get(req);
            else if(rmi.equals("Put")){
                callReply = stub.Put(req);}
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    // TODO: 1. At-most-once semantics on the server-side:
    //  According to the comments on Call(...), we should assume Call(...) will time out and
    //  return Null if the server does not reply after a while. This means it is possible that client C
    //  sends a Put operation to server S1. S1 is working but it just takes a bit long to respond and hence
    //  C thought S1 is dead and get a Null from the Call(seq, "Put", S1). C then go ahead and re-send the
    //  same command to S2. For S2, upon receiving the command, it should be able to first check whether this
    //  command is already handled by other servers (in this case S1) and don't apply this command if some other
    //  server has already applied this command. In other words, we want to enforce a At-Most-Once semantics
    //  on the server side. We have implemented an At-Lease-Once semantics on the client side (by re-sending
    //  messages through Call(...)). These two together gives Exactly-Once semantics, meaning each operation
    //  will be executed on each server exactly once.

    // TODO: 2. Read the homework handout and see what things are not clear. Pay attention to sentences that do
    //  not make much sense to us, because those are usually signs of things to do.

    // TODO: 3. Post a "trolling" post on Piazza, saying that the test for part B is so trivial that it's not worth
    //  the effort to read, understand, implement, and debug part B. Why? It's because the test assumes no failure
    //  of machines. So, we can "cheat" the test by simply asking the client to always talk to the same server,
    //  say, server_0, and the implementation of Server is merely a wrapper of a HashMap<String, Integer> --
    //  server.Get() returns map.get(), server.Put() does map.put(). Cann't be simpler, right?
    //  In fact, I started with this approach, and it's verified to be able to pass the test..... so why bother?

    // RMI handlers
    public Integer Get(String key){
        // Your code here
        int id = 0;
        Response response;
        while ((response = Call("Get", new Request("Get", nextSeqToUse++, key, null), id)) == null) {
            id = (id + 1) % this.servers.length;
        };
        return response.responseValue;
    }

    public boolean Put(String key, Integer value){
        // Your code here
        int id = 0;
        Response response;
        while ((response = Call("Put", new Request("Put", nextSeqToUse++, key, value), id)) == null) {
            id = (id + 1) % this.servers.length;
        };

        return true;
    }

}
