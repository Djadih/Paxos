package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    // One client -- multiple servers
    // For a particular server, say the "i-th" one, upon receiving a Put(key, value) Op from the client
    //
    Map<String, Integer> kvStore; // each Server's kvStore (the state machine) should be identical.
    List<Op> pendingPutOps; // pending put operations to apply to kvStore. Must be drained before serving any Get operation.
    int highestSeqForLearnedCommand; // highest seq number for a Put operation that we have learned from other servers.



    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        this.kvStore = new HashMap<>();
        this.pendingPutOps = new LinkedList<>();
        this.highestSeqForLearnedCommand = -1;





        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    // RMI handlers
    public Response Get(Request req){
        // Your code here
        // 0. catch up on all commands up until the one preceding this newly received req command.
        int seqOfOp = req.operation.ClientSeq;
        learnCommandsUpToSeq(seqOfOp - 1);

        // 1. start to reach an agreement for seqOfOp-th operation
        px.Start(seqOfOp, req.operation);
        wait(seqOfOp);
        drainPendingPutOps();
        Integer value = kvStore.get(req.operation.key);

        return new Response(value);
    }

    public Response Put(Request req){
        // Your code here
        // 0. catch up on all commands up until the one preceding this newly received req command.
        int seqOfOp = req.operation.ClientSeq;
        learnCommandsUpToSeq(seqOfOp - 1);

        // 1. start to reach an agreement for seqOfOp-th operation by proposing as the only proposer for seqOfOp-th operation
        px.Start(seqOfOp, req.operation);
        wait(seqOfOp);
        pendingPutOps.add(req.operation);

        return new Response(null);
    }

    // helper method
    private void drainPendingPutOps() {
        for (Op op : pendingPutOps) {
            kvStore.put(op.key, op.value);
        }
        pendingPutOps.clear();
    }


    private Op wait(int seq) {
        int to = 10;
        while (true) {
            Paxos.retStatus ret = this.px.Status(seq);
            if (ret.state == State.Decided) {
                return (Op) ret.v;
            }
            try {
                Thread.sleep(to);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (to < 1000) {
                to = to * 2;
            }
        }
    }

    // learn all the command from this.highestSeqForLearnedCommand + 1 to seq, left and right inclusive.
    private void learnCommandsUpToSeq(int seq) {
        if (highestSeqForLearnedCommand + 1 <= seq) {
            // this server doesn't know commands from highestSeqForLearnedCommand+1, ..., req.operation.ClientSeq - 1.
            for (int catchUpSeq = highestSeqForLearnedCommand + 1; catchUpSeq <= seq; ++catchUpSeq) {
                // the proposed value does not matter because it won't get accepted anyway.
                // We are just using the Start functionality to learn the catchUpSeq-th accepted command
                // (Read the testDeaf in PaxosTest.java to see how a similar thing is used to enable a deaf
                // server to learn about the decided value from others).
                px.Start(catchUpSeq, null);
                Op decidedOpAtCatchUpSeq = wait(catchUpSeq);
                // If the operation at catchUp position is a Put command, then add it to our pendingPut array
                // if it's a Get command, do nothing because that Get command must have been served by some other server.
                if (decidedOpAtCatchUpSeq.op.equals("Put")) {
                    pendingPutOps.add(decidedOpAtCatchUpSeq);
                }
                highestSeqForLearnedCommand = catchUpSeq;
            }
        }
    }


}
