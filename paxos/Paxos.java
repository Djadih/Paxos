package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
/*
 TODO: 1. Read the testDeaf test and understand what the expected behavior is. How is it different from the
           testBasic test?
       2. Run the testDeaf test, and expect to see errors
       3. What are the errors? and which method does it suggest we need to work on (a new method that we never touched/implemented
       or a old method (damn, that's a bad sign then)).
       4. read the Min() comments carefully and think about how to implement Min().
       5. the next test....
 */

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    public class ProposalNumberGenerator {
        private int me;
        private int N;
        private int cnt;

        public ProposalNumberGenerator(int me, int N) {
            this.me = me;
            this.N = N;
            cnt = 0;
        }

        public int next() {
            return me + N * (cnt++);
        }
    }

    public class PaxosInstance {
        public ProposalNumberGenerator gen; // gen.next() returns the next proposer number. Guaranteed the return number is totally ordered
        public int n_p; // highest number in a prepare request to which this acceptor has responded. This implies you should only update n_p in prepare request handler.
        public int n_a; // proposal number of the highest-numbered proposal this acceptor has ever accepted. This implies you should only update n_a, v_a in accept request handler.
        public Object v_a; // proposal value of the highest-numbered proposal this acceptor has ever accepted.
        public Object valueOriginallyPlanned; // value originally planned to propose; passed from the client code in Start() method.
        public State state;

        public PaxosInstance(ProposalNumberGenerator gen) {
            this.gen = gen;
            n_p = -1;
            n_a = -1;
            v_a = null;
            valueOriginallyPlanned = null;
            state = State.Pending;
        }
    }

    private Map<Integer, PaxosInstance> paxosInstances; // mapping from instance ID (sometimes called sequence number or just seq) to an instance of this Paxos peer (i.e. an instance of the server this Paxos object tries to model)
    public int[] highestDoneSeqs; // highest sequence number ever passed to each Paxos peer
    private int currSeq;





    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        paxosInstances = new HashMap<>();
        highestDoneSeqs = new int[this.peers.length];
        Arrays.fill(highestDoneSeqs, -1);


        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
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

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }

    private Response ReliableCall(String rmi, Request req, int id) {
        if (!rmi.equals("Prepare") && !rmi.equals("Accept") && !rmi.equals("Decide")) {
            throw new IllegalArgumentException();
        }
        Response result = null;

        if (id == this.me) {
            // if it's calling the handler on the same server then use function call
            if (rmi.equals("Prepare")) { result = Prepare(req); }
            else if (rmi.equals("Accept")) { result = Accept(req); }
            else { result = Decide(req); }
        } else {
            // otherwise, use RMI
            result = Call(rmi, req, id);
        }

        return result;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here

        // Ignore the proposal if seq < the minimal sequence number that is done for all servers
        if (seq < Min()) {
            return;
        }

        System.out.println("Start(" + seq + ", " + value + ").");
        if (!this.paxosInstances.containsKey(seq)) {
            this.paxosInstances.put(seq, new PaxosInstance(new ProposalNumberGenerator(this.me, this.peers.length)));
        }
        PaxosInstance paxosInstance = this.paxosInstances.get(seq);
        paxosInstance.valueOriginallyPlanned = value;


        this.currSeq = seq; // a hacky way to pass "seq" information to run().
        System.out.println("about to run run(). this.currSeq = " + this.currSeq);
        this.run();
    }

    @Override
    public void run(){
        //Your code here
        int currSeq = this.currSeq;
        PaxosInstance paxosInstance = this.paxosInstances.get(currSeq); // precondition: Start() should make sure there is a PaxosInstance in the map with key=currSeq.

        // repeated try to issue a proposal with a monotonically increasing proposal number until decided
        while (paxosInstance.state != State.Decided) {
            Response[] responses = new Response[this.peers.length];

            // 0. Get the next proposal number
            int n = paxosInstance.gen.next(); // e.g. 3 servers within one instance: server_1: 0, 3, 6, 9...    server_2: 1, 4, 7, 10, ...     server_3: 2, 5, 8, 11,....
            System.out.println("Phase 1, proposing n = " + n + " nPaxos = " + this.peers.length);

            // 1. Send a "prepare" request to all servers including itself
            for (int id = 0; id < this.peers.length; ++id) {
                // "prepare" requests don't have a value, only a proposal number
                responses[id] = ReliableCall("Prepare", new Request(currSeq, n, null), id);
                System.out.println("responses["+id+"] = " +responses[id]);
            }

            // 2. If received prepareOK from majority of acceptors
            if (isMajorityResponsesOK(responses)) {
                System.out.println("phase 2");
                // 2.1. find the value to propose for proposal number n.
                Object valToPropose = highestNumberedProposalValue(responses, paxosInstance.valueOriginallyPlanned);

                // 2.2. send "accept" request to all servers including itself
                for (int id = 0; id < this.peers.length; ++id) {
                    responses[id] = ReliableCall("Accept", new Request(currSeq, n, valToPropose), id);
                }

                // 3. If received acceptOK from majority of acceptors
                if (isMajorityResponsesOK(responses)) {
                    // 3.1. send "decide" request to all servers including itself
                    for (int id = 0; id < this.peers.length; ++id) {
                        responses[id] = ReliableCall("Decide", new Request(currSeq, n, valToPropose), id);
                        // learn the highest number ever passed to Done() for server "id".
                        if (responses[id] != null) { this.highestDoneSeqs[id] = responses[id].highestDoneSeq; }
                    }
                }
            }
        }
    }

    // check whether the majority of responses are OK
    private boolean isMajorityResponsesOK(Response[] responses) {
        int majorityNumber = responses.length / 2 + 1;
        int numberOfOKResponses = 0;
        for (Response r : responses) {
            if (r != null && r.ok) {
                numberOfOKResponses += 1;
            }
        }
        return numberOfOKResponses >= majorityNumber;
    }

    // return the highest numbered proposal value from responses, or valuePlannedOriginally if responses report no proposal
    private Object highestNumberedProposalValue(Response[] responses, Object valuePlannedOriginally) {
        Object result = null;
        int currHighestProposalNumber = -1;
        for (Response r : responses) {
            if (r != null && r.ok && r.n > currHighestProposalNumber) {
                currHighestProposalNumber = r.n;
                result = r.value;
            }
        }
        return result == null ? valuePlannedOriginally : result;
    }


    // RMI handler
    public Response Prepare(Request req){
        // your code here

        // get "me" (this server) at the instance that this req is referring to
        if (!this.paxosInstances.containsKey(req.seq)) {
            this.paxosInstances.put(req.seq, new PaxosInstance(new ProposalNumberGenerator(this.me, this.peers.length)));
        }
        PaxosInstance me = this.paxosInstances.get(req.seq);

        boolean ok = false;

        if (req.n > me.n_p) {
            me.n_p = req.n;
            ok = true;
        }

        return new Response(ok, req.seq, me.n_a, me.v_a); // respond with a promise and the highest-numbered accepted proposal
    }

    public Response Accept(Request req){
        // your code here
        if (!this.paxosInstances.containsKey(req.seq)) {
            this.paxosInstances.put(req.seq, new PaxosInstance(new ProposalNumberGenerator(this.me, this.peers.length)));
        }
        PaxosInstance me = this.paxosInstances.get(req.seq);
        boolean ok = false;


        if (req.n >= me.n_p) {
//            me.n_p = req.n;
            me.n_a = req.n;
            me.v_a = req.value;
            ok = true;
        }

        return new Response(ok, req.seq, req.n, req.value); // the seq, n, value fields in the response are not used
    }

    public Response Decide(Request req){
        // your code here
        if (!this.paxosInstances.containsKey(req.seq)) {
            this.paxosInstances.put(req.seq, new PaxosInstance(new ProposalNumberGenerator(this.me, this.peers.length)));
        }
        PaxosInstance me = this.paxosInstances.get(req.seq);


        boolean ok = false;
        if (me.state != State.Decided) {
            me.state = State.Decided;
            me.v_a = req.value;
            ok = true;
        }

        return new Response(ok, req.seq, req.n, req.value, this.highestDoneSeqs[this.me]); // the seq, n, value fields in the response are not used
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        this.highestDoneSeqs[this.me] = seq;
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        return Collections.max(this.paxosInstances.keySet());
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        return Arrays.stream(this.highestDoneSeqs).min().getAsInt() + 1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        if (!this.paxosInstances.containsKey(seq)) {
            this.paxosInstances.put(seq, new PaxosInstance(new ProposalNumberGenerator(this.me, this.peers.length)));
        }
        PaxosInstance me = this.paxosInstances.get(seq);

        State state = me.state;

        if (seq < Min()) {
            state = State.Forgotten;
        }

        return new retStatus(state, me.v_a);
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
