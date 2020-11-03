package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    public boolean ok; // whether the response is a okay message for the corresponding request (e.g. prepare -> prepareOk, accept -> acceptOk)
    public int seq;
    public int n;
    public Object value;

    // Your constructor and methods here
    public Response(boolean ok, int seq, int n, Object value) {
        this.ok = ok;
        this.seq = seq;
        this.n = n;
        this.value = value;
    }
}
