package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Objects;

/**
 * Implementation of daisy chaining. Multicast messages are sent to our neighbor, which sends them to its neighbor etc.
 * A TTL restricts the number of times a message is forwarded. This implementation of daisy chaining does not need to
 * make copies of messages before forwarding, at the cost of requiring a custom message bundler.  Currently implemented
 * bundlers are AsyncNoBundlerDaisy and TransferQueueBundlerDaisy.
 * @author Chris Johnson
 * @since 5.x
 */
@Experimental
@MBean(description="Protocol just above the transport which disseminates multicasts via daisy chaining")
public class DAISY2 extends Protocol {

    @ManagedAttribute(description="Local address")
    protected volatile Address       local_addr;
    @ManagedAttribute(description="The member to which all multicasts are forwarded")
    protected volatile Address       next;
    @ManagedAttribute(description="The current view")
    protected volatile View          view;

    @ManagedAttribute(type=AttributeType.SCALAR)
    protected int                    msgs_forwarded;
    @ManagedAttribute(type=AttributeType.SCALAR)
    protected int                    msgs_sent;
    protected TP                     transport;


    public void resetStats() {
        super.resetStats();
        msgs_forwarded=msgs_sent=0;
    }

    public void init() throws Exception {
        transport=getTransport();
    }

    public Object down(final Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    public Object down(Message msg) {
        //System.out.println("NEW MESSAGE: " + msg);

        if(msg.getDest() != null)
            return down_prot.down(msg); // only process multicast messages

        if(next == null) // view hasn't been received yet, use the normal transport
            return down_prot.down(msg);

        if(msg.getSrc() == null && local_addr != null)
            msg.setSrc(local_addr);

        msg.putHeader(getId(), new DAISYCHAIN.DaisyHeader(msg.src()));

        return down_prot.down(msg);
    }


    public Object up(Message msg) {

        if(msg.getDest() == null && next != null
                && !(Objects.equals(next, msg.getSrc()) || Objects.equals(local_addr, msg.getSrc()))) {
            //System.out.println(msg);
            // Set DONT_LOOPBACK to prevent an infinite loopback loop.
            // This will cause messages with a different source from their true source to be looped back twice
            // but that is rare and reliability layer should catch it.
            msg.setFlag(Message.TransientFlag.DONT_LOOPBACK);
            //((TCP_DAISY)getTransport()).daisySend(msg, msg.dest());
            down_prot.down(msg);
            //System.out.println("FORWARD TO " + next + " - " + msg.getHeader(getId()));
        } else if (msg.getDest() == null && Objects.equals(next, msg.getSrc())){
            //System.out.println("NO FORWARD - " + msg.getHeader(getId()));
        }

        return up_prot.up(msg);
    }


    protected void handleView(View view) {
        this.view=view;
        Address tmp=Util.pickNext(view.getMembers(), local_addr);
        if(tmp != null && !tmp.equals(local_addr)) {
            next=tmp;
            log.debug("%s: next=%s", local_addr, next);
        }
        else
            next=null;
    }

}
