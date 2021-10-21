package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AckChecker;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Implementation of daisy chaining. Multicast messages are sent to our neighbor, which sends them to its neighbor etc.
 * A TTL restricts the number of times a message is forwarded. The advantage of daisy chaining is that - for
 * point-to-point transports such as TCP - we can avoid the N-1 issue: when A sends a multicast message to 10
 * members, it needs to send it 9 times. With daisy chaining, it sends it 1 time, and in the next round, can already
 * send another message. This leads to much better throughput, see the ref in the JIRA.<p/>
 * Should be inserted just above MERGE3, in TCP based configurations.
 * JIRA: https://jira.jboss.org/browse/JGRP-1021
 * @author Bela Ban
 * @since 2.11
 */
@Experimental
@MBean(description="Protocol just above the transport which disseminates multicasts via daisy chaining")
public class DAISYCHAIN extends Protocol {

    @Property(description="Loop back multicast messages")
    boolean loopback=true;

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
        if(msg.getDest() != null)
            return down_prot.down(msg); // only process multicast messages

        if(next == null) // view hasn't been received yet, use the normal transport
            return down_prot.down(msg);

        if(loopback) {
            if(msg.getSrc() == null && local_addr != null)
                msg.setSrc(local_addr);
            if(log.isTraceEnabled())
                log.trace("%s: looping back message %s", local_addr, msg);
            transport.loopback(msg, true);
        }

        // we need to copy the message, as we cannot do a msg.setSrc(next): the next retransmission
        // would use 'next' as destination  !
        Message copy=msg.copy(true, true).setDest(next).setSrc(null)
          .putHeader(getId(), new DaisyHeader(msg.src()));
        msgs_sent++;
        if(log.isTraceEnabled())
            log.trace("%s: forwarding multicast message %s (hdrs: %s) to %s", local_addr, msg, msg.getHeaders(), next);
        return down_prot.down(copy);
    }

    public Object up(Message msg) {
        //ackChecker.processNak(msg);
        DaisyHeader hdr=msg.getHeader(getId());
        if(hdr == null)
            return up_prot.up(msg);

        // 1. forward the message to the next in line if src != next and srx still in view
        if(log.isTraceEnabled())
            log.trace("%s: received message from %s", local_addr, hdr.getSrc());
        if(!Objects.equals(hdr.getSrc(), next) &&
                (view == null || view.containsMember(hdr.getSrc()))) {
            Message copy=msg.copy(true, true)
              .setDest(next).setSrc(null).putHeader(getId(), hdr);
            msgs_forwarded++;
            if(log.isTraceEnabled())
                log.trace("%s: forwarding message to %s with src=%s", local_addr, next, hdr.getSrc());
            down_prot.down(copy);
        }
        //System.out.println(String.format("up: %s -> %s", msg.getSrc(), msg.getDest()));

        // 2. Pass up
        msg.setDest(null);
        msg.setSrc(hdr.getSrc());
        return up_prot.up(msg);
    }

    public void upBatch(MessageBatch batch) {
        for(Message msg: batch) {
            DaisyHeader hdr=msg.getHeader(getId());
            if(hdr == null) {
                continue;
            }
            // 1. forward the message to the next in line if src != next and srx still in view
            if(log.isTraceEnabled())
                log.trace("%s: received message from %s", local_addr, hdr.getSrc());
            if(!Objects.equals(hdr.getSrc(), next) &&
                    (view == null || view.containsMember(hdr.getSrc()))) {
                Message copy=msg.copy(true, true)
                        .setDest(next).setSrc(null).putHeader(getId(), hdr);
                msgs_forwarded++;
                if(log.isTraceEnabled())
                    log.trace("%s: forwarding message to %s with src=%s", local_addr, next, hdr.getSrc());
                down_prot.down(copy);
            }
        }
        up_prot.up(batch);
    }

    public void up(MessageBatch batch) {
        // Check seen before
        int batchSeen = checkNeedRebatching(batch);

        // No daisychain messages, so forward up.
        if (batchSeen == 0) {
            up_prot.up(batch);
            return;
        }

        // If not seen before, rebatch
        if (batchSeen == 2) {
            reBatch2(batch);
            return;
        }

        // Batch is all seen, should be on the right thread now (multicast, correct sender)
        if (batchSeen == 1) {
            upBatch(batch);
            return;
        }

        throw new RuntimeException("Daisychain batch shouldn't reach here");
    }

    public int checkNeedRebatching(MessageBatch batch) {
        boolean hasDaisyMessages = false;
        for(Message msg: batch) {
            DaisyHeader hdr=msg.getHeader(getId());
            if(hdr != null) {
                hasDaisyMessages = true;
                // Check if any messages have sender different from batch sender
                if (msg.getDest() != null) {
                    return 2;
                }
            }
        }
        return hasDaisyMessages ? 1 : 0;
    }

    public void reBatch(MessageBatch batch) {
        // For now just resubmit messages to thread pool with correct sender
        // Later can look into passing them as batches.

        // Add cluster header so thread pool doesn't throw exception
        TpHeader tpheader = new TpHeader(batch.clusterName());
        for(Message msg: batch) {
            DaisyHeader hdr=msg.getHeader(getId());
            if (hdr != null) {
                if (msg.getDest() == null) {
                    log.warn("REBATCHED TWICE. SOMETHING WENT WRONG");
                }
                msg.setDest(null);
                msg.setSrc(hdr.getSrc());
            }
            msg.putHeader(transport.getId(), tpheader);
            transport.submitMessage(msg);
        }
    }

    public void reBatch2(MessageBatch batch) {
        // Divides batch into per sender batches then resubmits to transport messaging policy

        Map<Address, MessageBatch> senderMap = new HashMap<>();

        int size = batch.size();

        for(Iterator<Message> it = batch.iterator(); it.hasNext();) {
            final Message msg = it.next();
            DaisyHeader hdr;
            if(msg == null || (hdr=msg.getHeader(getId())) == null)
                continue;

            it.remove();
            if (msg.getDest() == null) {
                log.warn("REBATCHED TWICE. SOMETHING WENT WRONG");
            }
            msg.setDest(null);
            msg.setSrc(hdr.getSrc());

            MessageBatch senderBatch;
            if ((senderBatch=senderMap.get(msg.getSrc())) == null) {
                senderBatch = new MessageBatch(null, msg.getSrc(), batch.clusterName(), true,
                        batch.getMode(), size);
                senderMap.put(msg.getSrc(), senderBatch);
            }
            senderBatch.add(msg);
        }

        for(MessageBatch senderBatch : senderMap.values()) {
            transport.submitBatch(senderBatch);
        }
        if (!batch.isEmpty())
            up_prot.up(batch);
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


    public static class DaisyHeader extends Header {
        private Address source;

        public DaisyHeader() {
        }

        public DaisyHeader(Address source) {
            this.source=source;
        }

        public short getMagicId()      {return 69;}
        public Address getSrc()          {return source;}
        public void  setSrc(Address source) {this.source=source;}

        public Supplier<? extends Header> create() {return DaisyHeader::new;}

        @Override public int  serializedSize() {return Util.size(source);}

        @Override public void writeTo(DataOutput out) throws IOException {
            if(source != null)
                Util.writeAddress(source, out);
        }
        @Override public void readFrom(DataInput in) throws IOException  {
            try {
                source=Util.readAddress(in);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        public String toString() {return "src=" + source;}
    }

}
