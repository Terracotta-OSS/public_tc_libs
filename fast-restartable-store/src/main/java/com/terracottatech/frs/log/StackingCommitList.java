/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mscott
 */
public class StackingCommitList implements CommitList {

    private final LogRecord[] regions;
// set at construction    
    private long baseLsn;
//  half synchronized
    private volatile boolean syncing = false;
//  these are synchronized     
    private long endLsn;
//    private long lowestLsn;
    private boolean closed = false;
    private boolean written = false;
    private Exception error;
    private int count = 0;
    
    private final Object guard = new Object();
    private volatile CommitList next;
    private int wait;

    public StackingCommitList(long startLsn, int maxSize, int wait) {
        baseLsn = startLsn;
        endLsn = startLsn-1;
        regions = new LogRecord[maxSize];
        this.wait = wait;
    }

     @Override
   public boolean append(LogRecord record, boolean sync) {
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length + baseLsn) {
            return false;
        }

        regions[(int) (record.getLsn() - baseLsn)] = record;

        if (!countRecord(record.getLsn(),sync)) {
            regions[(int) (record.getLsn() - baseLsn)] = null; //  just to be clean;
            return false;
        }
        
        return true;
    }
 
    @Override
    public CommitList next() {
        if (next == null) {
            synchronized (this) {
                if ( !closed ) {
                    closed = true;
                }
                if (next == null) {
                    next = create(endLsn + 1);
                }
            }
        }
        return next;
    }
    
    public CommitList create(long nextLsn) {
        return new StackingCommitList( nextLsn, regions.length, wait);
    }

    @Override
    public boolean isEmpty() {
        return ( endLsn < baseLsn );
    }
    
    @Override
    public long getBaseLsn() {
        return baseLsn;
    }

    @Override
    public long getEndLsn() {
        return endLsn;
    }
    //  TODO:  make more concurrent
    private synchronized boolean countRecord(long lsn, boolean sync) {
        if (closed) {
            if (lsn > endLsn) {
                return false;
            }
        } else if (lsn > endLsn) {
            endLsn = lsn;
        }
        if (count++ == endLsn - baseLsn) {
            this.notify();  // adding one will make count match slots
        }
        if ( sync ) {
            this.syncing = true;
            this.notify();
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean close(long lsn) {
        if ( lsn <= endLsn ) {
            closed = true;
            this.notify();
        }

        return closed;
    }

    @Override
    public boolean isSyncRequested() {
        return syncing;
    }

    private void waitForWrite() throws InterruptedException, ExecutionException {
        waitForWrite(0);
    }

    private void waitForWrite(long millis) throws InterruptedException, ExecutionException {
        long span = System.currentTimeMillis();
        synchronized (guard) {
            while (!this.written) {
                if ( error != null ) throw new ExecutionException(error);
                if (millis != 0 && System.currentTimeMillis() - span > millis) {
                    return;
                }
                guard.wait(millis);
            }
        }
    }

    private boolean isWritten() {
        synchronized (guard) {
            return written;
        }
    }

    @Override
    public void written() {
        synchronized (guard) {
            written = true;
            guard.notifyAll();
        }
    }
    
    
    @Override
    public void exceptionThrown(Exception exp) {
      CommitList chain = null;
        synchronized (guard) {
            error = exp;
            guard.notifyAll();
            if ( next != null ) {
              chain = next;
            }
        }
        if ( chain != null ) {
          chain.exceptionThrown(exp);
        }
    }

    @Override
    public synchronized void waitForContiguous() throws InterruptedException {
        boolean timedout = false;
        if ( count > 0 && !closed ) {
            this.close(baseLsn + count - 1);
        }
        
        while ((!closed && count != regions.length) || (closed && count != endLsn - baseLsn + 1)) {
            this.wait(wait);
            if ( timedout ) {
                if ( count > 0 ) {
                    this.close(baseLsn + count - 1);
                    timedout = false;
                }
            } else {
                timedout = true;
            }
            
        }
        if (count != regions.length && count != endLsn - baseLsn + 1) {
            throw new AssertionError();
        }
    }
    
 //  Future interface

    @Override
    public boolean cancel(boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    private synchronized void checkForClose() {
        if ( !closed ) {
            closed = true;
            this.notify();
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        checkForClose();
        this.waitForWrite();
        return null;
    }

    @Override
    public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
        checkForClose();
        this.waitForWrite(tu.convert(l, TimeUnit.MILLISECONDS));
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.isWritten();
    } 
    
    
//  iterator interface
    @Override
    public Iterator<LogRecord> iterator() {
        return new Iterator<LogRecord>() {
            int current = 0;
            @Override
            public boolean hasNext() {
                return ( current < count );
            }

            @Override
            public LogRecord next() {
                return regions[current++];
            }

            @Override
            public void remove() {

            }
        };
    }
    }
