package ca.waterloo.dsg.graphflow.util.container;

import java.io.Serializable;

/**
 * A mutable Triple (A a, B b, C c).
 */
public class Triple <A, B, C> implements Serializable {

    public A a;
    public B b;
    public C c;

    public Triple(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }
}
