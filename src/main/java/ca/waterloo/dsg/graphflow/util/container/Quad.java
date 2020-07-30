package ca.waterloo.dsg.graphflow.util.container;

import java.io.Serializable;

/**
 * A mutable Quad (A a, B b, C c, D d).
 */
public class Quad <A, B, C, D> implements Serializable {

    public A a;
    public B b;
    public C c;
    public D d;

    public Quad(A a, B b, C c, D d) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }
}
