package fr.marouni.apache.beam.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.io.IOException;
import java.io.Serializable;

public class IODriver {

    public static void main(String[] args) throws IOException {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        p
                .apply(new RandomStringGeneratorIO.Read(20))
                .apply(MapElements.via(new MySimpleFunction()))
                .apply(MapElements.via(new MySimpleFunction2()))
                .apply(TextIO.write().to("/tmp/xxxxxxx"));

        p.run().waitUntilFinish();
    }

    public static class MySimpleFunction extends SimpleFunction<String, MyBean> {
        @Override
        public MyBean apply(String input) {
            MyInnerBean myInnerBean = new MyInnerBean(12, "INNER_BEAN", 10L);
            return new MyBean("BEAN", "BEAN", myInnerBean);
        }
    }

    public static class MySimpleFunction2 extends SimpleFunction<MyBean, String> {
        @Override
        public String apply(MyBean input) {
            return input.toString();
        }
    }

    public static class MyBean implements Serializable {
        String d;
        String e;
        MyInnerBean f;

        public MyBean(String d, String e, MyInnerBean f) {
            this.d = d;
            this.e = e;
            this.f = f;
        }

        public String getD() {
            return d;
        }

        public void setD(String d) {
            this.d = d;
        }

        public String getE() {
            return e;
        }

        public void setE(String e) {
            this.e = e;
        }

        public MyInnerBean getF() {
            return f;
        }

        public void setF(MyInnerBean f) {
            this.f = f;
        }
    }

    public static class MyInnerBean {
        int a;
        String b;
        long c;

        public MyInnerBean(int a, String b, long c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        public void setA(int a) {
            this.a = a;
        }

        public void setB(String b) {
            this.b = b;
        }

        public void setC(long c) {
            this.c = c;
        }

        public int getA() {
            return a;
        }

        public String getB() {
            return b;
        }

        public long getC() {
            return c;
        }
    }

    public static class MySubBean extends MyBean {
        String a;

        public MySubBean(String d, String e, MyInnerBean f, String a) {
            super(d, e, f);
            this.a = a;
        }

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }
    }
}
