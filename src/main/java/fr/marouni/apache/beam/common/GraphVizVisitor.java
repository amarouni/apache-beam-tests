package fr.marouni.apache.beam.common;

import guru.nidi.graphviz.attribute.RankDir;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.Node;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.node;
import static guru.nidi.graphviz.model.Factory.to;

public class GraphVizVisitor extends Pipeline.PipelineVisitor.Defaults {

    private Graph graph;
    private Map<String, Node> nodesMap;
    private String graphOutputPath;

    public GraphVizVisitor(Pipeline pipeline, String graphOutputPath){
        nodesMap = new LinkedHashMap<>();
        graph = graph("example1").directed()
                .graphAttr().with(RankDir.LEFT_TO_RIGHT);
        this.graphOutputPath = graphOutputPath;
        pipeline.traverseTopologically(this);
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {}

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {
        String transformName = node.getFullName().split("/")[0];
        nodesMap.put(transformName, node(transformName));
    }

    public void writeGraph() throws IOException {
        Iterator<Node> iterator = nodesMap.values().iterator();

        Node source = null;
        if(iterator.hasNext()) {
            source = iterator.next();
        }

        while (iterator.hasNext()){
            Node target = iterator.next();
            graph = graph
                    .with(source.link(to(target)));
            source = target;

        }
        Graphviz.fromGraph(graph).render(Format.PNG).toFile(new File(graphOutputPath));
    }
}
