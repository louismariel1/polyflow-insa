package customdatatypes;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class AnimalDataStream implements DataStream<Animal> {

    List<Consumer<Animal>> consumerList = new ArrayList<>();
    String name;

    public AnimalDataStream(String name){
        this.name = name;
    }

    @Override
    public void addConsumer(Consumer<Animal> windowAssigner) {
        this.consumerList.add(windowAssigner);
    }

    @Override
    public void put(Animal animal, long ts) {
        consumerList.forEach(c->c.notify(this, animal, ts));
    }

    @Override
    public String getName() {
        return name;
    }
}

