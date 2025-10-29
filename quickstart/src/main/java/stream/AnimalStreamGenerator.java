package stream;

import customdatatypes.*;

import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.lang.annotation.Documented;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class AnimalStreamGenerator {

    private enum animal {SNAKE, MONKEY, DOG, CAT, RABBIT, TURTLE, ANT};
    private String[] animalStatus = {"sick", "tired", "agile", "happy"};
    private String[] animalSpecie = {"land", "air", "sea","forest"};
    private final Map<String, DataStream<Animal>> activeStreams;
    private final long TIMEOUT = 1000l;
    private final Random randomGenerator;
    private boolean isStreaming = false;

    public AnimalStreamGenerator(){
        this.activeStreams = new HashMap<>();
        this.randomGenerator = new Random(1336);
    }

    public DataStream<Animal> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            AnimalDataStream stream = new AnimalDataStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming) {
            this.isStreaming = true;
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming) {
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += 400;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }

    public void stopStreaming() {
        this.isStreaming = false;
    }

    private void generateDataAndAddToStream(DataStream<Animal> stream, long ts) {

        switch (animal.values()[randomGenerator.nextInt(0, 7)]){
            case SNAKE:
                stream.put(new Snake(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)], animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case MONKEY:
                stream.put(new Monkey(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case DOG:
                stream.put(new Dog(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case CAT:
                stream.put(new Cat(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case RABBIT:
                stream.put(new Rabbit(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case TURTLE:
                stream.put(new Turtle(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;
            case ANT:
                stream.put(new Ant(randomGenerator.nextFloat(0, 100), animalSpecie[randomGenerator.nextInt(0, 4)],animalStatus[randomGenerator.nextInt(0, 4)]), ts);
                break;

        }

    }


}

