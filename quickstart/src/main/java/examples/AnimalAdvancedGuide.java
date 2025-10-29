package examples;

import customdatatypes.Animal;
import customdatatypes.AnimalBasket;
import customdatatypes.AnimalDataStream;
import customoperators.*;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
import shared.coordinators.ContinuousProgramImpl;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import shared.querying.TaskImpl;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import shared.contentimpl.factories.AccumulatorContentFactory;
import shared.operatorsimpl.r2r.DAG.DAGImpl;
import shared.sds.SDSDefault;
import stream.AnimalStreamGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AnimalAdvancedGuide {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        AnimalStreamGenerator generator = new AnimalStreamGenerator();

        // Define the two input streams
        DataStream<Animal> inputStreamAnimal_one = generator.getStream("animal_market_one");
        DataStream<Animal> inputStreamAnimal_two = generator.getStream("animal_market_two");

        // define an output stream
        DataStream<Animal> outStream = new AnimalDataStream("animal_consumer");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        AnimalBasket emptyBasket = new AnimalBasket();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<Animal, Animal, AnimalBasket> filterContentFactory = new CustomFilterContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket,
                (animal)->animal.getWeight()>2
        );

        ContentFactory<Animal, Animal, AnimalBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    if(basket_1.getSize()>basket_2.getSize()){
                        basket_1.addAll(basket_2);
                        return basket_1;
                    }
                    else{
                        basket_2.addAll(basket_1);
                        return basket_2;
                    }
                },
                emptyBasket
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operators (blueprint of the windows)
        StreamToRelationOperator<Animal, Animal, AnimalBasket> animal_s2r_one =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_one",
                        accumulatorContentFactory,
                        report,
                        1000);

        StreamToRelationOperator<Animal, Animal, AnimalBasket> animal_s2r_two =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_two",
                        filterContentFactory,
                        report,
                        1000);

        //Define Relation to Relation operators and chain them together
        RelationToRelationOperator<AnimalBasket> r2r_filter_underripe = new FilterAnimalByStatusOp("sick", Collections.singletonList(animal_s2r_one.getName()), "filtered_animal");
        RelationToRelationOperator<AnimalBasket> r2r_join = new JoinAnimalBasketOp(List.of("filtered_animal", animal_s2r_two.getName()), "joined_animal");

        //Relation to Stream operator, take the final animal basket and send out each animal
        RelationToStreamOperator<AnimalBasket, Animal> r2sOp = new RelationToStreamAnimalOp();


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<Animal, Animal, AnimalBasket, Animal> task = new TaskImpl<>();
        task = task.addS2ROperator(animal_s2r_one, inputStreamAnimal_one)
                .addS2ROperator(animal_s2r_two, inputStreamAnimal_two)
                .addR2ROperator(r2r_filter_underripe)
                .addR2ROperator(r2r_join)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSDefault<>())
                .addTime(instance);
        task.initialize();




        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgram<Animal, Animal, AnimalBasket, Animal> cp = new ContinuousProgramImpl<>();

        List<DataStream<Animal>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamAnimal_one);
        inputStreams.add(inputStreamAnimal_two);

        List<DataStream<Animal>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);


        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }

}
