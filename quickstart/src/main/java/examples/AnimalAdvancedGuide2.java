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

public class AnimalAdvancedGuide2 {

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        AnimalStreamGenerator generator = new AnimalStreamGenerator();

        // Existing input streams
        DataStream<Animal> inputStreamAnimal_one = generator.getStream("animal_market_one");
        DataStream<Animal> inputStreamAnimal_two = generator.getStream("animal_market_two");

        // NEW: Additional input streams
        DataStream<Animal> inputStreamAnimal_three = generator.getStream("animal_market_three");
        DataStream<Animal> inputStreamAnimal_four = generator.getStream("animal_market_four");

        // Existing output stream
        DataStream<Animal> outStream1 = new AnimalDataStream("animal_consumer_1");

        // NEW: Additional output stream
        DataStream<Animal> outStream2 = new AnimalDataStream("animal_consumer_2");

        /*------------Window Content Factories------------*/

        AnimalBasket emptyBasket = new AnimalBasket();

        ContentFactory<Animal, Animal, AnimalBasket> filterContentFactory = new CustomFilterContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    basket_1.addAll(basket_2);
                    return basket_1;
                },
                emptyBasket,
                (animal) -> animal.getWeight() > 50
        );

        ContentFactory<Animal, Animal, AnimalBasket> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    basket_1.addAll(basket_2);
                    return basket_1;
                },
                emptyBasket
        );

        // NEW: Two additional window content factories
        ContentFactory<Animal, Animal, AnimalBasket> lightWeightFilterFactory = new CustomFilterContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    basket_1.addAll(basket_2);
                    return basket_1;
                },
                emptyBasket,
                (animal) -> animal.getWeight() <= 50
        );

        ContentFactory<Animal, Animal, AnimalBasket> speciesFilterFactory = new CustomFilterContentFactory<>(
                (animal) -> animal,
                (animal) -> {
                    AnimalBasket fb = new AnimalBasket();
                    fb.addAnimal(animal);
                    return fb;
                },
                (basket_1, basket_2) -> {
                    basket_1.addAll(basket_2);
                    return basket_1;
                },
                emptyBasket,
                (animal) -> animal.getSpecie().equalsIgnoreCase("dog")
        );


        /*------------Window Properties------------*/

        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Time instance = new TimeImpl(0);


        /*------------S2R Operators (Windows)------------*/

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

        // NEW: Two additional S2R windows
        StreamToRelationOperator<Animal, Animal, AnimalBasket> animal_s2r_three =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_three",
                        lightWeightFilterFactory,
                        report,
                        1000);

        StreamToRelationOperator<Animal, Animal, AnimalBasket> animal_s2r_four =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow_four",
                        speciesFilterFactory,
                        report,
                        1000);


        /*------------R2R and R2S Operators------------*/

        RelationToRelationOperator<AnimalBasket> r2r_filter_sick =
                new FilterAnimalByStatusOp("sick", Collections.singletonList(animal_s2r_one.getName()), "filtered_animal");

        RelationToRelationOperator<AnimalBasket> r2r_join_main =
                new JoinAnimalBasketOp(List.of("filtered_animal", animal_s2r_two.getName()), "joined_main");

        // NEW: Another join for the additional two streams
        RelationToRelationOperator<AnimalBasket> r2r_join_secondary =
                new JoinAnimalBasketOp(List.of(animal_s2r_three.getName(), animal_s2r_four.getName()), "joined_secondary");

        RelationToStreamOperator<AnimalBasket, Animal> r2sOp1 = new RelationToStreamAnimalOp();
        RelationToStreamOperator<AnimalBasket, Animal> r2sOp2 = new RelationToStreamAnimalOp();


        /*------------Task definition------------*/

        Task<Animal, Animal, AnimalBasket, Animal> task = new TaskImpl<>();

        task = task
                // Input streams 1 & 2
                .addS2ROperator(animal_s2r_one, inputStreamAnimal_one)
                .addS2ROperator(animal_s2r_two, inputStreamAnimal_two)
                // Input streams 3 & 4 (NEW)
                .addS2ROperator(animal_s2r_three, inputStreamAnimal_three)
                .addS2ROperator(animal_s2r_four, inputStreamAnimal_four)
                // R2R operators
                .addR2ROperator(r2r_filter_sick)
                .addR2ROperator(r2r_join_main)
                .addR2ROperator(r2r_join_secondary)
                // Two R2S operators (output)
                .addR2SOperator(r2sOp1)
                .addR2SOperator(r2sOp2)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSDefault<>())
                .addTime(instance);

        task.initialize();


        /*------------Continuous Program definition------------*/

        ContinuousProgram<Animal, Animal, AnimalBasket, Animal> cp = new ContinuousProgramImpl<>();

        List<DataStream<Animal>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamAnimal_one);
        inputStreams.add(inputStreamAnimal_two);
        inputStreams.add(inputStreamAnimal_three);
        inputStreams.add(inputStreamAnimal_four);

        List<DataStream<Animal>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream1);
        outputStreams.add(outStream2);

        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumers------------*/

        outStream1.addConsumer((out, el, ts) ->
                System.out.println("Output Stream 1: [" + el + "] @ " + ts));

        outStream2.addConsumer((out, el, ts) ->
                System.out.println("Output Stream 2: [" + el + "] @ " + ts));


        /*------------Run------------*/

        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }
}
