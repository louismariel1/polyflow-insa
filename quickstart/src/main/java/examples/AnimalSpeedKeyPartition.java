package relational.examples;

import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.AggregateContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.factories.ContainerContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.ParallelContinuousProgram;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;

import static tech.tablesaw.aggregate.AggregateFunctions.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class polyflow_animalSpeed_partition_more_animals {

    public static void main(String[] args) throws InterruptedException {

        // === Simulated animal movement stream ===
        RowStreamGenerator generator = new RowStreamGenerator() {
            @Override
            public void startStreaming() {
                Random rand = new Random();
                long start = System.currentTimeMillis();

                // ✅ Updated species list
                String[] species = { "lion", "zebra", "gazelle", "elephant", "dog", "cat" };

                for (int i = 0; i < 100; i++) {
                    // ✅ Fixed ID-species mapping (1 → lion, 2 → zebra, etc.)
                    long animalId = (rand.nextInt(species.length) + 1); // 1..6
                    String sp = species[(int) (animalId - 1)];

                    double speed = 5.0 + rand.nextDouble() * 20.0; // 5–25 m/s
                    long timestamp = System.currentTimeMillis() - start;

                    // Tuple: (animalId, species, speed, timestamp)
                    Tuple tuple = new org.javatuples.Quartet<>(animalId, sp, speed, timestamp);
                    System.out.printf("Sending %s (ID=%d) in partition %d%n", sp, animalId, animalId % 3);
                    getStream("http://wildlife/animals").put(tuple, timestamp);

                    try {
                        Thread.sleep(200); // simulate delay
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };

        DataStream<Tuple> inputStream = generator.getStream("http://wildlife/animals");
        RowStream<Tuple> outStream = new RowStream<>("out");

        // === Window reporting & timing ===
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;

        Time instance_1 = new TimeImpl(0);
        Time instance_2 = new TimeImpl(0);
        Time instance_3 = new TimeImpl(0);

        Table empty = Table.create("empty");

        // === Aggregate factory: converts Tuple → Table and aggregates ===
        AggregateContentFactory<Tuple, Tuple, Table> aggFactory = new AggregateContentFactory<>(
                t -> t,
                (t) -> {
                    Table table = Table.create("animal_data");
                    LongColumn id = LongColumn.create("animalId");
                    StringColumn species = StringColumn.create("species");
                    DoubleColumn speed = DoubleColumn.create("speed");
                    table.addColumns(id, species, speed);

                    Long longVal = null;
                    String strVal = null;
                    Double dblVal = null;

                    for (int i = 0; i < t.getSize(); i++) {
                        Object val = t.getValue(i);
                        if (val instanceof Long && longVal == null)
                            longVal = (Long) val;
                        else if (val instanceof Integer && longVal == null)
                            longVal = ((Integer) val).longValue();
                        else if (val instanceof String && strVal == null)
                            strVal = (String) val;
                        else if (val instanceof Double && dblVal == null)
                            dblVal = (Double) val;
                        else if (val instanceof Float && dblVal == null)
                            dblVal = ((Float) val).doubleValue();
                    }

                    if (longVal != null || strVal != null || dblVal != null) {
                        id.append(longVal != null ? longVal : -1L);
                        species.append(strVal != null ? strVal : "unknown");
                        speed.append(dblVal != null ? dblVal : Double.NaN);
                    }

                    return table;
                },
                (table1, table2) -> table1.isEmpty() ? table2 : table1.append(table2),
                (table) -> {
                    if (table.rowCount() == 0)
                        return Table.create("empty");
                    // ✅ Aggregate by species (mean, min, max of speed)
                    return table.summarize("speed", mean, min, max).by("species");
                },
                empty);

        // === Container factory (partition key: animalId) ===
        ContainerContentFactory<Tuple, Tuple, Table, Long> containerFactory = new ContainerContentFactory<>(
                i -> {
                    Object val = i.getValue(0);
                    if (val instanceof Long)
                        return (Long) val;
                    if (val instanceof Integer)
                        return ((Integer) val).longValue();
                    return 0L;
                },
                w -> null,
                r -> null,
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                Table.create(),
                aggFactory);

        // === Define hopping windows ===
        StreamToRelationOperator<Tuple, Tuple, Table> s2r1 = new HoppingWindowOpImpl<>(tick, instance_1, "win1",
                containerFactory, report, 5000, 2000);
        StreamToRelationOperator<Tuple, Tuple, Table> s2r2 = new HoppingWindowOpImpl<>(tick, instance_2, "win2",
                containerFactory, report, 5000, 2000);
        StreamToRelationOperator<Tuple, Tuple, Table> s2r3 = new HoppingWindowOpImpl<>(tick, instance_3, "win3",
                containerFactory, report, 5000, 2000);

        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();

        // === Define tasks for 3 partitions ===
        Task<Tuple, Tuple, Table, Tuple> task1 = new TaskImpl<>("Task1");
        task1 = task1.addS2ROperator(s2r1, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_1);
        task1.initialize();

        Task<Tuple, Tuple, Table, Tuple> task2 = new TaskImpl<>("Task2");
        task2 = task2.addS2ROperator(s2r2, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_2);
        task2.initialize();

        Task<Tuple, Tuple, Table, Tuple> task3 = new TaskImpl<>("Task3");
        task3 = task3.addS2ROperator(s2r3, inputStream)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_3);
        task3.initialize();

        // === Continuous program ===
        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ParallelContinuousProgram<>(i -> {
            Object val = i.getValue(0);
            if (val instanceof Long)
                return (Long) val;
            if (val instanceof Integer)
                return ((Integer) val).longValue();
            return 0L;
        }, 3);

        List<DataStream<Tuple>> inputs = List.of(inputStream);
        List<DataStream<Tuple>> outputs = List.of(outStream);

        cp.buildTask(task1, inputs, outputs);
        cp.buildTask(task2, inputs, outputs);
        cp.buildTask(task3, inputs, outputs);

        // === Windowed output ===
        Map<Long, List<Tuple>> windowBuffer = new ConcurrentHashMap<>();

        outStream.addConsumer((DataStream<Tuple> stream, Tuple el, long ts) -> {
            if (el != null) {
                windowBuffer.computeIfAbsent(ts, k -> new ArrayList<>()).add(el);
                System.out.printf("===== 🪟 Window closed at t=%d =====%n", ts);
                for (Tuple t : windowBuffer.get(ts)) {
                    System.out.println("Summary tuple: " + t);
                }
                windowBuffer.remove(ts);
            } else {
                System.out.printf("Non-tuple element at t=%.2f s%n", ts);
            }
        });

        // === Start streaming ===
        generator.startStreaming();
    }
}
