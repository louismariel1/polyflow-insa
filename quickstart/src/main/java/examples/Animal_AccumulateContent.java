package relational.examples;

import org.javatuples.Quartet;
import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.operatorsimpl.r2r.CustomRelationalQuery;
import relational.operatorsimpl.r2r.R2RjtablesawSelection;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;

import java.util.*;

public class Animal_AccumulateContent {

    public static void main(String[] args) throws InterruptedException {

        // --- Create a row stream generator ---
        RowStreamGenerator generator = new RowStreamGenerator() {
            private volatile boolean running = false;

            @Override
            public void startStreaming() {
                running = true;
                Random rand = new Random();
                long start = System.currentTimeMillis();

                String[] species = { "lion", "zebra", "gazelle", "elephant", "dog", "cat" };
                long animalId = 1;

                while (running) {
                    String sp = species[(int) (animalId - 1)];
                    int speed = (int) Math.round(5.0 + rand.nextDouble() * 20.0); // cast to int to fix Tablesaw R2R
                    long timestamp = System.currentTimeMillis() - start;

                    Tuple tuple = new Quartet<>(animalId, sp, speed, timestamp);
                    System.out.printf("Sending %s (ID=%d) in partition %d%n", sp, animalId, animalId % 3);
                    getStream("http://wildlife/animals").put(tuple, timestamp);

                    animalId = (animalId % species.length) + 1;

                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                System.out.println("🐾 Stream generator stopped.");
            }

            @Override
            public void stopStreaming() {
                running = false;
            }
        };

        // --- Setup input/output streams ---
        DataStream<Tuple> inputStream = generator.getStream("http://wildlife/animals");
        DataStream<Tuple> outStream = new RowStream("out");

        // --- Engine configuration ---
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;
        Time timeInstance = new TimeImpl(0);
        Table emptyContent = Table.create();

        // --- Accumulator content factory ---
        AccumulatorContentFactory<Tuple, Tuple, Table> accumulatorContentFactory = new AccumulatorContentFactory<>(
                t -> t,
                t -> {
                    Table r = Table.create();
                    for (int i = 0; i < t.getSize(); i++) {
                        Object value = t.getValue(i);
                        String columnName = "c" + (i + 1);

                        if (value instanceof Long) {
                            LongColumn lc = r.containsColumn(columnName) ? (LongColumn) r.column(columnName) : LongColumn.create(columnName);
                            lc.append((Long) value);
                            if (!r.containsColumn(columnName)) r.addColumns(lc);
                        } else if (value instanceof Integer) {
                            IntColumn ic = r.containsColumn(columnName) ? (IntColumn) r.column(columnName) : IntColumn.create(columnName);
                            ic.append((Integer) value);
                            if (!r.containsColumn(columnName)) r.addColumns(ic);
                        } else if (value instanceof String) {
                            StringColumn sc = r.containsColumn(columnName) ? (StringColumn) r.column(columnName) : StringColumn.create(columnName);
                            sc.append((String) value);
                            if (!r.containsColumn(columnName)) r.addColumns(sc);
                        } else if (value instanceof Boolean) {
                            BooleanColumn bc = r.containsColumn(columnName) ? (BooleanColumn) r.column(columnName) : BooleanColumn.create(columnName);
                            bc.append((Boolean) value);
                            if (!r.containsColumn(columnName)) r.addColumns(bc);
                        }
                    }
                    return r;
                },
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                emptyContent
        );

        // --- Continuous program ---
        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgramImpl<>();

        StreamToRelationOperator<Tuple, Tuple, Table> s2rOp =
                new HoppingWindowOpImpl<>(tick, timeInstance, "w1", accumulatorContentFactory, report, 1000, 1000);

        CustomRelationalQuery selectionQuery = new CustomRelationalQuery(4, "c3"); // c3 is speed column
        RelationToRelationOperator<Table> r2rOp = new R2RjtablesawSelection(selectionQuery, Collections.singletonList(s2rOp.getName()), "partial_1");

        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();

        // --- Task ---
        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>("1");
        task = task.addS2ROperator(s2rOp, inputStream)
                .addR2ROperator(r2rOp)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(timeInstance);

        task.initialize();

        cp.buildTask(task, Collections.singletonList(inputStream), Collections.singletonList(outStream));

        outStream.addConsumer((stream, element, ts) -> System.out.println(element + " @ " + ts));

        // --- Start streaming for 10 seconds ---
        Thread generatorThread = new Thread(generator::startStreaming);
        generatorThread.start();

        Thread.sleep(10000); // stream for 10 seconds
        generator.stopStreaming();
        generatorThread.join();

        System.out.println("✅ Streaming stopped after 10 seconds.");
    }
}
