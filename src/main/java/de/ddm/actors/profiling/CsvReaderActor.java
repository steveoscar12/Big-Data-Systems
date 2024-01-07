package de.ddm.actors.profiling;

package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.configuration.InputConfiguration;
import de.ddm.messages.HeaderMessage;
import de.ddm.messages.BatchMessage;
import java.io.File;
import java.io.IOException;



package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import de.ddm.configuration.InputConfiguration;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyMiner.BatchMessage;
import de.ddm.actors.profiling.DependencyMiner.HeaderMessage;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;

public class CSVInputReader extends AbstractBehavior<String> {
    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadHeaderMessage implements InputReader.Message {
        private static final long serialVersionUID = 1729062814525657711L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReadBatchMessage implements InputReader.Message {
        private static final long serialVersionUID = -7915854043207237318L;
        ActorRef<DependencyMiner.Message> replyTo;
    }

    private final ActorRef<DependencyMiner.Message> dependencyMiner;

    public static Behavior<String> create(ActorRef<DependencyMiner.Message> dependencyMiner) {
        return Behaviors.setup(context -> new CSVInputReader(context, dependencyMiner));
    }

    private CSVInputReader(akka.actor.typed.javadsl.ActorContext<String> context, ActorRef<DependencyMiner.Message> dependencyMiner) {
        super(context);
        this.dependencyMiner = dependencyMiner;
    }

    @Override
    public Receive<String> createReceive() {
        return newReceiveBuilder().onMessage(String.class, this::handleStartMessage).build();
    }

    private Behavior<String> handleStartMessage(String filePath) {
        // Assuming filePath is the path to the CSV file
        File inputFile = new File(filePath);

        try {
            InputConfiguration config = new InputConfiguration();
            config.update(/* your command master */); // Update with proper CommandMaster

            String[] header = config.getHeader(inputFile);
            dependencyMiner.tell(new HeaderMessage(1, header)); // Change 1 with appropriate id

            CSVReader reader = config.createCSVReader(inputFile);
            String[] line;
            while ((line = reader.readNext()) != null) {
                dependencyMiner.tell(new BatchMessage(1, line)); // Change 1 with appropriate id
            }
            reader.close();
        } catch (Exception e) {
            // Handle exceptions appropriately
            e.printStackTrace();
        }

        return this;
    }
}

