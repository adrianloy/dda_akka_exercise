package de.hpi.akka_tutorial;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import de.hpi.akka_tutorial.Participant;
import de.hpi.akka_tutorial.remote.PWCalculator;
import de.hpi.akka_tutorial.remote.actors.scheduling.PWReactiveSchedulingStrategy;
import de.hpi.akka_tutorial.remote.actors.scheduling.SSReactiveSchedulingStrategy;

public class ExerciseMain {

	public static void main(String[] args) {
		// Read CSV file, path to is should be in args[0]. 
		// Then start a PWmaster with 4 local workers
		String csvFile = "";
		if (args.length > 0) {
			csvFile = args[0];
		} else {
			csvFile = "./students.csv";
			
		}
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<Participant> all_participants = new ArrayList<Participant>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] user = line.split(cvsSplitBy);
				// for (String s : user) {
				//System.out.println(user[1]);
				// }
				if (user.length == 4) {
					Participant p = new Participant(Integer.parseInt(user[0]), user[1], user[2], user[3]);
					all_participants.add(p);
				}
			}

		} catch (FileNotFoundException e) {
			System.out.println("Could not find students.csv file. I was looking for it at " + csvFile);
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		// Calculator.runMaster(masterCommand.host, masterCommand.port,
		// schedulingStrategyFactory, masterCommand.numLocalWorkers);
		System.out.println("Found " + all_participants.size() + " students in students.csv");
		PWCalculator.runMaster("localhost", 7877, new PWReactiveSchedulingStrategy.PWFactory(), new SSReactiveSchedulingStrategy.SSFactory(), 4, all_participants);

	}
}
