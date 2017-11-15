package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobControlTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		ControlledJob userItemJob = UserItemTest.getUserItemJob(conf);
		ControlledJob historyRemoveJob = HistoryRemoveTest.getHistoryRemoveJob(conf);
		ControlledJob itemCooreJob = ItemCooreTest.getItemCooreJob(conf);
		ControlledJob itemPreferJob = ItemPreferListTest.getItemPreferJob(conf);
		ControlledJob matrixMulJob = MatrixMulListTest.getMatrixMulJob(conf);
		ControlledJob matrixAddJob = MatrixAddListTest.getMatrixAddJob(conf);
		ControlledJob referenceSortJob = ReferenceSortTest.getReferenceSortJob(conf);
		ControlledJob userPreferJob = UserPreferListTest.getUserPreferJob(conf);
		
		itemCooreJob.addDependingJob(userItemJob);
		itemPreferJob.addDependingJob(itemCooreJob);
		matrixMulJob.addDependingJob(itemPreferJob);
		matrixMulJob.addDependingJob(userPreferJob);
		matrixAddJob.addDependingJob(matrixMulJob);
		historyRemoveJob.addDependingJob(matrixAddJob);
		referenceSortJob.addDependingJob(historyRemoveJob);
		
		JobControl jobControl = new JobControl("jobcontrol!");
		jobControl.addJob(userPreferJob);
		jobControl.addJob(referenceSortJob);
		jobControl.addJob(matrixAddJob);
		jobControl.addJob(matrixMulJob);
		jobControl.addJob(itemPreferJob);
		jobControl.addJob(itemCooreJob);
		jobControl.addJob(historyRemoveJob);
		jobControl.addJob(userItemJob);
		
		Thread thread = new Thread(jobControl);
		thread.start();
		
		while(true) {
			if (!jobControl.allFinished()) {
				for (ControlledJob j: jobControl.getRunningJobList()) {
					System.out.println(j.getJobName()+ "-------------" + j.getMessage());
				}
			} else {
				break;
			}
		}
		return 0;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new JobControlTest(), args));
	}

}
