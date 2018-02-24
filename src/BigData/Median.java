package BigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output   .TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// TODO - Unique bucket
public class Median {
    public static class MedianMap extends Mapper<LongWritable, Text, PairWritable, LongWritable> {
        int count=0;
        long [] bucket;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           System.out.println("Here");
           long num = Long.parseLong(line);
            long leftLimit = 0l;
            long rightLimit = 20l;
            int bucketCount =1000;
           if(count==0){

             bucket = new long [bucketCount];
          //   Random rand = new Random();
           /*  for(int i=0;i<bucketCount;i++){
              //   bucket[i]= rand.nextLong(20l);
              bucket[i] = leftLimit + (long) (Math.random() * (rightLimit - leftLimit));
                 System.out.println("Before Sort");
                 System.out.println("Index :"+i+"Bucket Content "+bucket[i]);
             }*/
            LinkedList<Long> list = new LinkedList();
            for(Long i=0L;i<1000000L;i++){
                list.add(i);
            }

            Collections.shuffle(list);
            for(int i=0; i<bucketCount;i++){
                bucket[i]=list.remove();
                System.out.println("Before Sort");
                System.out.println("Index :"+i+"Bucket Content "+bucket[i]);
            }

            Arrays.sort(bucket);

               for(int i=0;i<bucketCount;i++) {
                   System.out.println("After Sort");
                   System.out.println("Index :"+i+"Bucket Content "+bucket[i]);
               }

             count++;
           }
           int index = binarysearch(bucket,num,0,bucket.length-1);
            PairWritable pair = new PairWritable();
            pair.set(index,bucketCount);

           System.out.println("Bucket no "+index+"Number "+num);
            LongWritable number = new LongWritable(num);
           context.write(pair, number);
           }

       public static int binarysearch(long [] arr, long num, int start, int end){
            if(start+1==end ) {
             if(arr[start]==num)   return start;
             else {
                 if(num<arr[start]) return start;
                 return end;}
             }

            int mid = (start + end)/2;
            if(arr[mid]==num) return mid;
            else if(arr[mid]>num) return binarysearch(arr,num,start,mid);
             else return binarysearch(arr,num,mid,end);


       }
    }

    public static class MedianReduce extends Reducer<PairWritable,LongWritable,Text,Text> {
        int count=1;
        Deque<ArrayList<Long>> deqBucket = new ArrayDeque<>();
        long min=Long.MAX_VALUE;
        long max=0;
        int elementCnt =0;
        int del=0;
        @Override
        public void reduce(PairWritable key, Iterable<LongWritable> list, Context context) throws IOException, InterruptedException {
            System.out.println("count is :"+count);
            int bucketCount=key.getNum2();
            ArrayList<Long> arrlist = new ArrayList<>();
            System.out.println();
            if(count==1){

            for (LongWritable val:list
                 )
            {
                    long temp = val.get();

                    System.out.print(temp+",");
                    if(min>temp)
                        min=temp;
                    arrlist.add(temp);
                    elementCnt++;
                }

                System.out.println("Minimum is: "+min);
                deqBucket.add(arrlist);
                count++;
                System.out.println("count is :"+count);
            }


            else if(count==bucketCount){
                for (LongWritable val:list
                        )
                {
                    long temp = val.get();
                    System.out.print(temp+",");
                    if(max<temp)
                        max=temp;
                    arrlist.add(temp);
                    elementCnt++;
                }
                System.out.println("Maximum is: "+max);
                deqBucket.add(arrlist);
                int mid = elementCnt%2==0?elementCnt/2:((elementCnt/2)+1);
                System.out.println("Mid outside While "+mid);
                while((deqBucket.peekFirst().size() + del )<mid){
                    //   elementCnt -= deqBucket.peekFirst().size();
                    //   mid = mid - deqBucket.peekFirst().size();
                    //   mid = elementCnt%2==0?elementCnt/2:((elementCnt/2)+1);
                    del += deqBucket.peekFirst().size();
                    // System.out.println("Mid inside While "+mid);
                    deqBucket.removeFirst();
                }
                System.out.println("Below WHile");
                Object []  arrTemp = deqBucket.peekFirst().toArray();
                Long [] arr = Arrays.copyOf( arrTemp , arrTemp.length, Long[].class );
                Arrays.sort(arr);
                long median = arr[mid-1-del];
                System.out.println("Median is:"+median);
                String str = min + "\t" + max +"\t" + median;
                Text result = new Text(str);
                System.out.println("Final Result ="+ str);
                Text nULL = new Text("");
                context.write(nULL,result);
              }

              else{
                for (LongWritable val:list
                        )
                {
                    long temp = val.get();
                    System.out.print(temp+",");
                    arrlist.add(temp);
                    elementCnt++;
                }
                deqBucket.add(arrlist);
                int mid = elementCnt%2==0?elementCnt/2:((elementCnt/2)+1);
                System.out.println("Mid outside While "+mid);
                while((deqBucket.peekFirst().size() + del )<mid){
                 //   elementCnt -= deqBucket.peekFirst().size();
                 //   mid = mid - deqBucket.peekFirst().size();
                 //   mid = elementCnt%2==0?elementCnt/2:((elementCnt/2)+1);
                    del += deqBucket.peekFirst().size();
                   // System.out.println("Mid inside While "+mid);
                       deqBucket.removeFirst();
                }
                count++;
                System.out.println("count is :"+count);
              }

            }
        }


        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: CountYelpReview <in> <out>");
            System.exit(2);
        }


        Job job = new Job(conf, "Median");
        job.setJarByClass(Median.class);

        job.setMapperClass(MedianMap.class);
        job.setReducerClass(MedianReduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set output value type
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

       // job.setSortComparatorClass(LongComparator.class);


        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
