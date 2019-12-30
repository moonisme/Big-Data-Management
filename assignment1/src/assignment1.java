import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// this is the comment for this assignment.
// two mappers and two reducers are used to solve this movie rating problem.
// intermediary is the output path for the first mapreduce process, output is the output path for final result.

// the first mapper <UserMapper> read data from rating.dat and output in the format as follows:
// <UserMapper> (Object, Text, Text, movie_rating) 
// key: user value: a pair of movie & rating 
// movie_rating is a Writable class for movie and rating.

// the first reducer <UserReducer> reduce the output from the first mapper, the output format is as follows:
// <UserReducer> (Text, movie_rating, Text, Text) key: user value: all movie & rating pair made by this user.
// for example, user 1 movie 2 rating 10, user 1 movie 5 rating 4, the output is like 1 2,10 5,4

// the second mapper <MovieMapper> read data from the output of the first reducer
// <MovieMapper> (Object,Text,movie_movie,user_rating) 
// key: (movie1,movie2) value: user rating1 rating2
// movie_movie is a WritableComparable class for movie1 movie2 as key need to be comparable for avoiding redundant output.
// user_rating is a Writable class for user rating1 and rating2.

// the second reducer <MovieReducer> reduce the output from the second mapper, the output format is as follows:
// <MovieReducer> (movie_movie,user_rating,Text,array_user_rating) 
// key: (movie1,movie2) value: all (user,rating1,rating2) rating both of the movies as required in the specification.
// array_user_rating is an ArrayWritable class for output, as Text for complex object is not allowed in this assignment.


public class assignment1 {
	
	public static class movie_rating implements Writable {
		
		private Text movie;
		private IntWritable rating;
		
		public movie_rating() {
			this.movie = new Text();
			this.rating = new IntWritable();
		}
		
		public Text getMovie() {
			return movie;
		}
		public void setMovie(Text movie) {
			this.movie = movie;
		}
		public IntWritable getRating() {
			return rating;
		}
		public void setRating(IntWritable rating) {
			this.rating = rating;
		}
		public movie_rating(Text movie, IntWritable rating) {
			super();
			this.movie = movie;
			this.rating = rating;
		}
		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie.readFields(data);
			this.rating.readFields(data);
			
		}
		@Override
		public void write(DataOutput data) throws IOException {
			this.movie.write(data);
			this.rating.write(data);
			
		}
		
		public String toString() {
			return this.movie.toString() + "," + this.rating.toString();
		}
	}
	
	public static class movie_movie implements WritableComparable<movie_movie> {
		
		private Text movie1;
		private Text movie2;
		
		public movie_movie() {
			this.movie1 = new Text();
			this.movie2 = new Text();
		}
		
		public movie_movie(Text movie1, Text movie2) {
			super();
			this.movie1 = movie1;
			this.movie2 = movie2;
		}
		
		
		public Text getMovie1() {
			return movie1;
		}


		public void setMovie1(Text movie1) {
			this.movie1 = movie1;
		}


		public Text getMovie2() {
			return movie2;
		}


		public void setMovie2(Text movie2) {
			this.movie2 = movie2;
		}


		@Override
		public void readFields(DataInput data) throws IOException {
			this.movie1.readFields(data);
			this.movie2.readFields(data);
			
		}
		@Override
		public void write(DataOutput data) throws IOException {
			this.movie1.write(data);
			this.movie2.write(data);
			
		}
		@Override
		public int compareTo(movie_movie o) {
			if(this.movie1.compareTo(o.getMovie1()) == 0)
				return this.movie2.compareTo(o.getMovie2());
			else
				return this.movie1.compareTo(o.getMovie1());
		}
		
		public String toString() {
			return "("+ this.movie1.toString() + "," + this.movie2.toString() + ")";
		}
		
	}
	
	public static class user_rating implements Writable {
		
		private Text user;
		private IntWritable rating1;
		private IntWritable rating2;
		
		public user_rating() {
			this.user = new Text("");
			this.rating1 = new IntWritable(-1);
			this.rating2 = new IntWritable(-1);
			
		}
		
		
		public user_rating(user_rating s) {
			super();
			this.user = new Text(s.getUser().toString());
            this.rating1 = new IntWritable(Integer.parseInt(s.getRating1().toString()));
            this.rating2 = new IntWritable(Integer.parseInt(s.getRating2().toString()));
		}
		
		
		public Text getUser() {
			return user;
		}


		public void setUser(Text user) {
			this.user = user;
		}


		public IntWritable getRating1() {
			return rating1;
		}


		public void setRating1(IntWritable rating1) {
			this.rating1 = rating1;
		}


		public IntWritable getRating2() {
			return rating2;
		}


		public void setRating2(IntWritable rating2) {
			this.rating2 = rating2;
		}


		@Override
		public void readFields(DataInput data) throws IOException {
			this.user.readFields(data);
			this.rating1.readFields(data);
			this.rating2.readFields(data);
			
		}

		@Override
		public void write(DataOutput data) throws IOException {
			this.user.write(data);
			this.rating1.write(data);
			this.rating2.write(data);
			
		}
		
		public String toString() {
			return this.user.toString() + "," + this.rating1 + "," + this.rating2; 
		}
		
	}
	
	public static class array_user_rating extends ArrayWritable {
		
		public array_user_rating() {
			super(user_rating.class);
		}
		
		public array_user_rating(user_rating[] array) {
			super(user_rating.class, array);
		}

		public String toString() {
			
			StringBuffer line = new StringBuffer();
			for(int i=0; i<this.get().length; i++) {
				String a = "(" + this.get()[i].toString() + ")";
				if(i!= this.get().length-1) {
					line.append(a + ",");	
				}
				else
					line.append(a);
			}
			return "[" + line.toString() + "]";
		}
			
	}
	
	
	public static class UserMapper extends Mapper<Object, Text, Text, movie_rating> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, movie_rating>.Context context)
				throws IOException, InterruptedException {
			String [] lines = value.toString().split("::");
			movie_rating val = new movie_rating();
			val.setMovie(new Text(lines[1]));
			val.setRating(new IntWritable(Integer.parseInt(lines[2])));
			context.write(new Text(lines[0]), val);
			
		}
		
	}
		
	public static class UserReducer extends Reducer<Text, movie_rating, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<movie_rating> value,
				Reducer<Text, movie_rating, Text, Text>.Context context) throws IOException, InterruptedException {
			
			Set<String> set = new HashSet<String>();
			String list = "";
			
			for(movie_rating a: value) {
				String x = a.toString();
				if(!set.contains(x)) {
					list += x + " ";
					set.add(x);
				}
			}
			context.write(key, new Text(list));
						
		}
		
	}
	
	public static class MovieMapper extends Mapper<Object,Text,movie_movie,user_rating> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, movie_movie, user_rating>.Context context)
				throws IOException, InterruptedException {
			
			String[] lines = value.toString().split("\t");
			String[] a = lines[1].split(" ");
			
			for(int i=0;i<a.length-1;i++) {
				String[] x = a[i].split(",");
				String movie1 = x[0];
				String rating1 = x[1];
				
				for(int j=i+1;j<a.length;j++) {
					String[] y = a[j].split(",");
					String movie2 = y[0];
					String rating2 = y[1];
					
					movie_movie pair = new movie_movie();
					if(movie1.compareTo(movie2) > 0) {
						pair.setMovie1(new Text(movie2));
						pair.setMovie2(new Text(movie1));
						user_rating val = new user_rating();
						val.setUser(new Text(lines[0].toString()));
						val.setRating1(new IntWritable(Integer.parseInt(rating2.toString())));
						val.setRating2(new IntWritable(Integer.parseInt(rating1.toString())));
						context.write(pair, val);
				}
					else {
						pair.setMovie1(new Text(movie1));
						pair.setMovie2(new Text(movie2));
						user_rating val = new user_rating();
						val.setUser(new Text(lines[0].toString()));
						val.setRating1(new IntWritable(Integer.parseInt(rating1.toString())));
						val.setRating2(new IntWritable(Integer.parseInt(rating2.toString())));
						context.write(pair, val);
					}
				}

			}
			
		}

	}
	
	public static class MovieReducer extends Reducer<movie_movie,user_rating,Text,array_user_rating> {

		@Override
		protected void reduce(movie_movie key, Iterable<user_rating> value,
				Reducer<movie_movie, user_rating, Text, array_user_rating>.Context context) throws IOException, InterruptedException {
			ArrayList<user_rating> pair = new ArrayList<user_rating>();
			
			for(user_rating val:value) {
				user_rating tmp = new user_rating(val);
				pair.add(tmp);
			}
			
			user_rating[] arr = new user_rating[pair.size()];
			for(int i=0; i<pair.size(); i++)
				arr[i] = pair.get(i);
			
			array_user_rating result = new array_user_rating(arr);
			context.write(new Text(key.toString()), result);
		}
		
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job1 = Job.getInstance(conf, "state1");
		
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(UserReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(movie_rating.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("intermediary"));
		
		job1.waitForCompletion(true);
		
        Job job2 = Job.getInstance(conf, "state2");

        job2.setMapperClass(MovieMapper.class);
      	job2.setReducerClass(MovieReducer.class);
      
        job2.setMapOutputKeyClass(movie_movie.class);
        job2.setMapOutputValueClass(user_rating.class);
      
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(array_user_rating.class);
      
        FileInputFormat.addInputPath(job2, new Path("intermediary"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
      
        job2.waitForCompletion(true);

	}

}
